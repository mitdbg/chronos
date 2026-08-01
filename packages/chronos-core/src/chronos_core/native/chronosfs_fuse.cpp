#define FUSE_USE_VERSION 31

#include "chronosfs_fuse.hpp"

#include "interval_data_plane.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ankerl/unordered_dense.h>
#include <fuse3/fuse.h>
#include <nlohmann/json.hpp>
#include <pybind11/stl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace py = pybind11;
using Json = nlohmann::json;

namespace {

using chronos::native::IntervalBlob;
using chronos::native::IntervalRows;
using chronos::native::IntervalValue;
using chronos::native::NativeMergeChange;
using chronos::native::NativeMergePreview;
using chronos::native::NativeRowDiff;

std::int64_t as_int(const IntervalValue &value);
std::string as_string(const IntervalValue &value);
IntervalBlob as_blob(const IntervalValue &value);

// ChronosFS table map
// -------------------
// ChronosFS is intentionally built on top of the same native interval backend
// used by relational tables.  The FUSE layer should not implement its own
// branch algorithm: it maps POSIX concepts into logical tables, registers those
// tables with NativeBranchStore, and then reads/writes through branch sessions.
//
// Versioned logical tables:
//   chronosfs_inodes
//       POSIX inode metadata: type, mode, uid/gid, size, link count, symlink
//       target, and timestamps.  Metadata changes are interval upserts, so chmod,
//       truncate, symlink updates, and size changes are branch-local.
//
//   chronosfs_dirents
//       Directory edges from (parent_inode_id, name) to child inode.  Create,
//       unlink, rename, and rmdir are represented by interval upserts/tombstones
//       in this table.
//
//   chronosfs_file_blocks
//       Byte-range content records keyed by (inode_id, byte_start).  Imported
//       host files may start as one large external extent with SQL NULL data,
//       while Chronos-owned writes are stored as inline block extents.
//       The interval backend only versions rows; ChronosFS enforces the
//       branch-visible no-overlap range invariant before calling upsert_many().
//
// Physical interval tables:
//   _chronos_b_interval_chronosfs_inodes
//   _chronos_b_interval_chronosfs_dirents
//   _chronos_b_interval_chronosfs_file_blocks
//       Created by ensure_registered_table().  Each physical table contains the
//       logical columns plus live_lo/live_hi/writer_segment_id/deleted, matching
//       ordinary Chronos interval tables.  Branch visibility is therefore driven
//       only by the metadata branch head and segment branch_point.
//
// Non-versioned ChronosFS metadata:
//   _chronosfs_inode_allocator
//       Transactional inode-id allocator.  It is intentionally outside the
//       interval store: inode ids are stable identifiers, not branch-visible
//       content, and gaps after crashes are harmless.
//
//   _chronosfs_metadata
//       Small store-level metadata such as block_size.  This is not branch-local
//       state.  If table count becomes a problem, this table is a candidate for
//       replacement by store metadata/config as long as cross-process block-size
//       validation is preserved.
//
// Caching note:
//   This file may cache path/inode/dirent metadata inside one native FUSE backend
//   process, but it must not cache file block contents.  Block reads and writes
//   should go back to the interval store so separate handles, mount points, and
//   branch operations cannot observe stale data.

struct FsError : std::runtime_error {
    int code;
    FsError(int err, const std::string &message) : std::runtime_error(message), code(err) {}
};

std::string now_text() {
    std::time_t t = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

std::string time_text(const struct timespec &ts) {
    std::time_t t = ts.tv_sec;
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

struct timespec parse_time_text(const std::string &text) {
    struct timespec ts{};
    std::tm tm{};
    if (text.empty() || strptime(text.c_str(), "%Y-%m-%dT%H:%M:%SZ", &tm) == nullptr) {
        return ts;
    }
    ts.tv_sec = timegm(&tm);
    ts.tv_nsec = 0;
    return ts;
}

std::int64_t as_int(const IntervalValue &value) {
    if (auto ptr = std::get_if<std::int64_t>(&value)) return *ptr;
    if (auto ptr = std::get_if<double>(&value)) return static_cast<std::int64_t>(*ptr);
    if (auto ptr = std::get_if<std::string>(&value)) {
        if (*ptr == "t" || *ptr == "true") return 1;
        if (*ptr == "f" || *ptr == "false") return 0;
        return ptr->empty() ? 0 : std::stoll(*ptr);
    }
    return 0;
}

std::string as_string(const IntervalValue &value) {
    if (auto ptr = std::get_if<std::string>(&value)) return *ptr;
    if (auto ptr = std::get_if<std::int64_t>(&value)) return std::to_string(*ptr);
    if (auto ptr = std::get_if<double>(&value)) return std::to_string(*ptr);
    return {};
}

IntervalBlob as_blob(const IntervalValue &value) {
    if (auto ptr = std::get_if<IntervalBlob>(&value)) return *ptr;
    if (auto ptr = std::get_if<std::string>(&value)) return IntervalBlob(ptr->begin(), ptr->end());
    return {};
}

std::string sql_placeholders(std::size_t count) {
    std::string sql;
    for (std::size_t i = 0; i < count; ++i) {
        if (i) sql += ", ";
        sql += "?";
    }
    return sql;
}

struct Inode {
    std::int64_t id = 0;
    std::string kind;
    mode_t mode = 0;
    uid_t uid = 0;
    gid_t gid = 0;
    std::int64_t size = 0;
    nlink_t nlink = 1;
    std::string symlink_target;
    std::string atime;
    std::string mtime;
    std::string ctime;
    std::string source_path;
    std::time_t cached_atime = 0;
    std::time_t cached_mtime = 0;
    std::time_t cached_ctime = 0;
    bool timestamps_cached = false;
};

struct OpenHandle {
    Inode inode;
    std::string path;
    struct DirtyBlock {
        IntervalBlob data;
        std::vector<unsigned char> dirty;
    };
    std::map<std::int64_t, DirtyBlock> dirty_blocks;
    std::int64_t max_dirty_end = -1;
};

struct FileExtent {
    std::int64_t inode_id = 0;
    std::int64_t byte_start = 0;
    std::int64_t byte_end = 0;
    bool external = false;
    IntervalBlob data;
};

struct DirectoryEntry {
    std::string name;
    Inode inode;
};

std::string quote_ident(const std::string &identifier) {
    std::string out = "\"";
    for (char ch : identifier) {
        if (ch == '"') out += "\"\"";
        else out.push_back(ch);
    }
    out.push_back('"');
    return out;
}

std::string json_array(const std::vector<std::string> &items) {
    std::string out = "[";
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i) out += ",";
        out += "\"";
        for (char ch : items[i]) {
            if (ch == '"' || ch == '\\') out.push_back('\\');
            out.push_back(ch);
        }
        out += "\"";
    }
    out += "]";
    return out;
}

std::string hex_blob(const IntervalBlob &blob) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(blob.size() * 2);
    for (unsigned char byte : blob) {
        out.push_back(kHex[(byte >> 4) & 0x0f]);
        out.push_back(kHex[byte & 0x0f]);
    }
    return out;
}

bool looks_textual(const IntervalBlob &blob) {
    for (unsigned char ch : blob) {
        if (ch == '\n' || ch == '\r' || ch == '\t') continue;
        if (ch < 0x20 || ch == 0x7f) return false;
    }
    return true;
}

std::string blob_text(const IntervalBlob &blob) {
    return std::string(reinterpret_cast<const char *>(blob.data()), blob.size());
}

std::vector<std::string> split_diff_lines(const std::string &text) {
    std::vector<std::string> lines;
    std::size_t start = 0;
    while (start < text.size()) {
        std::size_t end = text.find('\n', start);
        if (end == std::string::npos) {
            lines.push_back(text.substr(start));
            break;
        }
        lines.push_back(text.substr(start, end - start));
        start = end + 1;
    }
    if (text.empty()) lines.emplace_back();
    return lines;
}

std::string simple_unified_diff(
    const std::string &before_label,
    const std::string &after_label,
    const IntervalBlob &before,
    const IntervalBlob &after) {
    if (!looks_textual(before) || !looks_textual(after)) return {};
    std::ostringstream out;
    out << "--- " << before_label << "\n";
    out << "+++ " << after_label << "\n";
    out << "@@\n";
    for (const auto &line : split_diff_lines(blob_text(before))) out << "-" << line << "\n";
    for (const auto &line : split_diff_lines(blob_text(after))) out << "+" << line << "\n";
    return out.str();
}

Json interval_value_to_json(const IntervalValue &value) {
    if (std::holds_alternative<std::monostate>(value)) return nullptr;
    if (auto ptr = std::get_if<std::int64_t>(&value)) return *ptr;
    if (auto ptr = std::get_if<double>(&value)) return *ptr;
    if (auto ptr = std::get_if<std::string>(&value)) return *ptr;
    if (auto ptr = std::get_if<IntervalBlob>(&value)) {
        return Json{
            {"encoding", "hex"},
            {"size", ptr->size()},
            {"hex", hex_blob(*ptr)},
        };
    }
    return nullptr;
}

const IntervalValue *value_for_column(
    const std::vector<std::string> &columns,
    const std::vector<IntervalValue> &values,
    const std::string &column) {
    for (std::size_t i = 0; i < columns.size() && i < values.size(); ++i) {
        if (columns[i] == column) return &values[i];
    }
    return nullptr;
}

std::int64_t int_for_column(
    const std::vector<std::string> &columns,
    const std::vector<IntervalValue> &values,
    const std::string &column,
    std::int64_t fallback = 0) {
    const IntervalValue *value = value_for_column(columns, values, column);
    return value ? as_int(*value) : fallback;
}

IntervalBlob blob_for_column(
    const std::vector<std::string> &columns,
    const std::vector<IntervalValue> &values,
    const std::string &column) {
    const IntervalValue *value = value_for_column(columns, values, column);
    return value ? as_blob(*value) : IntervalBlob{};
}

std::string path_join(const std::string &parent, const std::string &name) {
    if (parent.empty() || parent == "/") return "/" + name;
    return parent + "/" + name;
}

std::uint64_t fnv1a64(const std::string &text) {
    std::uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : text) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string stable_control_id(const std::string &payload) {
    std::ostringstream out;
    out << std::hex << std::setw(16) << std::setfill('0') << fnv1a64(payload);
    return out.str();
}

struct ControlMergePath {
    std::string source;
    std::string target;
    bool valid = false;
};

ControlMergePath parse_control_merge_path(const std::string &path, const std::string &prefix) {
    if (path.rfind(prefix, 0) != 0 || path.size() <= prefix.size()) return {};
    std::string spec = path.substr(prefix.size());
    if (spec.size() > 5 && spec.substr(spec.size() - 5) == ".json") {
        spec.resize(spec.size() - 5);
    }
    std::size_t sep = spec.find("..");
    if (sep == std::string::npos || sep == 0 || sep + 2 >= spec.size()) return {};
    return {spec.substr(0, sep), spec.substr(sep + 2), true};
}

NativeMergeChange merge_change_from_diff(const NativeRowDiff &diff) {
    NativeMergeChange change;
    change.table = diff.table;
    change.change = diff.change;
    change.key_columns = diff.key_columns;
    change.key_values = diff.key_values;
    change.after_columns = diff.columns;
    change.after_values = diff.after;
    change.has_after = diff.has_after;
    return change;
}

bool is_file_block_diff(const NativeRowDiff &diff) {
    return diff.table == "chronosfs_file_blocks";
}

bool is_inode_diff(const NativeRowDiff &diff) {
    return diff.table == "chronosfs_inodes";
}

std::int64_t diff_inode_id(const NativeRowDiff &diff) {
    return int_for_column(diff.key_columns, diff.key_values, "inode_id", 0);
}

std::unordered_set<std::int64_t> file_conflict_inodes(const std::vector<NativeRowDiff> &conflicts) {
    std::unordered_set<std::int64_t> out;
    for (const auto &conflict : conflicts) {
        if (is_file_block_diff(conflict)) {
            std::int64_t inode_id = diff_inode_id(conflict);
            if (inode_id) out.insert(inode_id);
        }
    }
    return out;
}

std::string join_ident_list(const std::vector<std::string> &columns) {
    std::string out;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i) out += ", ";
        out += quote_ident(columns[i]);
    }
    return out;
}

std::string interval_sql_type(const chronos::native::NativeBranchStore &store) {
    return store.dialect() == "postgres" ? "NUMERIC(32,0)" : "BIGINT";
}

IntervalValue max_interval_value(const chronos::native::NativeBranchStore &store) {
    if (store.dialect() == "postgres") return std::string("10000000000000000000000000000000");
    return std::int64_t{9000000000000000000LL};
}

std::string blob_sql_type(const chronos::native::NativeBranchStore &store) {
    return store.dialect() == "postgres" ? "BYTEA" : "BLOB";
}

std::string chronosfs_object_dir(const std::string &database_url) {
    constexpr const char *prefix = "sqlite:///";
    if (database_url.rfind(prefix, 0) != 0) return {};
    std::string database_path = database_url.substr(std::strlen(prefix));
    if (database_path.empty() || database_path == ":memory:") return {};
    return database_path + ".chronosfs-objects";
}

class NativeChronosFS {
  public:
    NativeChronosFS(std::string database_url, std::string branch_id, std::int64_t block_size)
        : store_(std::make_shared<chronos::native::NativeBranchStore>(database_url)),
          branch_id_(std::move(branch_id)),
          session_(store_->checkout(branch_id_)),
          block_size_(block_size),
          object_dir_(chronosfs_object_dir(database_url)) {
        if (block_size_ <= 0) throw FsError(EINVAL, "block size must be positive");
        if (store_->dialect() == "sqlite") {
            // A successful close publishes data to the filesystem but, like
            // the Linux page cache, need not force it to stable storage.
            // SQLite pragmas are connection-local, so configuring the daemon's
            // checkpoint connection is insufficient: the actual FUSE writer
            // must use NORMAL mode as well. op_fsync temporarily switches this
            // connection to FULL and commits a durability barrier.
            store_->execute_sql("PRAGMA synchronous=NORMAL", {});
            store_->execute_sql("PRAGMA wal_autocheckpoint=0", {});
        }
    }

    NativeChronosFS(
        std::shared_ptr<chronos::native::NativeBranchStore> store,
        std::string branch_id,
        std::int64_t block_size,
        std::string object_dir = {})
        : store_(std::move(store)),
          branch_id_(std::move(branch_id)),
          session_(store_->checkout(branch_id_)),
          block_size_(block_size),
          object_dir_(std::move(object_dir)) {
        if (block_size_ <= 0) throw FsError(EINVAL, "block size must be positive");
    }

    void ensure() {
        ensure_schema();
        ensure_registered_table(
            "chronosfs_inodes",
            inode_cols(),
            {
                "inode_id BIGINT",
                "kind TEXT NOT NULL",
                "mode INTEGER NOT NULL",
                "uid INTEGER NOT NULL",
                "gid INTEGER NOT NULL",
                "size BIGINT NOT NULL",
                "nlink INTEGER NOT NULL",
                "symlink_target TEXT",
                "atime TEXT NOT NULL",
                "mtime TEXT NOT NULL",
                "ctime TEXT NOT NULL",
                "source_path TEXT",
            },
            {"inode_id"});
        ensure_registered_table(
            "chronosfs_dirents",
            dirent_cols(),
            {
                "parent_inode_id BIGINT NOT NULL",
                "name TEXT NOT NULL",
                "inode_id BIGINT NOT NULL",
                "created_at TEXT NOT NULL",
            },
            {"parent_inode_id", "name"});
        ensure_registered_table(
            "chronosfs_file_blocks",
            block_cols(),
            {
                "inode_id BIGINT NOT NULL",
                "byte_start BIGINT NOT NULL",
                "byte_end BIGINT NOT NULL",
                "external BOOLEAN NOT NULL",
                "data " + blob_sql_type(*store_),
            },
            {"inode_id", "byte_start"});

        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS _chronosfs_inode_allocator "
            "(id INTEGER PRIMARY KEY, next_inode_id BIGINT NOT NULL)");
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS _chronosfs_metadata "
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL)");
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS _chronosfs_orphan_inodes "
            "(branch_id TEXT NOT NULL, inode_id BIGINT NOT NULL, "
            "PRIMARY KEY (branch_id, inode_id))");
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS _chronosfs_private_orphan_inodes "
            "(branch_id TEXT NOT NULL, inode_id BIGINT NOT NULL, "
            "PRIMARY KEY (branch_id, inode_id))");
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS _chronosfs_objects "
            "(path TEXT PRIMARY KEY)");
        if (store_->dialect() == "postgres") {
            store_->execute_sql(
                "INSERT INTO _chronosfs_inode_allocator (id, next_inode_id) "
                "VALUES (1, 2) ON CONFLICT (id) DO NOTHING");
            store_->execute_sql(
                "INSERT INTO _chronosfs_metadata (key, value) "
                "VALUES ('block_size', ?) ON CONFLICT (key) DO NOTHING",
                {std::to_string(block_size_)});
        } else {
            store_->execute_sql(
                "INSERT OR IGNORE INTO _chronosfs_inode_allocator (id, next_inode_id) VALUES (1, 2)");
            store_->execute_sql(
                "INSERT OR IGNORE INTO _chronosfs_metadata (key, value) VALUES ('block_size', ?)",
                {std::to_string(block_size_)});
        }

        try {
            inode_by_id(1);
        } catch (const FsError &) {
            std::string now = now_text();
            session_.upsert_rows(
                "chronosfs_inodes",
                inode_cols(),
                {"inode_id"},
                IntervalRows{{
                    std::int64_t{1},
                    std::string("directory"),
                    std::int64_t{0755},
                    static_cast<std::int64_t>(getuid()),
                    static_cast<std::int64_t>(getgid()),
                    std::int64_t{0},
                    std::int64_t{1},
                    std::monostate{},
                    now,
                    now,
                    now,
                    std::monostate{},
                }});
        }
    }

    bool getattr(const std::string &path, struct stat *st) {
        if (is_control(path)) {
            control_stat(path, st);
            return true;
        }
        const Inode *inode = try_inode_ref_for_path(path);
        if (inode == nullptr) return false;
        fill_stat(*inode, st);
        return true;
    }

    const std::vector<DirectoryEntry> &listdir_entries(const std::string &path) {
        const Inode *dir = try_inode_ref_for_path(path);
        if (dir == nullptr) throw FsError(ENOENT, "path not found");
        if (dir->kind != "directory") throw FsError(ENOTDIR, "not a directory");
        const std::int64_t dir_id = dir->id;
        auto cached_children = directory_children_cache_.find(dir_id);
        if (cached_children != directory_children_cache_.end()) {
            return cached_children->second;
        }
        auto rows = visible("chronosfs_dirents", {"name", "inode_id"}, "parent_inode_id = ?", {dir_id}, "ORDER BY name");
        std::vector<std::pair<std::string, std::int64_t>> children;
        std::vector<std::int64_t> inode_ids;
        children.reserve(rows.size());
        inode_ids.reserve(rows.size());
        for (auto &row : rows) {
            std::string name = as_string(row[0]);
            std::int64_t inode_id = as_int(row[1]);
            children.emplace_back(name, inode_id);
            inode_ids.push_back(inode_id);
            dirent_cache_[dirent_key(dir_id, name)] = {true, inode_id};
            cache_path(path == "/" ? "/" + name : path + "/" + name, inode_id);
        }
        loaded_dirents_.insert(dir_id);
        cache_inodes_by_id(inode_ids);
        std::vector<DirectoryEntry> entries;
        entries.reserve(children.size());
        for (const auto &[name, inode_id] : children) {
            entries.push_back({name, inode_by_id(inode_id)});
        }
        auto inserted =
            directory_children_cache_.emplace(dir_id, std::move(entries));
        return inserted.first->second;
    }

    std::vector<std::string> listdir(const std::string &path) {
        if (path == "/.chronos") return {"branches", "current", "merge-preview", "merge-apply"};
        if (path == "/.chronos/branches") return branches();
        if (path == "/.chronos/merge-preview" || path == "/.chronos/merge-apply") return {};
        const auto &entries = listdir_entries(path);
        std::vector<std::string> names;
        names.reserve(entries.size());
        for (const auto &entry : entries) names.push_back(entry.name);
        return names;
    }

    IntervalBlob read(const std::string &path, std::int64_t offset, std::int64_t size) {
        if (is_control(path)) return control_read(path, offset, size);
        Inode inode = inode_for_path(path);
        if (inode.kind == "symlink") return IntervalBlob(inode.symlink_target.begin(), inode.symlink_target.end());
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        return read_inode_range(inode, offset, size);
    }

    IntervalBlob read_handle(const OpenHandle *handle, const std::string &path, std::int64_t offset, std::int64_t size) {
        if (!handle) return read(path, offset, size);
        const Inode &inode = handle->inode;
        if (inode.kind == "symlink") return IntervalBlob(inode.symlink_target.begin(), inode.symlink_target.end());
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        // The handle was opened after the kernel's lookup/getattr sequence and
        // therefore carries the size for this open.  Re-reading the inode
        // through a different parallel reader slot can resurrect that slot's
        // pre-write size and expose zero padding or a stale EOF.  Extents are
        // still read from the shared store, but the open handle is the
        // authority for its logical size and external source.
        const Inode &persisted = inode;
        const std::int64_t logical_size =
            std::max(persisted.size, handle->max_dirty_end);
        if (size <= 0 || offset >= logical_size) return {};

        const std::int64_t end = std::min<std::int64_t>(logical_size, offset + size);
        IntervalBlob out = read_inode_range(persisted, offset, end - offset);
        out.resize(static_cast<std::size_t>(end - offset), 0);
        if (handle->dirty_blocks.empty()) return out;

        const std::int64_t first_block = (offset / block_size_) * block_size_;
        for (auto it = handle->dirty_blocks.lower_bound(first_block);
             it != handle->dirty_blocks.end() && it->first < end;
             ++it) {
            const std::int64_t block_start = it->first;
            const auto &dirty_block = it->second;
            const std::int64_t copy_start = std::max(offset, block_start);
            const std::int64_t copy_end = std::min<std::int64_t>(
                end,
                block_start + static_cast<std::int64_t>(dirty_block.data.size()));
            for (std::int64_t absolute = copy_start; absolute < copy_end; ++absolute) {
                const std::size_t block_offset = static_cast<std::size_t>(absolute - block_start);
                if (block_offset < dirty_block.dirty.size() && dirty_block.dirty[block_offset]) {
                    out[static_cast<std::size_t>(absolute - offset)] = dirty_block.data[block_offset];
                }
            }
        }
        return out;
    }

    void write(const std::string &path, const char *data, std::int64_t size, std::int64_t offset) {
        if (is_control(path)) return control_write(path, data, size, offset);
        Inode inode = inode_for_path(path);
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        IntervalBlob payload(
            reinterpret_cast<const unsigned char *>(data),
            reinterpret_cast<const unsigned char *>(data + size));
        write_inode_at(inode, offset, payload);
    }

    void write_handle(OpenHandle *handle, const std::string &path, const char *data, std::int64_t size, std::int64_t offset) {
        if (!handle) return write(path, data, size, offset);
        if (handle->inode.kind != "file") throw FsError(EISDIR, "not a file");
        if (size <= 0) return;
        if (offset < 0) throw FsError(EINVAL, "negative write offset");
        std::int64_t cursor = 0;
        while (cursor < size) {
            const std::int64_t absolute = offset + cursor;
            const std::int64_t block_start = (absolute / block_size_) * block_size_;
            const std::int64_t block_offset = absolute - block_start;
            const std::int64_t take = std::min<std::int64_t>(
                size - cursor,
                block_size_ - block_offset);
            auto &block = handle->dirty_blocks[block_start];
            if (block.data.empty()) {
                block.data.resize(static_cast<std::size_t>(block_size_), 0);
                block.dirty.resize(static_cast<std::size_t>(block_size_), 0);
            }
            std::copy(
                reinterpret_cast<const unsigned char *>(data + cursor),
                reinterpret_cast<const unsigned char *>(data + cursor + take),
                block.data.begin() + block_offset);
            std::fill(
                block.dirty.begin() + block_offset,
                block.dirty.begin() + block_offset + take,
                static_cast<unsigned char>(1));
            cursor += take;
        }
        handle->max_dirty_end = std::max<std::int64_t>(
            handle->max_dirty_end,
            offset + size);
    }

    void flush_handle(OpenHandle *handle) {
        if (!handle || handle->dirty_blocks.empty()) return;
        Inode persisted = inode_by_id(handle->inode.id);
        const std::int64_t final_size = std::max(persisted.size, handle->max_dirty_end);
        const bool externalize =
            can_externalize_complete_file(*handle, persisted, final_size);
        std::string object_path;
        if (externalize) {
            object_path = materialize_complete_file(*handle, final_size);
        }

        try {
            transaction([&] {
                if (externalize) {
                    store_->execute_sql(
                        "INSERT OR IGNORE INTO _chronosfs_objects (path) "
                        "VALUES (?)",
                        {object_path});
                    upsert(
                        "chronosfs_file_blocks",
                        block_cols(),
                        {"inode_id", "byte_start"},
                        {persisted.id,
                         std::int64_t{0},
                         final_size,
                         std::int64_t{1},
                         std::monostate{}},
                        false);
                    persisted.source_path = object_path;
                } else {
                    IntervalRows replacement_blocks;
                    if (persisted.source_path.empty()) {
                        replacement_blocks.reserve(handle->dirty_blocks.size());
                    }
                    for (const auto &[block_start, dirty_block] :
                         handle->dirty_blocks) {
                        if (block_start >= final_size) continue;
                        const std::int64_t valid = std::min<std::int64_t>(
                            block_size_,
                            final_size - block_start);
                        IntervalBlob merged =
                            read_inode_range(persisted, block_start, valid);
                        merged.resize(static_cast<std::size_t>(valid), 0);
                        for (std::int64_t i = 0; i < valid; ++i) {
                            const std::size_t index =
                                static_cast<std::size_t>(i);
                            if (index < dirty_block.dirty.size() &&
                                dirty_block.dirty[index]) {
                                merged[index] = dirty_block.data[index];
                            }
                        }
                        if (persisted.source_path.empty()) {
                            replacement_blocks.push_back(
                                {persisted.id,
                                 block_start,
                                 block_start + valid,
                                 std::int64_t{0},
                                 std::move(merged)});
                        } else {
                            replace_inode_bytes(
                                persisted,
                                block_start,
                                merged);
                        }
                    }
                    if (!replacement_blocks.empty()) {
                        // Buffered writes already provide complete fixed blocks
                        // here. Submit the file's blocks in one interval upsert.
                        upsert_many(
                            "chronosfs_file_blocks",
                            block_cols(),
                            {"inode_id", "byte_start"},
                            replacement_blocks,
                            false);
                    }
                }

                if (final_size != persisted.size || externalize) {
                    const std::string now = now_text();
                    persisted.size = final_size;
                    persisted.mtime = now;
                    persisted.ctime = now;
                    upsert(
                        "chronosfs_inodes",
                        inode_cols(),
                        {"inode_id"},
                        inode_values(persisted),
                        false);
                }
            });
        } catch (...) {
            if (!object_path.empty()) ::unlink(object_path.c_str());
            throw;
        }
        if (!object_path.empty()) {
            pending_object_paths_.insert(object_path);
        }

        persisted.size = final_size;
        handle->inode = persisted;
        handle->dirty_blocks.clear();
        handle->max_dirty_end = -1;
        cache_inode(persisted);
    }

    void sync_storage() {
        if (store_->dialect() == "sqlite") {
            // File contents live in immutable ChronosFS objects. Make those
            // objects durable before publishing a durable SQLite metadata
            // prefix that references them.
            for (const auto &path : pending_object_paths_) {
                int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
                if (fd < 0) {
                    if (errno == ENOENT) continue;
                    throw FsError(errno, "could not open ChronosFS object");
                }
                if (::fsync(fd) != 0) {
                    const int error = errno;
                    ::close(fd);
                    throw FsError(error, "could not sync ChronosFS object");
                }
                ::close(fd);
            }
            if (!pending_object_paths_.empty() && !object_dir_.empty()) {
                int dir_fd =
                    ::open(object_dir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
                if (dir_fd >= 0) {
                    if (::fsync(dir_fd) != 0) {
                        const int error = errno;
                        ::close(dir_fd);
                        throw FsError(
                            error,
                            "could not sync ChronosFS object directory");
                    }
                    ::close(dir_fd);
                }
            }
            // NORMAL mode may defer the WAL sync, but fsync only requires the
            // committed WAL prefix to become durable; it does not require
            // copying every dirty page back into the database file. Commit one
            // metadata frame under FULL synchronous mode as a durability
            // barrier for all preceding WAL frames, then return to NORMAL.
            store_->execute_sql("PRAGMA synchronous=FULL", {});
            try {
                store_->execute_sql("BEGIN IMMEDIATE", {});
                store_->execute_sql(
                    "INSERT INTO _chronosfs_metadata (key, value) "
                    "VALUES ('fsync_generation', '1') "
                    "ON CONFLICT(key) DO UPDATE SET "
                    "value = CAST(value AS INTEGER) + 1",
                    {});
                store_->execute_sql("COMMIT", {});
            } catch (...) {
                try {
                    store_->execute_sql("ROLLBACK", {});
                } catch (...) {
                }
                store_->execute_sql("PRAGMA synchronous=NORMAL", {});
                throw;
            }
            store_->execute_sql("PRAGMA synchronous=NORMAL", {});
            pending_object_paths_.clear();
        }
    }

    std::vector<std::int64_t> orphan_inode_ids() {
        auto rows = store_->query_sql(
            "SELECT inode_id FROM _chronosfs_orphan_inodes "
            "WHERE branch_id = ? UNION "
            "SELECT inode_id FROM _chronosfs_private_orphan_inodes "
            "WHERE branch_id = ? ORDER BY inode_id",
            {branch_id_, branch_id_});
        std::vector<std::int64_t> ids;
        ids.reserve(rows.size());
        for (const auto &row : rows) {
            ids.push_back(as_int(row[0]));
        }
        return ids;
    }

    void truncate_handle(OpenHandle *handle, const std::string &path, std::int64_t size) {
        if (!handle) return truncate(path, size);
        if (handle->inode.kind != "file") throw FsError(EISDIR, "not a file");
        flush_handle(handle);
        truncate_inode(handle->inode, size);
        handle->inode.size = size;
        handle->max_dirty_end = -1;
    }

    Inode create(const std::string &path, mode_t mode) {
        auto [parent_path, name] = split_parent(path);
        Inode parent = inode_for_path(parent_path);
        Inode inode;
        transaction([&] {
            if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
            std::int64_t id = allocate_inode();
            std::string now = now_text();
            upsert(
                "chronosfs_inodes",
                inode_cols(),
                {"inode_id"},
                {id,
                 std::string("file"),
                 static_cast<std::int64_t>(mode & 07777),
                 static_cast<std::int64_t>(getuid()),
                 static_cast<std::int64_t>(getgid()),
                 std::int64_t{0},
                 std::int64_t{1},
                 std::monostate{},
                 now,
                 now,
                 now,
                 std::monostate{}},
                false);
            upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
            touch(parent.id);
            inode = Inode{id, "file", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now, ""};
        });
        remember_created_path(path, parent.id, name, inode);
        return inode;
    }

    OpenHandle *open_handle(const std::string &path, int flags = 0) {
        if (is_control(path)) return nullptr;
        Inode inode = inode_for_path(path);
        if (inode.kind == "directory") throw FsError(EISDIR, "is directory");
        if (inode.kind == "file" && (flags & O_TRUNC)) {
            truncate_inode(inode, 0);
            inode.size = 0;
            inode.mtime = now_text();
            inode.ctime = inode.mtime;
        }
        return new OpenHandle{std::move(inode), path, {}, -1};
    }

    OpenHandle *create_handle(const std::string &path, mode_t mode) {
        if (is_control(path)) return nullptr;
        return new OpenHandle{create(path, mode), path, {}, -1};
    }

    void mkdir(const std::string &path, mode_t mode) {
        std::string branch = control_branch_name(path);
        if (!branch.empty()) return create_branch(branch);
        auto [parent_path, name] = split_parent(path);
        Inode parent = inode_for_path(parent_path);
        Inode inode;
        transaction([&] {
            if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
            std::int64_t id = allocate_inode();
            std::string now = now_text();
            upsert(
                "chronosfs_inodes",
                inode_cols(),
                {"inode_id"},
                {id,
                 std::string("directory"),
                 static_cast<std::int64_t>(mode & 07777),
                 static_cast<std::int64_t>(getuid()),
                 static_cast<std::int64_t>(getgid()),
                 std::int64_t{0},
                 std::int64_t{1},
	                 std::monostate{},
	                 now,
	                 now,
	                 now,
	                 std::monostate{}},
                false);
            upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
            touch(parent.id);
            inode = Inode{id, "directory", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now, ""};
        });
        remember_created_path(path, parent.id, name, inode);
    }

    void truncate(const std::string &path, std::int64_t size) {
        if (path == "/.chronos/current") return;
        Inode inode = inode_for_path(path);
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        truncate_inode(inode, size);
    }

    void unlink_path(const std::string &path, bool allow_dir) {
        auto [parent_path, name] = split_parent(path);
        std::int64_t parent_id = 0;
        std::int64_t inode_id = 0;
        bool deleted_directory = false;
        transaction([&] {
            Inode parent = inode_for_path(parent_path);
            parent_id = parent.id;
            auto entry = dirent(parent.id, name);
            if (!entry.first) throw FsError(ENOENT, "path not found");
            inode_id = entry.second;
            Inode inode = inode_by_id(entry.second);
            if (inode.kind == "directory" && !allow_dir) throw FsError(EISDIR, "is directory");
            if (inode.kind == "directory" && !listdir(path).empty()) throw FsError(ENOTEMPTY, "not empty");
            deleted_directory = inode.kind == "directory";
            if (inode.kind == "file") queue_orphan_inode(inode.id);
            upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, entry.second, now_text()}, true);
            upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), true);
            touch(parent.id);
        });
        if (deleted_directory) {
            forget_path_tree(path);
        } else {
            forget_path(path);
        }
        inode_cache_.erase(inode_id);
        invalidate_directory_cache(parent_id);
        dirent_cache_.erase(dirent_key(parent_id, name));
    }

    void rename_path(const std::string &from, const std::string &to) {
        auto [op, on] = split_parent(from);
        auto [np, nn] = split_parent(to);
        Inode old_parent = inode_for_path(op);
        Inode new_parent = inode_for_path(np);
        auto source = dirent(old_parent.id, on);
        if (!source.first) throw FsError(ENOENT, "path not found");
        auto repl = dirent(new_parent.id, nn);
        if (repl.first) {
            upsert(
                "chronosfs_dirents",
                dirent_cols(),
                {"parent_inode_id", "name"},
                {new_parent.id, nn, repl.second, now_text()},
                true);
        }
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {old_parent.id, on, source.second, now_text()}, true);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {new_parent.id, nn, source.second, now_text()}, false);
        touch(old_parent.id);
        touch(new_parent.id);
        invalidate_directory_cache(old_parent.id);
        invalidate_directory_cache(new_parent.id);
        forget_path_tree(from);
        forget_path_tree(to);
        dirent_cache_[dirent_key(old_parent.id, on)] = {false, 0};
        dirent_cache_[dirent_key(new_parent.id, nn)] = {true, source.second};
        if (repl.first) inode_cache_.erase(repl.second);
    }

    void chmod_path(const std::string &path, mode_t mode) {
        Inode inode = inode_for_path(path);
        inode.mode = mode & 07777;
        inode.ctime = now_text();
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        cache_inode(inode);
    }

    void utimens_path(const std::string &path, const struct timespec tv[2]) {
        if (is_control(path)) return;
        Inode inode = inode_for_path(path);
        std::string now = now_text();
        if (tv == nullptr) {
            inode.atime = now;
            inode.mtime = now;
        } else {
            if (tv[0].tv_nsec == UTIME_NOW) {
                inode.atime = now;
            } else if (tv[0].tv_nsec != UTIME_OMIT) {
                inode.atime = time_text(tv[0]);
            }
            if (tv[1].tv_nsec == UTIME_NOW) {
                inode.mtime = now;
            } else if (tv[1].tv_nsec != UTIME_OMIT) {
                inode.mtime = time_text(tv[1]);
            }
        }
        inode.ctime = now;
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        cache_inode(inode);
    }

    void symlink_path(const std::string &target, const std::string &link_path) {
        auto [parent_path, name] = split_parent(link_path);
        Inode parent = inode_for_path(parent_path);
        if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
        std::int64_t id = allocate_inode();
        std::string now = now_text();
        Inode inode{
            id,
            "symlink",
            0777,
            getuid(),
            getgid(),
            static_cast<std::int64_t>(target.size()),
            1,
            target,
            now,
            now,
            now,
            "",
        };
        upsert(
            "chronosfs_inodes",
            inode_cols(),
            {"inode_id"},
            {id,
             std::string("symlink"),
             std::int64_t{0777},
             static_cast<std::int64_t>(getuid()),
             static_cast<std::int64_t>(getgid()),
             static_cast<std::int64_t>(target.size()),
             std::int64_t{1},
	             target,
	             now,
	             now,
	             now,
	             std::monostate{}},
	            false);
        upsert(
            "chronosfs_dirents",
            dirent_cols(),
            {"parent_inode_id", "name"},
            {parent.id, name, id, now},
            false);
        touch(parent.id);
        remember_created_path(link_path, parent.id, name, inode);
    }

    std::string readlink_path(const std::string &path) {
        Inode inode = inode_for_path(path);
        if (inode.kind != "symlink") throw FsError(EINVAL, "not symlink");
        return inode.symlink_target;
    }

    bool exists_path(const std::string &path) {
        try {
            inode_for_path(path);
            return true;
        } catch (const FsError &err) {
            if (err.code == ENOENT) return false;
            throw;
        }
    }

    Inode stat_path(const std::string &path) {
        return inode_for_path(path);
    }

    void fill_stat_public(const Inode &inode, struct stat *st) {
        fill_stat(inode, st);
    }

    Inode stat_inode_id(std::int64_t inode_id) {
        return inode_by_id(inode_id);
    }

    Inode lookup_child_inode(std::int64_t parent_inode_id, const std::string &name) {
        auto entry = dirent(parent_inode_id, name);
        if (!entry.first) throw FsError(ENOENT, "path not found");
        return inode_by_id(entry.second);
    }

    std::vector<std::string> listdir_inode_id(std::int64_t inode_id) {
        Inode dir = inode_by_id(inode_id);
        if (dir.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        auto rows = visible("chronosfs_dirents", {"name"}, "parent_inode_id = ?", {dir.id}, "ORDER BY name");
        std::vector<std::string> names;
        for (auto &row : rows) names.push_back(as_string(row[0]));
        return names;
    }

    IntervalBlob read_file_path(const std::string &path) {
        Inode inode = inode_for_path(path);
        if (inode.kind == "symlink") return read_file_path(resolve_symlink_target(path, inode.symlink_target));
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        return read_inode_range(inode, 0, inode.size);
    }

    IntervalBlob read_inode_range_public(std::int64_t inode_id, std::int64_t offset, std::int64_t size) {
        if (offset < 0 || size < 0) throw FsError(EINVAL, "negative read range");
        Inode inode = inode_by_id(inode_id);
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        return read_inode_range(inode, offset, size);
    }

    void write_file_path(const std::string &path, const IntervalBlob &payload, mode_t mode, bool parents) {
        auto [parent_path, name] = split_parent(path);
        transaction([&] {
            Inode parent = parents ? ensure_directory_path(parent_path, 0755) : inode_for_path(parent_path);
            auto entry = dirent(parent.id, name);
            Inode inode;
            if (!entry.first) {
                inode = create_file_child(parent, name, mode);
            } else {
                inode = inode_by_id(entry.second);
                if (inode.kind != "file") throw FsError(EISDIR, "not a file");
                inode.mode = mode & 07777;
                truncate_inode(inode, 0);
                inode.size = 0;
            }
            write_inode_at(inode, 0, payload);
            if (payload.empty()) {
                inode.size = 0;
                inode.mtime = now_text();
                inode.ctime = inode.mtime;
                upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
                cache_inode(inode);
            }
        });
    }

    void write_files_public(
        const std::vector<std::string> &paths,
        const std::vector<IntervalBlob> &payloads,
        mode_t mode,
        bool parents) {
        if (paths.size() != payloads.size()) {
            throw FsError(EINVAL, "path and payload counts differ");
        }
        transaction([&] {
            for (std::size_t index = 0; index < paths.size(); ++index) {
                write_file_path(paths[index], payloads[index], mode, parents);
            }
        });
    }

    void write_at_path(const std::string &path, std::int64_t offset, const IntervalBlob &payload) {
        if (offset < 0) throw FsError(EINVAL, "negative write offset");
        Inode inode = inode_for_path(path);
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        write_inode_at(inode, offset, payload);
    }

    void write_inode_at_public(std::int64_t inode_id, std::int64_t offset, const IntervalBlob &payload) {
        if (offset < 0) throw FsError(EINVAL, "negative write offset");
        Inode inode = inode_by_id(inode_id);
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        write_inode_at(inode, offset, payload);
    }

    void truncate_inode_public(std::int64_t inode_id, std::int64_t size) {
        Inode inode = inode_by_id(inode_id);
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        truncate_inode(inode, size);
    }

    void chmod_inode_public(std::int64_t inode_id, mode_t mode) {
        Inode inode = inode_by_id(inode_id);
        inode.mode = mode & 07777;
        inode.ctime = now_text();
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        cache_inode(inode);
    }

    std::int64_t create_file_at_public(std::int64_t parent_inode_id, const std::string &name, mode_t mode) {
        Inode parent = inode_by_id(parent_inode_id);
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        return create_file_child(parent, name, mode).id;
    }

    std::int64_t mkdir_at_public(std::int64_t parent_inode_id, const std::string &name, mode_t mode) {
        Inode parent = inode_by_id(parent_inode_id);
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        return mkdir_child(parent, name, mode).id;
    }

    std::int64_t symlink_at_public(std::int64_t parent_inode_id, const std::string &name, const std::string &target) {
        Inode parent = inode_by_id(parent_inode_id);
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        return symlink_child(parent, name, target).id;
    }

    void unlink_at_public(std::int64_t parent_inode_id, const std::string &name, bool allow_dir) {
        std::int64_t inode_id = 0;
        transaction([&] {
            Inode parent = inode_by_id(parent_inode_id);
            auto entry = dirent(parent.id, name);
            if (!entry.first) throw FsError(ENOENT, "path not found");
            inode_id = entry.second;
            Inode inode = inode_by_id(entry.second);
            if (inode.kind == "directory" && !allow_dir) throw FsError(EISDIR, "is directory");
            if (inode.kind == "directory" && !listdir_inode_id(inode.id).empty()) throw FsError(ENOTEMPTY, "not empty");
            if (inode.kind == "file") queue_orphan_inode(inode.id);
            upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, entry.second, now_text()}, true);
            upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), true);
            touch(parent.id);
        });
        inode_cache_.erase(inode_id);
        invalidate_directory_cache(parent_inode_id);
        dirent_cache_[dirent_key(parent_inode_id, name)] = {false, 0};
    }

    void rename_at_public(
        std::int64_t old_parent_inode_id,
        const std::string &old_name,
        std::int64_t new_parent_inode_id,
        const std::string &new_name) {
        Inode old_parent = inode_by_id(old_parent_inode_id);
        Inode new_parent = inode_by_id(new_parent_inode_id);
        auto source = dirent(old_parent.id, old_name);
        if (!source.first) throw FsError(ENOENT, "path not found");
        auto repl = dirent(new_parent.id, new_name);
        if (repl.first) {
            upsert(
                "chronosfs_dirents",
                dirent_cols(),
                {"parent_inode_id", "name"},
                {new_parent.id, new_name, repl.second, now_text()},
                true);
            inode_cache_.erase(repl.second);
        }
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {old_parent.id, old_name, source.second, now_text()}, true);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {new_parent.id, new_name, source.second, now_text()}, false);
        touch(old_parent.id);
        touch(new_parent.id);
        invalidate_directory_cache(old_parent.id);
        invalidate_directory_cache(new_parent.id);
        dirent_cache_[dirent_key(old_parent.id, old_name)] = {false, 0};
        dirent_cache_[dirent_key(new_parent.id, new_name)] = {true, source.second};
    }

    std::int64_t mkdir_path(const std::string &path, mode_t mode, bool parents) {
        if (path == "/" || path.empty()) return 1;
        if (parents) return ensure_directory_path(path, mode).id;
        mkdir(path, mode);
        return inode_for_path(path).id;
    }

    std::int64_t symlink_path_public(const std::string &target, const std::string &link_path, bool parents) {
        auto [parent_path, name] = split_parent(link_path);
        Inode parent = parents ? ensure_directory_path(parent_path, 0755) : inode_for_path(parent_path);
        return symlink_child(parent, name, target).id;
    }

    void import_tree_public(const std::string &source_root) {
        namespace fs = std::filesystem;
        fs::path root = fs::absolute(fs::path(source_root)).lexically_normal();
        if (!fs::exists(root) || !fs::is_directory(root)) {
            throw FsError(ENOENT, "ChronosFS import source is not a directory");
        }
        transaction([&] {
            for (fs::recursive_directory_iterator it(root, fs::directory_options::skip_permission_denied), end;
                 it != end;
                 ++it) {
                const fs::path path = it->path();
                const fs::path rel_path = path.lexically_relative(root);
                if (rel_path.empty()) continue;
                std::string rel = rel_path.generic_string();
                if (rel == ".chronos" || rel.rfind(".chronos/", 0) == 0) {
                    if (it->is_directory()) it.disable_recursion_pending();
                    continue;
                }
                const std::string target = "/" + rel;
                import_one_path(path, target);
            }
        });
        clear_metadata_cache();
    }

    void clear_cache_public() {
        session_ = store_->checkout(branch_id_);
        clear_metadata_cache();
    }

    void clear_private_inode_hints_public() {
        private_created_inodes_.clear();
    }

    void begin_write_batch_public() {
        if (!session_.in_transaction()) session_.begin();
    }

    void commit_write_batch_public() {
        if (session_.in_transaction()) session_.commit();
    }

    void rollback_write_batch_public() {
        if (session_.in_transaction()) session_.rollback();
    }

    void invalidate_cached_paths_public(const std::vector<std::string> &paths) {
        for (const auto &path : paths) {
            if (path.empty()) continue;
            if (path == "/") {
                clear_metadata_cache();
                continue;
            }

            auto inode_it = path_inode_cache_.find(path);
            if (inode_it != path_inode_cache_.end()) {
                inode_cache_.erase(inode_it->second);
            }

            const std::string parent_path = cached_parent_path(path);
            std::int64_t parent_inode_id = 0;
            if (parent_path == "/") {
                parent_inode_id = 1;
            } else {
                auto parent_it = path_inode_cache_.find(parent_path);
                if (parent_it != path_inode_cache_.end()) {
                    parent_inode_id = parent_it->second;
                }
            }

            forget_path_tree(path);
            if (parent_inode_id != 0) {
                // Namespace mutations also update the parent timestamps.
                inode_cache_.erase(parent_inode_id);
                invalidate_directory_cache(parent_inode_id);
                const auto slash = path.find_last_of('/');
                const std::string name =
                    slash == std::string::npos ? path : path.substr(slash + 1);
                dirent_cache_.erase(dirent_key(parent_inode_id, name));
            }
        }
    }

    std::vector<std::string> branch_names_public() {
        return branches();
    }

    IntervalBlob control_read(const std::string &path, std::int64_t offset, std::int64_t size) {
        std::string content;
        if (path == "/.chronos/current") {
            content = branch_id_ + "\n";
        } else if (parse_control_merge_path(path, "/.chronos/merge-preview/").valid) {
            content = merge_preview_json(path);
        } else if (parse_control_merge_path(path, "/.chronos/merge-apply/").valid) {
            content.clear();
        } else if (
            path == "/.chronos" ||
            path == "/.chronos/branches" ||
            path == "/.chronos/merge-preview" ||
            path == "/.chronos/merge-apply" ||
            !control_branch_name(path).empty()) {
            throw FsError(EISDIR, "control path is a directory");
        } else {
            throw FsError(ENOENT, "control path not found");
        }
        if (size <= 0 || offset >= static_cast<std::int64_t>(content.size())) return {};
        auto take = std::min<std::int64_t>(size, content.size() - offset);
        return IntervalBlob(content.begin() + offset, content.begin() + offset + take);
    }

    void control_write(const std::string &path, const char *data, std::int64_t size, std::int64_t offset) {
        if (path == "/.chronos/current") {
            if (offset != 0) throw FsError(EINVAL, "current branch writes must start at zero");
            std::string next(data, data + size);
            while (!next.empty() && (next.back() == '\n' || next.back() == '\r' || next.back() == '\0')) next.pop_back();
            auto names = branches();
            if (std::find(names.begin(), names.end(), next) == names.end()) throw FsError(ENOENT, "branch not found");
            branch_id_ = next;
            session_ = store_->checkout(branch_id_);
            clear_metadata_cache();
            return;
        }
        if (parse_control_merge_path(path, "/.chronos/merge-preview/").valid) {
            throw FsError(EROFS, "merge preview is read-only");
        }
        if (parse_control_merge_path(path, "/.chronos/merge-apply/").valid) {
            if (offset != 0) throw FsError(EINVAL, "merge-apply control writes must start at zero");
            apply_control_merge(path, std::string(data, data + size));
            return;
        }
        throw FsError(EINVAL, "unsupported control write");
    }

  private:
    bool can_externalize_complete_file(
        const OpenHandle &handle,
        const Inode &persisted,
        std::int64_t final_size) const {
        constexpr std::int64_t kExternalObjectThreshold = 64 * 1024;
        if (object_dir_.empty() ||
            !persisted.source_path.empty() ||
            handle.inode.size != 0 ||
            final_size < kExternalObjectThreshold ||
            handle.max_dirty_end != final_size) {
            return false;
        }

        std::int64_t covered = 0;
        for (const auto &[block_start, dirty_block] : handle.dirty_blocks) {
            if (block_start != covered || block_start >= final_size) return false;
            const std::int64_t valid = std::min<std::int64_t>(
                block_size_,
                final_size - block_start);
            if (dirty_block.data.size() < static_cast<std::size_t>(valid) ||
                dirty_block.dirty.size() < static_cast<std::size_t>(valid) ||
                std::find(
                    dirty_block.dirty.begin(),
                    dirty_block.dirty.begin() + valid,
                    static_cast<unsigned char>(0)) !=
                    dirty_block.dirty.begin() + valid) {
                return false;
            }
            covered += valid;
        }
        return covered == final_size;
    }

    std::string materialize_complete_file(
        const OpenHandle &handle,
        std::int64_t final_size) {
        static std::atomic<std::uint64_t> next_object_id{1};
        namespace fs = std::filesystem;
        std::error_code error;
        fs::create_directories(object_dir_, error);
        if (error) {
            throw FsError(
                error.value(),
                "could not create ChronosFS object directory");
        }

        const std::string basename =
            "object-" + std::to_string(getpid()) + "-" +
            std::to_string(
                next_object_id.fetch_add(1, std::memory_order_relaxed)) +
            "-" + std::to_string(handle.inode.id);
        const fs::path final_path = fs::path(object_dir_) / basename;
        const fs::path temporary_path =
            fs::path(object_dir_) / ("." + basename + ".tmp");
        int fd = ::open(
            temporary_path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            0600);
        if (fd < 0) {
            throw FsError(errno, "could not create ChronosFS object");
        }

        try {
            std::int64_t written = 0;
            for (const auto &[block_start, dirty_block] : handle.dirty_blocks) {
                const std::int64_t valid = std::min<std::int64_t>(
                    block_size_,
                    final_size - block_start);
                std::int64_t block_written = 0;
                while (block_written < valid) {
                    const ssize_t result = ::write(
                        fd,
                        dirty_block.data.data() + block_written,
                        static_cast<std::size_t>(valid - block_written));
                    if (result < 0) {
                        if (errno == EINTR) continue;
                        throw FsError(
                            errno,
                            "could not write ChronosFS object");
                    }
                    block_written += result;
                    written += result;
                }
            }
            if (written != final_size) {
                throw FsError(EIO, "incomplete ChronosFS object write");
            }
            if (::close(fd) != 0) {
                fd = -1;
                throw FsError(errno, "could not close ChronosFS object");
            }
            fd = -1;
            if (::rename(temporary_path.c_str(), final_path.c_str()) != 0) {
                throw FsError(errno, "could not publish ChronosFS object");
            }
        } catch (...) {
            if (fd >= 0) ::close(fd);
            ::unlink(temporary_path.c_str());
            ::unlink(final_path.c_str());
            throw;
        }
        return final_path.string();
    }

    // chronosfs_inodes is the POSIX metadata table. It is interval-versioned by
    // inode_id, so chmod/truncate/link metadata changes are branch-local rows.
    static std::vector<std::string> inode_cols() {
        return {
            "inode_id",
            "kind",
            "mode",
            "uid",
            "gid",
            "size",
            "nlink",
            "symlink_target",
            "atime",
            "mtime",
            "ctime",
            "source_path",
        };
    }

    // chronosfs_dirents maps a directory inode plus entry name to the child
    // inode. Rename/unlink are represented as interval upserts/deletes here.
    static std::vector<std::string> dirent_cols() {
        return {
            "parent_inode_id",
            "name",
            "inode_id",
            "created_at",
        };
    }

    // chronosfs_file_blocks is the record-level COW data table. External rows
    // can cover a large imported source-file range with data empty. Inline rows
    // are the actual Chronos-owned block payloads produced by writes. Both row
    // kinds are ordinary interval-versioned records; ChronosFS performs range
    // splitting before upsert so the interval backend remains generic.
    static std::vector<std::string> block_cols() {
        return {
            "inode_id",
            "byte_start",
            "byte_end",
            "external",
            "data",
        };
    }

    void validate_branch_pair(const ControlMergePath &spec) {
        if (!spec.valid) throw FsError(ENOENT, "invalid merge control path");
        auto names = branches();
        if (std::find(names.begin(), names.end(), spec.source) == names.end()) {
            throw FsError(ENOENT, "source branch not found");
        }
        if (std::find(names.begin(), names.end(), spec.target) == names.end()) {
            throw FsError(ENOENT, "target branch not found");
        }
    }

    std::unordered_map<std::int64_t, std::string> inode_paths_for_branch(const std::string &branch) {
        NativeChronosFS fs(store_, branch, block_size_);
        std::unordered_map<std::int64_t, std::string> paths;
        collect_inode_paths(fs, "/", paths);
        return paths;
    }

    void collect_inode_paths(
        NativeChronosFS &fs,
        const std::string &path,
        std::unordered_map<std::int64_t, std::string> &paths) {
        Inode inode = fs.stat_path(path);
        paths[inode.id] = path;
        if (inode.kind != "directory") return;
        for (const auto &name : fs.listdir(path)) {
            collect_inode_paths(fs, path_join(path, name), paths);
        }
    }

    std::string path_for_inode(
        std::int64_t inode_id,
        const std::unordered_map<std::int64_t, std::string> &source_paths,
        const std::unordered_map<std::int64_t, std::string> &target_paths) const {
        auto found_source = source_paths.find(inode_id);
        if (found_source != source_paths.end()) return found_source->second;
        auto found_target = target_paths.find(inode_id);
        if (found_target != target_paths.end()) return found_target->second;
        return "/.chronosfs/inode/" + std::to_string(inode_id);
    }

    Json row_payload_json(
        const std::string &table,
        const std::vector<std::string> &columns,
        const std::vector<IntervalValue> &values,
        const std::unordered_map<std::int64_t, std::string> &source_paths,
        const std::unordered_map<std::int64_t, std::string> &target_paths) const {
        if (values.empty()) return nullptr;
        Json out = Json::object();
        for (std::size_t i = 0; i < columns.size() && i < values.size(); ++i) {
            const std::string &column = columns[i];
            // These ids are implementation details for record-level COW.  The
            // public control plane reports paths and byte ranges instead.
            if (table.rfind("chronosfs_", 0) == 0 &&
                (column == "inode_id" || column == "parent_inode_id" ||
                 column == "byte_start" || column == "byte_end")) {
                continue;
            }
            out[column] = interval_value_to_json(values[i]);
        }
        if (table == "chronosfs_inodes") {
            std::int64_t inode_id = int_for_column(columns, values, "inode_id", 0);
            if (inode_id) out["path"] = path_for_inode(inode_id, source_paths, target_paths);
        } else if (table == "chronosfs_dirents") {
            std::int64_t parent_id = int_for_column(columns, values, "parent_inode_id", 0);
            std::int64_t child_id = int_for_column(columns, values, "inode_id", 0);
            const IntervalValue *name_value = value_for_column(columns, values, "name");
            std::string name = name_value ? as_string(*name_value) : std::string();
            std::string parent_path = path_for_inode(parent_id, source_paths, target_paths);
            out["parent_path"] = parent_path;
            out["path"] = child_id ? path_for_inode(child_id, source_paths, target_paths) : path_join(parent_path, name);
        }
        return out;
    }

    Json content_json(const IntervalBlob &blob, const std::string &diff = {}) const {
        if (looks_textual(blob)) {
            Json out = {
                {"encoding", "utf-8"},
                {"text", blob_text(blob)},
            };
            if (!diff.empty()) out["unified_diff"] = diff;
            return out;
        }
        return Json{
            {"encoding", "hex"},
            {"size", blob.size()},
            {"hex", hex_blob(blob)},
        };
    }

    Json file_block_diff_json(
        const NativeRowDiff &diff,
        const std::unordered_map<std::int64_t, std::string> &source_paths,
        const std::unordered_map<std::int64_t, std::string> &target_paths) const {
        std::int64_t inode_id = int_for_column(diff.key_columns, diff.key_values, "inode_id", 0);
        std::int64_t key_start = int_for_column(diff.key_columns, diff.key_values, "byte_start", 0);
        std::int64_t before_start = diff.has_before ? int_for_column(diff.columns, diff.before, "byte_start", key_start) : key_start;
        std::int64_t before_end = diff.has_before ? int_for_column(diff.columns, diff.before, "byte_end", key_start) : key_start;
        std::int64_t after_start = diff.has_after ? int_for_column(diff.columns, diff.after, "byte_start", key_start) : key_start;
        std::int64_t after_end = diff.has_after ? int_for_column(diff.columns, diff.after, "byte_end", key_start) : key_start;
        IntervalBlob before_blob = diff.has_before ? blob_for_column(diff.columns, diff.before, "data") : IntervalBlob{};
        IntervalBlob after_blob = diff.has_after ? blob_for_column(diff.columns, diff.after, "data") : IntervalBlob{};
        std::int64_t start = std::min(before_start, after_start);
        std::int64_t end = std::max(before_end, after_end);
        std::string path = path_for_inode(inode_id, source_paths, target_paths);
        std::string label = path + "@bytes:" + std::to_string(start) + "-" + std::to_string(end);
        std::string unified = simple_unified_diff("target:" + label, "source:" + label, before_blob, after_blob);

        return Json{
            {"table", "chronosfs_file_range"},
            {"key", Json{
                {"path", path},
                {"byte_range", Json{{"start", start}, {"end", end}}},
            }},
            {"change", diff.change},
            {"before", diff.has_before ? content_json(before_blob) : Json(nullptr)},
            {"after", diff.has_after ? content_json(after_blob, unified) : Json(nullptr)},
        };
    }

    Json row_diff_json(
        const NativeRowDiff &diff,
        const std::unordered_map<std::int64_t, std::string> &source_paths,
        const std::unordered_map<std::int64_t, std::string> &target_paths,
        bool include_conflict_id = false) const {
        if (diff.table == "chronosfs_file_blocks") {
            Json out = file_block_diff_json(diff, source_paths, target_paths);
            if (include_conflict_id) out["conflict_id"] = public_conflict_id(diff, source_paths, target_paths);
            return out;
        }
        Json key = Json::object();
        if (diff.table == "chronosfs_inodes") {
            std::int64_t inode_id = int_for_column(diff.key_columns, diff.key_values, "inode_id", 0);
            key["path"] = path_for_inode(inode_id, source_paths, target_paths);
        } else if (diff.table == "chronosfs_dirents") {
            std::int64_t parent_id = int_for_column(diff.key_columns, diff.key_values, "parent_inode_id", 0);
            const IntervalValue *name_value = value_for_column(diff.key_columns, diff.key_values, "name");
            std::string name = name_value ? as_string(*name_value) : std::string();
            std::string parent_path = path_for_inode(parent_id, source_paths, target_paths);
            key["parent_path"] = parent_path;
            key["name"] = name;
            key["path"] = path_join(parent_path, name);
        } else {
            for (std::size_t i = 0; i < diff.key_columns.size() && i < diff.key_values.size(); ++i) {
                key[diff.key_columns[i]] = interval_value_to_json(diff.key_values[i]);
            }
        }
        Json out = {
            {"table", diff.table},
            {"key", key},
            {"change", diff.change},
            {"before", diff.has_before ? row_payload_json(diff.table, diff.columns, diff.before, source_paths, target_paths) : Json(nullptr)},
            {"after", diff.has_after ? row_payload_json(diff.table, diff.columns, diff.after, source_paths, target_paths) : Json(nullptr)},
        };
        if (include_conflict_id) out["conflict_id"] = public_conflict_id(diff, source_paths, target_paths);
        return out;
    }

    std::string public_conflict_id(
        const NativeRowDiff &diff,
        const std::unordered_map<std::int64_t, std::string> &source_paths,
        const std::unordered_map<std::int64_t, std::string> &target_paths) const {
        return stable_control_id(row_diff_json(diff, source_paths, target_paths, false).dump());
    }

    std::string merge_preview_json(const std::string &path) {
        ControlMergePath spec = parse_control_merge_path(path, "/.chronos/merge-preview/");
        validate_branch_pair(spec);
        NativeMergePreview preview = store_->merge_preview(spec.source, spec.target);
        auto source_paths = inode_paths_for_branch(spec.source);
        auto target_paths = inode_paths_for_branch(spec.target);
        auto content_conflict_inodes = file_conflict_inodes(preview.conflicts);
        Json out = {
            {"source", spec.source},
            {"target", spec.target},
            {"changes", Json::array()},
            {"conflicts", Json::array()},
        };
        for (const auto &change : preview.changes) {
            out["changes"].push_back(row_diff_json(change, source_paths, target_paths, false));
        }
        for (const auto &conflict : preview.conflicts) {
            if (is_inode_diff(conflict) && content_conflict_inodes.count(diff_inode_id(conflict)) > 0) {
                // A file-content conflict and its inode metadata conflict are
                // the same agent-facing decision: choose source/target bytes
                // and carry the matching size/timestamp metadata with it.
                continue;
            }
            out["conflicts"].push_back(row_diff_json(conflict, source_paths, target_paths, true));
        }
        return out.dump();
    }

    void apply_control_merge(const std::string &path, const std::string &body) {
        ControlMergePath spec = parse_control_merge_path(path, "/.chronos/merge-apply/");
        validate_branch_pair(spec);
        NativeMergePreview preview = store_->merge_preview(spec.source, spec.target);
        auto source_paths = inode_paths_for_branch(spec.source);
        auto target_paths = inode_paths_for_branch(spec.target);
        auto content_conflict_inodes = file_conflict_inodes(preview.conflicts);
        Json request = Json::object();
        if (body.find_first_not_of(" \t\r\n") != std::string::npos) {
            request = Json::parse(body, nullptr, false);
            if (request.is_discarded() || !request.is_object()) {
                throw FsError(EINVAL, "invalid merge-apply JSON object");
            }
        }
        std::string policy;
        if (request.contains("policy")) {
            if (!request["policy"].is_string()) throw FsError(EINVAL, "merge policy must be a string");
            policy = request["policy"].get<std::string>();
        }
        std::unordered_map<std::string, std::string> choices;
        if (request.contains("conflicts")) {
            if (!request["conflicts"].is_object()) {
                throw FsError(EINVAL, "merge conflicts must be a JSON object");
            }
            for (auto it = request["conflicts"].begin(); it != request["conflicts"].end(); ++it) {
                if (!it.value().is_string()) {
                    throw FsError(EINVAL, "merge conflict choices must be strings");
                }
                choices[it.key()] = it.value().get<std::string>();
            }
        }
        if (policy.empty()) policy = choices.empty() ? "abort_on_conflict" : "manual_review";

        auto normalize_choice = [](const std::string &raw) -> std::string {
            if (raw == "source" || raw == "theirs") return "source";
            if (raw == "target" || raw == "ours" || raw == "skip") return "target";
            return raw;
        };
        auto matching_content_choice = [&](std::int64_t inode_id) -> std::string {
            std::string chosen;
            for (const auto &conflict : preview.conflicts) {
                if (!is_file_block_diff(conflict) || diff_inode_id(conflict) != inode_id) continue;
                std::string id = public_conflict_id(conflict, source_paths, target_paths);
                auto explicit_choice = choices.find(id);
                if (explicit_choice == choices.end()) continue;
                std::string normalized = normalize_choice(explicit_choice->second);
                if (normalized == "source") return normalized;
                if (normalized == "target") chosen = normalized;
            }
            return chosen;
        };

        std::vector<NativeMergeChange> changes;
        changes.reserve(preview.changes.size() + preview.conflicts.size());
        for (const auto &change : preview.changes) {
            changes.push_back(merge_change_from_diff(change));
        }

        std::unordered_map<std::string, bool> seen_conflicts;
        for (const auto &conflict : preview.conflicts) {
            std::string id = public_conflict_id(conflict, source_paths, target_paths);
            bool hidden_inode_conflict =
                is_inode_diff(conflict) && content_conflict_inodes.count(diff_inode_id(conflict)) > 0;
            std::string choice;
            auto explicit_choice = choices.find(id);
            if (!hidden_inode_conflict || explicit_choice != choices.end()) {
                seen_conflicts[id] = true;
            }
            if (policy == "source_wins" || policy == "weak_snapshot_isolation") {
                choice = "source";
            } else if (policy == "target_wins") {
                choice = "target";
            } else if (policy == "abort_on_conflict" || policy == "snapshot_isolation") {
                throw FsError(EINVAL, "merge has unresolved conflicts");
            } else if (explicit_choice != choices.end()) {
                choice = normalize_choice(explicit_choice->second);
            } else if (hidden_inode_conflict) {
                choice = matching_content_choice(diff_inode_id(conflict));
                if (choice.empty()) {
                    throw FsError(EINVAL, "merge conflict is unresolved: " + id);
                }
            } else {
                throw FsError(EINVAL, "merge conflict is unresolved: " + id);
            }

            if (choice == "source") {
                changes.push_back(merge_change_from_diff(conflict));
            } else if (choice == "target") {
                continue;
            } else {
                throw FsError(EINVAL, "unsupported merge conflict choice: " + choice);
            }
        }
        for (const auto &entry : choices) {
            if (seen_conflicts.find(entry.first) == seen_conflicts.end()) {
                throw FsError(EINVAL, "merge resolution is stale for conflict: " + entry.first);
            }
        }

        store_->apply_merge_changes(spec.source, spec.target, changes);
        session_ = store_->checkout(branch_id_);
        clear_metadata_cache();
    }

    void ensure_schema() {
        // ChronosFS is stored as three ordinary logical tables and then
        // registered with the native interval store:
        //   chronosfs_inodes      POSIX inode metadata, file size, and optional
        //                         source_path for lazy-imported file bytes
        //   chronosfs_dirents     directory edges from parent/name to child
        //   chronosfs_file_blocks byte-range content records. Large external
        //                         ranges point at inode.source_path; inline
        //                         ranges contain Chronos-owned block payloads.
        // Once registered, these logical tables are copied into physical
        // _chronos_b_interval_* tables and inherit the same branch/diff/merge
        // semantics as SQL user tables.
        const std::string blob_type = blob_sql_type(*store_);
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS chronosfs_inodes ("
            "inode_id BIGINT PRIMARY KEY, "
            "kind TEXT NOT NULL, "
            "mode INTEGER NOT NULL, "
            "uid INTEGER NOT NULL, "
            "gid INTEGER NOT NULL, "
            "size BIGINT NOT NULL, "
            "nlink INTEGER NOT NULL, "
            "symlink_target TEXT, "
            "atime TEXT NOT NULL, "
            "mtime TEXT NOT NULL, "
            "ctime TEXT NOT NULL, "
            "source_path TEXT)");
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS chronosfs_dirents ("
            "parent_inode_id BIGINT NOT NULL, "
            "name TEXT NOT NULL, "
            "inode_id BIGINT NOT NULL, "
            "created_at TEXT NOT NULL, "
            "PRIMARY KEY (parent_inode_id, name))");
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS chronosfs_file_blocks ("
            "inode_id BIGINT NOT NULL, "
            "byte_start BIGINT NOT NULL, "
            "byte_end BIGINT NOT NULL, "
            "external BOOLEAN NOT NULL, "
            "data " + blob_type + ", "
            "PRIMARY KEY (inode_id, byte_start), "
            "CHECK (byte_start < byte_end))");
    }

    void ensure_registered_table(
        const std::string &logical,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &column_defs,
        const std::vector<std::string> &pk_columns) {
        auto existing = store_->query_sql(
            "SELECT 1 FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = ? LIMIT 1",
            {logical});
        if (!existing.empty()) return;

        const std::string physical = "_chronos_b_interval_" + logical;
        const std::string interval_type = interval_sql_type(*store_);
        const std::string user_defs = [&] {
            std::string out;
            for (std::size_t i = 0; i < column_defs.size(); ++i) {
                if (i) out += ", ";
                out += column_defs[i];
            }
            return out;
        }();
        const std::string pk_sql = join_ident_list(pk_columns);
        // The FUSE store uses the same physical row layout as relational data:
        // user columns plus live_lo/live_hi/writer_segment_id/deleted.  That is
        // why ChronosFS does not maintain a separate branching algorithm.
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS " + quote_ident(physical) + " ("
            + user_defs + ", "
            "live_lo " + interval_type + " NOT NULL, "
            "live_hi " + interval_type + " NOT NULL, "
            "writer_segment_id INTEGER NOT NULL, "
            "deleted BOOLEAN NOT NULL DEFAULT FALSE, "
            "PRIMARY KEY (" + pk_sql + ", live_lo), "
            "CHECK (live_lo < live_hi))");
        store_->execute_sql(
            "CREATE INDEX IF NOT EXISTS " + quote_ident("idx_" + physical + "_pk_hi") +
            " ON " + quote_ident(physical) + " (" + pk_sql + ", live_hi)");
        store_->execute_sql(
            "CREATE INDEX IF NOT EXISTS " + quote_ident("idx_" + physical + "_writer_segment") +
            " ON " + quote_ident(physical) + " (writer_segment_id, " + pk_sql + ")");

        const std::string cols = join_ident_list(columns);
        store_->execute_sql(
            "INSERT INTO " + quote_ident(physical) +
            " (" + cols + ", live_lo, live_hi, writer_segment_id, deleted) "
            "SELECT " + cols + ", 0, ?, 1, FALSE FROM " + quote_ident(logical),
            {max_interval_value(*store_)});
        if (store_->dialect() == "postgres") {
            store_->execute_sql(
                "INSERT INTO _chronos_branch_tables "
                "(table_name, physical_table, pk_columns, columns, column_defs, backend) "
                "VALUES (?, ?, ?, ?, ?, 'interval') "
                "ON CONFLICT (table_name) DO NOTHING",
                {logical, physical, json_array(pk_columns), json_array(columns), json_array(column_defs)});
        } else {
            store_->execute_sql(
                "INSERT OR IGNORE INTO _chronos_branch_tables "
                "(table_name, physical_table, pk_columns, columns, column_defs, backend) "
                "VALUES (?, ?, ?, ?, ?, 'interval')",
                {logical, physical, json_array(pk_columns), json_array(columns), json_array(column_defs)});
        }
    }

    Inode ensure_directory_path(const std::string &path, mode_t mode) {
        if (path.empty() || path == "/") return inode_by_id(1);
        std::int64_t current = 1;
        std::string current_path;
        std::size_t start = 1;
        while (start < path.size()) {
            std::size_t slash = path.find('/', start);
            std::string part = path.substr(start, slash == std::string::npos ? std::string::npos : slash - start);
            current_path += "/" + part;
            auto entry = dirent(current, part);
            if (!entry.first) {
                Inode parent = inode_by_id(current);
                Inode child = mkdir_child(parent, part, mode);
                current = child.id;
                cache_path(current_path, current);
            } else {
                current = entry.second;
                Inode inode = inode_by_id(current);
                if (inode.kind != "directory") throw FsError(ENOTDIR, "not a directory");
            }
            if (slash == std::string::npos) break;
            start = slash + 1;
        }
        return inode_by_id(current);
    }

    Inode create_file_child(const Inode &parent, const std::string &name, mode_t mode) {
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
        std::int64_t id = allocate_inode();
        std::string now = now_text();
        Inode inode{id, "file", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now, ""};
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
        touch(parent.id);
        invalidate_directory_cache(parent.id);
        cache_inode(inode);
        dirent_cache_[dirent_key(parent.id, name)] = {true, inode.id};
        return inode;
    }

    Inode mkdir_child(const Inode &parent, const std::string &name, mode_t mode) {
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
        std::int64_t id = allocate_inode();
        std::string now = now_text();
        Inode inode{id, "directory", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now, ""};
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
        touch(parent.id);
        invalidate_directory_cache(parent.id);
        cache_inode(inode);
        dirent_cache_[dirent_key(parent.id, name)] = {true, inode.id};
        return inode;
    }

    Inode symlink_child(const Inode &parent, const std::string &name, const std::string &target) {
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
        std::int64_t id = allocate_inode();
        std::string now = now_text();
        Inode inode{id, "symlink", 0777, getuid(), getgid(), static_cast<std::int64_t>(target.size()), 1, target, now, now, now, ""};
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
        touch(parent.id);
        invalidate_directory_cache(parent.id);
        cache_inode(inode);
        dirent_cache_[dirent_key(parent.id, name)] = {true, inode.id};
        return inode;
    }

    Inode import_file_child(
        const Inode &parent,
        const std::string &name,
        const std::filesystem::path &source_path,
        const struct stat &st) {
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
        const std::int64_t id = allocate_inode();
        std::string now = now_text();
        Inode inode{
            id,
            "file",
            static_cast<mode_t>(st.st_mode & 07777),
            st.st_uid,
            st.st_gid,
            static_cast<std::int64_t>(st.st_size),
            1,
            "",
            now,
            time_text(st.st_mtim),
            time_text(st.st_ctim),
            source_path.string(),
        };
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
        if (inode.size > 0) {
            FileExtent external{
                inode.id,
                0,
                inode.size,
                true,
                IntervalBlob{},
            };
            upsert("chronosfs_file_blocks", block_cols(), {"inode_id", "byte_start"}, extent_values(external), false);
        }
        touch(parent.id);
        invalidate_directory_cache(parent.id);
        cache_inode(inode);
        dirent_cache_[dirent_key(parent.id, name)] = {true, inode.id};
        return inode;
    }

    void import_one_path(const std::filesystem::path &source_path, const std::string &target) {
        struct stat st {};
        if (::lstat(source_path.c_str(), &st) != 0) {
            throw FsError(errno, "could not stat ChronosFS import source");
        }
        auto [parent_path, name] = split_parent(target);
        Inode parent = ensure_directory_path(parent_path, 0755);
        if (S_ISLNK(st.st_mode)) {
            std::filesystem::path link_target = std::filesystem::read_symlink(source_path);
            symlink_child(parent, name, link_target.string());
            return;
        }
        if (S_ISDIR(st.st_mode)) {
            if (dirent(parent.id, name).first) {
                Inode existing = inode_by_id(dirent(parent.id, name).second);
                if (existing.kind != "directory") throw FsError(EEXIST, "path exists");
                return;
            }
            Inode inode = mkdir_child(parent, name, static_cast<mode_t>(st.st_mode & 07777));
            inode.uid = st.st_uid;
            inode.gid = st.st_gid;
            inode.mtime = time_text(st.st_mtim);
            inode.ctime = time_text(st.st_ctim);
            upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
            cache_inode(inode);
            return;
        }
        if (S_ISREG(st.st_mode)) {
            import_file_child(parent, name, std::filesystem::absolute(source_path).lexically_normal(), st);
        }
    }

    void truncate_inode(Inode inode, std::int64_t size) {
        if (size < 0) throw FsError(EINVAL, "negative truncate size");
        transaction([&] {
            std::string now = now_text();
            if (size < inode.size) {
                IntervalRows removed;
                IntervalRows replacements;
                auto extents = visible_extents_after(inode.id, size);
                removed.reserve(extents.size());
                replacements.reserve(extents.size());
                for (const auto &extent : extents) {
                    removed.push_back(extent_values(extent));
                    if (extent.byte_start < size) {
                        replacements.push_back(extent_values(trim_extent(extent, extent.byte_start, size)));
                    }
                }
                if (!removed.empty()) {
                    upsert_many("chronosfs_file_blocks", block_cols(), {"inode_id", "byte_start"}, removed, true);
                }
                if (!replacements.empty()) {
                    upsert_many("chronosfs_file_blocks", block_cols(), {"inode_id", "byte_start"}, replacements, false);
                }
            }
            inode.size = size;
            inode.mtime = now;
            inode.ctime = now;
            upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        });
        cache_inode(inode);
    }

    void queue_orphan_inode(std::int64_t inode_id) {
        const bool known_private =
            private_created_inodes_.find(inode_id) !=
            private_created_inodes_.end();
        if (store_->dialect() == "postgres") {
            if (known_private) {
                store_->execute_sql(
                    "INSERT INTO _chronosfs_private_orphan_inodes "
                    "(branch_id, inode_id) VALUES (?, ?) "
                    "ON CONFLICT DO NOTHING",
                    {branch_id_, inode_id});
            } else {
                store_->execute_sql(
                    "INSERT INTO _chronosfs_orphan_inodes "
                    "(branch_id, inode_id) VALUES (?, ?) "
                    "ON CONFLICT DO NOTHING",
                    {branch_id_, inode_id});
            }
        } else {
            if (known_private) {
                store_->execute_sql(
                    "INSERT OR IGNORE INTO "
                    "_chronosfs_private_orphan_inodes "
                    "(branch_id, inode_id) VALUES (?, ?)",
                    {branch_id_, inode_id});
            } else {
                store_->execute_sql(
                    "INSERT OR IGNORE INTO _chronosfs_orphan_inodes "
                    "(branch_id, inode_id) VALUES (?, ?)",
                    {branch_id_, inode_id});
            }
        }
    }

    std::string resolve_symlink_target(const std::string &path, const std::string &target) {
        if (!target.empty() && target.front() == '/') return target;
        auto [parent, _name] = split_parent(path);
        if (parent == "/") return "/" + target;
        return parent + "/" + target;
    }

    std::vector<std::vector<IntervalValue>> visible(
        const std::string &logical,
        const std::vector<std::string> &cols,
        const std::string &where,
        const std::vector<IntervalValue> &params,
        const std::string &suffix = "") {
        return session_.query_visible(logical, cols, where, params, suffix);
    }

    void upsert(
        const std::string &logical,
        const std::vector<std::string> &cols,
        const std::vector<std::string> &pk,
        const std::vector<IntervalValue> &row,
        bool deleted) {
        upsert_many(logical, cols, pk, IntervalRows{row}, deleted);
    }

    void upsert_many(
        const std::string &logical,
        const std::vector<std::string> &cols,
        const std::vector<std::string> &pk,
        const IntervalRows &rows,
        bool deleted) {
        if (deleted) session_.delete_rows(logical, cols, pk, rows);
        else session_.upsert_rows(logical, cols, pk, rows);
    }

    void begin_tx() {
        session_.begin();
    }

    void commit_tx() {
        session_.commit();
    }

    void rollback_tx() {
        try {
            session_.rollback();
        } catch (...) {
        }
    }

    template <typename Fn>
    void transaction(Fn &&fn) {
        if (session_.in_transaction()) {
            const std::string savepoint =
                "chronosfs_operation_" +
                std::to_string(++transaction_savepoint_id_);
            store_->execute_sql("SAVEPOINT " + savepoint, {});
            try {
                fn();
                store_->execute_sql("RELEASE SAVEPOINT " + savepoint, {});
            } catch (...) {
                try {
                    store_->execute_sql(
                        "ROLLBACK TO SAVEPOINT " + savepoint,
                        {});
                    store_->execute_sql(
                        "RELEASE SAVEPOINT " + savepoint,
                        {});
                } catch (...) {
                }
                throw;
            }
            return;
        }
        begin_tx();
        try {
            fn();
            commit_tx();
        } catch (...) {
            rollback_tx();
            throw;
        }
    }

    Inode inode_by_id(std::int64_t id) {
        auto cached = inode_cache_.find(id);
        if (cached != inode_cache_.end()) return cached->second;
        auto rows = visible("chronosfs_inodes", inode_cols(), "inode_id = ?", {id});
        if (rows.empty()) throw FsError(ENOENT, "inode not found");
        Inode inode = inode_from_row(rows[0]);
        cache_inode(inode);
        return inode;
    }

    void cache_inodes_by_id(const std::vector<std::int64_t> &inode_ids) {
        std::vector<std::int64_t> missing;
        missing.reserve(inode_ids.size());
        for (std::int64_t id : inode_ids) {
            if (inode_cache_.find(id) == inode_cache_.end()) missing.push_back(id);
        }
        if (missing.empty()) return;
        std::vector<IntervalValue> params;
        params.reserve(missing.size());
        for (std::int64_t id : missing) params.push_back(id);
        auto rows = visible(
            "chronosfs_inodes",
            inode_cols(),
            "inode_id IN (" + sql_placeholders(missing.size()) + ")",
            params);
        for (auto &row : rows) cache_inode(inode_from_row(row));
    }

    Inode inode_from_row(const std::vector<IntervalValue> &row) {
        return {
            as_int(row[0]),
            as_string(row[1]),
            static_cast<mode_t>(as_int(row[2])),
            static_cast<uid_t>(as_int(row[3])),
            static_cast<gid_t>(as_int(row[4])),
            as_int(row[5]),
            static_cast<nlink_t>(as_int(row[6])),
            as_string(row[7]),
            as_string(row[8]),
            as_string(row[9]),
            as_string(row[10]),
            row.size() > 11 ? as_string(row[11]) : std::string(),
        };
    }

    std::vector<IntervalValue> inode_values(const Inode &inode) {
        std::string now = now_text();
        return {
            inode.id,
            inode.kind,
            static_cast<std::int64_t>(inode.mode),
            static_cast<std::int64_t>(inode.uid),
            static_cast<std::int64_t>(inode.gid),
            inode.size,
            static_cast<std::int64_t>(inode.nlink),
            inode.symlink_target.empty() ? IntervalValue(std::monostate{}) : IntervalValue(inode.symlink_target),
            inode.atime.empty() ? now : inode.atime,
            inode.mtime.empty() ? now : inode.mtime,
            inode.ctime.empty() ? now : inode.ctime,
            inode.source_path.empty() ? IntervalValue(std::monostate{}) : IntervalValue(inode.source_path),
        };
    }

    bool try_inode_for_path(const std::string &path, Inode &result) {
        if (path == "/" || path.empty()) {
            result = inode_by_id(1);
            return true;
        }
        auto cached = path_inode_cache_.find(path);
        if (cached != path_inode_cache_.end()) {
            result = inode_by_id(cached->second);
            return true;
        }
        std::int64_t current = 1;
        std::string current_path;
        std::size_t start = 1;
        while (start < path.size()) {
            std::size_t slash = path.find('/', start);
            std::string part = path.substr(start, slash == std::string::npos ? std::string::npos : slash - start);
            current_path += "/" + part;
            auto path_cached = path_inode_cache_.find(current_path);
            if (path_cached != path_inode_cache_.end()) {
                current = path_cached->second;
            } else {
                auto entry = dirent(current, part);
                if (!entry.first) return false;
                current = entry.second;
                cache_path(current_path, current);
            }
            if (slash == std::string::npos) break;
            start = slash + 1;
        }
        result = inode_by_id(current);
        return true;
    }

    const Inode *try_inode_ref_for_path(const std::string &path) {
        const std::int64_t inode_id = [&] {
            if (path == "/" || path.empty()) return std::int64_t{1};
            auto path_cached = path_inode_cache_.find(path);
            return path_cached == path_inode_cache_.end()
                ? std::int64_t{0}
                : path_cached->second;
        }();
        if (inode_id != 0) {
            auto inode_cached = inode_cache_.find(inode_id);
            if (inode_cached != inode_cache_.end()) return &inode_cached->second;
        }

        Inode loaded;
        if (!try_inode_for_path(path, loaded)) return nullptr;
        auto inode_cached = inode_cache_.find(loaded.id);
        if (inode_cached == inode_cache_.end()) {
            cache_inode(loaded);
            inode_cached = inode_cache_.find(loaded.id);
        }
        return &inode_cached->second;
    }

    Inode inode_for_path(const std::string &path) {
        Inode inode;
        if (!try_inode_for_path(path, inode)) {
            throw FsError(ENOENT, "path not found");
        }
        return inode;
    }

    std::pair<bool, std::int64_t> dirent(std::int64_t parent, const std::string &name) {
        std::string key = dirent_key(parent, name);
        auto cached = dirent_cache_.find(key);
        if (cached != dirent_cache_.end()) return cached->second;
        if (loaded_dirents_.find(parent) != loaded_dirents_.end()) {
            return {false, 0};
        }
        auto rows = visible("chronosfs_dirents", {"inode_id"}, "parent_inode_id = ? AND name = ?", {parent, name});
        auto resolved = rows.empty() ? std::pair<bool, std::int64_t>{false, 0} : std::pair<bool, std::int64_t>{true, as_int(rows[0][0])};
        dirent_cache_[std::move(key)] = resolved;
        return resolved;
    }

    std::pair<std::string, std::string> split_parent(const std::string &path) {
        if (path.empty() || path == "/") throw FsError(EINVAL, "invalid root operation");
        std::size_t slash = path.find_last_of('/');
        return {slash == 0 ? "/" : path.substr(0, slash), path.substr(slash + 1)};
    }

    static std::string dirent_key(std::int64_t parent, const std::string &name) {
        return std::to_string(parent) + '\0' + name;
    }

    void cache_inode(const Inode &inode) {
        Inode cached = inode;
        cached.cached_atime = parse_time_text(cached.atime).tv_sec;
        cached.cached_mtime = parse_time_text(cached.mtime).tv_sec;
        cached.cached_ctime = parse_time_text(cached.ctime).tv_sec;
        cached.timestamps_cached = true;
        inode_cache_[cached.id] = std::move(cached);
    }

    void remember_created_path(const std::string &path, std::int64_t parent, const std::string &name, const Inode &inode) {
        invalidate_directory_cache(parent);
        cache_inode(inode);
        cache_path(path, inode.id);
        dirent_cache_[dirent_key(parent, name)] = {true, inode.id};
    }

    static std::string cached_parent_path(const std::string &path) {
        if (path.empty() || path == "/") return "";
        std::size_t slash = path.find_last_of('/');
        if (slash == 0) return "/";
        return path.substr(0, slash);
    }

    void cache_path(const std::string &path, std::int64_t inode_id) {
        if (path.empty()) return;
        path_inode_cache_[path] = inode_id;
        if (path != "/") {
            path_children_cache_[cached_parent_path(path)].insert(path);
        }
    }

    void forget_path(const std::string &path) {
        path_inode_cache_.erase(path);
        if (path != "/") {
            const std::string parent = cached_parent_path(path);
            auto parent_it = path_children_cache_.find(parent);
            if (parent_it != path_children_cache_.end()) {
                parent_it->second.erase(path);
                if (parent_it->second.empty()) path_children_cache_.erase(parent_it);
            }
        }
        path_children_cache_.erase(path);
    }

    void forget_path_tree(const std::string &path) {
        std::vector<std::string> stack{path};
        while (!stack.empty()) {
            std::string current = std::move(stack.back());
            stack.pop_back();

            auto children_it = path_children_cache_.find(current);
            if (children_it != path_children_cache_.end()) {
                stack.insert(stack.end(), children_it->second.begin(), children_it->second.end());
                path_children_cache_.erase(children_it);
            }

            path_inode_cache_.erase(current);
            if (current != "/") {
                const std::string parent = cached_parent_path(current);
                auto parent_it = path_children_cache_.find(parent);
                if (parent_it != path_children_cache_.end()) {
                    parent_it->second.erase(current);
                    if (parent_it->second.empty()) path_children_cache_.erase(parent_it);
                }
            }
        }
    }

    void clear_metadata_cache() {
        inode_cache_.clear();
        path_inode_cache_.clear();
        path_children_cache_.clear();
        dirent_cache_.clear();
        loaded_dirents_.clear();
        directory_children_cache_.clear();
    }

    void invalidate_directory_cache(std::int64_t inode_id) {
        loaded_dirents_.erase(inode_id);
        directory_children_cache_.erase(inode_id);
    }

    std::int64_t allocate_inode() {
        if (next_reserved_inode_id_ < reserved_inode_id_end_) {
            const std::int64_t inode_id = next_reserved_inode_id_++;
            private_created_inodes_.insert(inode_id);
            return inode_id;
        }
        constexpr std::int64_t kInodeReservationBatch = 128;
        // Allocate inode ids in batches under the store transaction.  Unused ids
        // may become gaps after a process exits, which is fine for POSIX inode
        // identity and avoids one allocator update per file creation.
        auto rows = store_->query_sql(
            "UPDATE _chronosfs_inode_allocator "
            "SET next_inode_id = next_inode_id + ? "
            "WHERE id = 1 "
            "RETURNING next_inode_id - ?",
            {kInodeReservationBatch, kInodeReservationBatch});
        if (rows.empty()) throw FsError(EIO, "inode allocator is missing");
        next_reserved_inode_id_ = as_int(rows[0][0]);
        reserved_inode_id_end_ = next_reserved_inode_id_ + kInodeReservationBatch;
        const std::int64_t inode_id = next_reserved_inode_id_++;
        private_created_inodes_.insert(inode_id);
        return inode_id;
    }

    FileExtent extent_from_row(const std::vector<IntervalValue> &row) const {
        return FileExtent{
            as_int(row[0]),
            as_int(row[1]),
            as_int(row[2]),
            as_int(row[3]) != 0,
            as_blob(row[4]),
        };
    }

    std::vector<IntervalValue> extent_values(const FileExtent &extent) const {
        return {
            extent.inode_id,
            extent.byte_start,
            extent.byte_end,
            std::int64_t{extent.external ? 1 : 0},
            extent.external ? IntervalValue(std::monostate{}) : IntervalValue(extent.data),
        };
    }

    FileExtent trim_extent(const FileExtent &extent, std::int64_t start, std::int64_t end) const {
        FileExtent out = extent;
        start = std::max(start, extent.byte_start);
        end = std::min(end, extent.byte_end);
        if (start >= end) throw FsError(EINVAL, "invalid extent trim");
        if (!extent.external) {
            const std::int64_t data_start = start - extent.byte_start;
            const std::int64_t data_end = end - extent.byte_start;
            IntervalBlob sliced;
            if (data_start < static_cast<std::int64_t>(extent.data.size())) {
                const std::int64_t clamped_end =
                    std::min<std::int64_t>(data_end, extent.data.size());
                sliced.insert(
                    sliced.end(),
                    extent.data.begin() + data_start,
                    extent.data.begin() + clamped_end);
            }
            sliced.resize(static_cast<std::size_t>(end - start), 0);
            out.data = std::move(sliced);
        } else {
            out.data.clear();
        }
        out.byte_start = start;
        out.byte_end = end;
        return out;
    }

    std::vector<FileExtent> visible_extents(std::int64_t inode_id, std::int64_t start, std::int64_t end) {
        if (start >= end) return {};
        auto rows = visible(
            "chronosfs_file_blocks",
            block_cols(),
            "inode_id = ? AND byte_start < ? AND byte_end > ?",
            {inode_id, end, start},
            "ORDER BY byte_start");
        std::vector<FileExtent> extents;
        extents.reserve(rows.size());
        for (auto &row : rows) extents.push_back(extent_from_row(row));
        return extents;
    }

    std::vector<FileExtent> visible_extents_after(std::int64_t inode_id, std::int64_t start) {
        auto rows = visible(
            "chronosfs_file_blocks",
            block_cols(),
            "inode_id = ? AND byte_end > ?",
            {inode_id, start},
            "ORDER BY byte_start");
        std::vector<FileExtent> extents;
        extents.reserve(rows.size());
        for (auto &row : rows) extents.push_back(extent_from_row(row));
        return extents;
    }

    std::vector<FileExtent> visible_fixed_block_extents(
        std::int64_t inode_id,
        std::int64_t start,
        std::int64_t end) {
        if (start >= end) return {};
        std::vector<std::int64_t> block_starts;
        std::int64_t block_start = (start / block_size_) * block_size_;
        const std::int64_t last_block = ((end - 1) / block_size_) * block_size_;
        while (block_start <= last_block) {
            block_starts.push_back(block_start);
            block_start += block_size_;
        }
        if (block_starts.empty()) return {};

        constexpr std::size_t kMaxExactBlockLookup = 512;
        if (block_starts.size() > kMaxExactBlockLookup) {
            return visible_extents(inode_id, start, end);
        }

        std::vector<IntervalValue> params;
        params.reserve(1 + block_starts.size());
        params.push_back(inode_id);
        for (std::int64_t value : block_starts) params.push_back(value);
        auto rows = visible(
            "chronosfs_file_blocks",
            block_cols(),
            "inode_id = ? AND byte_start IN (" + sql_placeholders(block_starts.size()) + ")",
            params,
            "ORDER BY byte_start");
        std::vector<FileExtent> extents;
        extents.reserve(rows.size());
        for (auto &row : rows) extents.push_back(extent_from_row(row));
        return extents;
    }

    IntervalBlob read_source_range(
        const Inode &inode,
        std::int64_t offset,
        std::int64_t size) const {
        if (size <= 0) return {};
        if (inode.source_path.empty()) {
            throw FsError(EIO, "external ChronosFS extent has no source path");
        }
        int fd = ::open(inode.source_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) throw FsError(errno, "could not open external ChronosFS source");
        IntervalBlob out(static_cast<std::size_t>(size), 0);
        std::int64_t done = 0;
        while (done < size) {
            ssize_t got = ::pread(
                fd,
                out.data() + done,
                static_cast<std::size_t>(size - done),
                static_cast<off_t>(offset + done));
            if (got < 0) {
                int err = errno;
                ::close(fd);
                throw FsError(err, "could not read external ChronosFS source");
            }
            if (got == 0) break;
            done += got;
        }
        ::close(fd);
        return out;
    }

    void append_extent_bytes(
        IntervalBlob &out,
        const Inode &inode,
        const FileExtent &extent,
        std::int64_t start,
        std::int64_t end) const {
        start = std::max(start, extent.byte_start);
        end = std::min(end, extent.byte_end);
        if (start >= end) return;
        const std::int64_t len = end - start;
        if (extent.external) {
            auto bytes = read_source_range(inode, start, len);
            out.insert(out.end(), bytes.begin(), bytes.end());
            return;
        }
        const std::int64_t data_start = start - extent.byte_start;
        std::int64_t copied = 0;
        if (data_start < static_cast<std::int64_t>(extent.data.size())) {
            const std::int64_t available = std::min<std::int64_t>(
                len,
                static_cast<std::int64_t>(extent.data.size()) - data_start);
            out.insert(
                out.end(),
                extent.data.begin() + data_start,
                extent.data.begin() + data_start + available);
            copied = available;
        }
        if (copied < len) out.insert(out.end(), len - copied, 0);
    }

    IntervalBlob read_inode_range(
        const Inode &inode,
        std::int64_t offset,
        std::int64_t size) {
        if (size <= 0 || offset >= inode.size) return {};
        std::int64_t end = std::min(inode.size, offset + size);
        auto extents = inode.source_path.empty()
            ? visible_fixed_block_extents(inode.id, offset, end)
            : visible_extents(inode.id, offset, end);
        IntervalBlob out;
        out.reserve(static_cast<std::size_t>(end - offset));
        std::int64_t cursor = offset;
        for (const auto &extent : extents) {
            if (extent.byte_start > cursor) {
                std::int64_t gap_end = std::min(extent.byte_start, end);
                out.insert(out.end(), gap_end - cursor, 0);
                cursor = gap_end;
            }
            if (cursor >= end) break;
            append_extent_bytes(out, inode, extent, cursor, end);
            cursor = std::max(cursor, std::min(extent.byte_end, end));
        }
        if (cursor < end) out.insert(out.end(), end - cursor, 0);
        return out;
    }

    IntervalRows replacement_rows_for_write(
        const Inode &inode,
        std::int64_t offset,
        const IntervalBlob &payload,
        std::int64_t &replace_start,
        std::int64_t &replace_end) {
        IntervalRows replacements;
        replace_start = (offset / block_size_) * block_size_;
        replace_end = replace_start;
        for (std::int64_t cursor = 0; cursor < static_cast<std::int64_t>(payload.size());) {
            const std::int64_t absolute = offset + cursor;
            const std::int64_t block_start = (absolute / block_size_) * block_size_;
            const std::int64_t block_offset = absolute - block_start;
            const std::int64_t take =
                std::min<std::int64_t>(payload.size() - cursor, block_size_ - block_offset);
            const bool full_block_write = block_offset == 0 && take == block_size_;
            const std::int64_t prior_valid = std::min<std::int64_t>(
                block_size_,
                std::max<std::int64_t>(0, inode.size - block_start));
            std::int64_t valid = std::max<std::int64_t>(block_offset + take, prior_valid);
            IntervalBlob current;
            if (!full_block_write && prior_valid > 0) {
                current = read_inode_range(inode, block_start, prior_valid);
            }
            current.resize(static_cast<std::size_t>(valid), 0);
            std::copy(
                payload.begin() + cursor,
                payload.begin() + cursor + take,
                current.begin() + block_offset);
            const std::int64_t byte_end = block_start + valid;
            replacements.push_back(
                {inode.id, block_start, byte_end, std::int64_t{0}, std::move(current)});
            replace_end = std::max(replace_end, byte_end);
            cursor += take;
        }
        return replacements;
    }

    IntervalRows fixed_block_replacement_rows_for_write(
        const Inode &inode,
        std::int64_t offset,
        const IntervalBlob &payload) {
        IntervalRows replacements;
        std::vector<std::int64_t> partial_starts;

        // Ordinary Chronos-owned files are stored as fixed-size byte ranges
        // whose keys are block-aligned byte_start values.  For these files we
        // can keep the old fast write behavior: full-block writes are blind
        // exact-key upserts, and only partial writes read existing touched
        // blocks to preserve bytes outside the write range.
        for (std::int64_t cursor = 0; cursor < static_cast<std::int64_t>(payload.size());) {
            const std::int64_t absolute = offset + cursor;
            const std::int64_t block_start = (absolute / block_size_) * block_size_;
            const std::int64_t block_offset = absolute - block_start;
            const std::int64_t take =
                std::min<std::int64_t>(payload.size() - cursor, block_size_ - block_offset);
            const bool full_block_write = block_offset == 0 && take == block_size_;
            const bool block_has_existing_bytes = block_start < inode.size;
            if (!full_block_write && block_has_existing_bytes) {
                partial_starts.push_back(block_start);
            }
            cursor += take;
        }

        std::unordered_map<std::int64_t, std::pair<IntervalBlob, std::int64_t>> blocks;
        if (!partial_starts.empty()) {
            std::vector<IntervalValue> params{inode.id};
            for (std::int64_t block_start : partial_starts) params.push_back(block_start);
            auto rows = visible(
                "chronosfs_file_blocks",
                {"byte_start", "byte_end", "external", "data"},
                "inode_id = ? AND byte_start IN (" + sql_placeholders(partial_starts.size()) + ")",
                params);
            blocks.reserve(rows.size());
            for (auto &row : rows) {
                const std::int64_t block_start = as_int(row[0]);
                const std::int64_t byte_end = as_int(row[1]);
                const bool external = as_int(row[2]) != 0;
                if (external) {
                    throw FsError(EIO, "external file extent is missing its source inode");
                }
                blocks[block_start] = std::make_pair(as_blob(row[3]), byte_end - block_start);
            }
        }

        for (std::int64_t cursor = 0; cursor < static_cast<std::int64_t>(payload.size());) {
            const std::int64_t absolute = offset + cursor;
            const std::int64_t block_start = (absolute / block_size_) * block_size_;
            const std::int64_t block_offset = absolute - block_start;
            const std::int64_t take =
                std::min<std::int64_t>(payload.size() - cursor, block_size_ - block_offset);
            IntervalBlob current;
            auto found = blocks.find(block_start);
            if (found != blocks.end()) current = found->second.first;
            const std::int64_t prior_valid = std::min<std::int64_t>(
                block_size_,
                std::max<std::int64_t>(0, inode.size - block_start));
            const std::int64_t valid = std::max<std::int64_t>(block_offset + take, prior_valid);
            current.resize(static_cast<std::size_t>(valid), 0);
            std::copy(
                payload.begin() + cursor,
                payload.begin() + cursor + take,
                current.begin() + block_offset);
            replacements.push_back(
                {inode.id, block_start, block_start + valid, std::int64_t{0}, std::move(current)});
            cursor += take;
        }
        return replacements;
    }

    void replace_inode_bytes(
        const Inode &inode,
        std::int64_t offset,
        const IntervalBlob &payload) {
        if (inode.source_path.empty()) {
            IntervalRows replacement_blocks =
                fixed_block_replacement_rows_for_write(inode, offset, payload);
            if (!replacement_blocks.empty()) {
                upsert_many("chronosfs_file_blocks", block_cols(), {"inode_id", "byte_start"}, replacement_blocks, false);
            }
            return;
        }

        std::int64_t replace_start = 0;
        std::int64_t replace_end = 0;
        IntervalRows replacement_blocks =
            replacement_rows_for_write(inode, offset, payload, replace_start, replace_end);

        IntervalRows removed;
        IntervalRows fragments;
        auto overlapped = visible_extents(inode.id, replace_start, replace_end);
        removed.reserve(overlapped.size());
        fragments.reserve(overlapped.size() * 2);
        for (const auto &extent : overlapped) {
            removed.push_back(extent_values(extent));
            if (extent.byte_start < replace_start) {
                fragments.push_back(extent_values(trim_extent(extent, extent.byte_start, replace_start)));
            }
            if (replace_end < extent.byte_end) {
                fragments.push_back(extent_values(trim_extent(extent, replace_end, extent.byte_end)));
            }
        }
        if (!removed.empty()) {
            upsert_many("chronosfs_file_blocks", block_cols(), {"inode_id", "byte_start"}, removed, true);
        }
        if (!fragments.empty()) {
            upsert_many("chronosfs_file_blocks", block_cols(), {"inode_id", "byte_start"}, fragments, false);
        }
        if (!replacement_blocks.empty()) {
            upsert_many("chronosfs_file_blocks", block_cols(), {"inode_id", "byte_start"}, replacement_blocks, false);
        }
    }

    void write_inode_at(
        Inode inode,
        std::int64_t offset,
        const IntervalBlob &payload) {
        if (payload.empty()) return;
        transaction([&] {
            replace_inode_bytes(inode, offset, payload);

            std::int64_t new_size = std::max<std::int64_t>(inode.size, offset + payload.size());
            if (new_size != inode.size) {
                // Size-extending writes update inode metadata in the same
                // logical operation.  Same-size overwrites avoid this inode
                // upsert so hot block workloads do not rewrite metadata.
                std::string now = now_text();
                inode.size = new_size;
                inode.mtime = now;
                inode.ctime = now;
                upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
            }
        });
        cache_inode(inode);
    }

    void touch(std::int64_t inode) {
        Inode row = inode_by_id(inode);
        std::string now = now_text();
        // FUSE currently reports second-resolution timestamps. Bulk file
        // creation can touch the same parent directory thousands of times
        // within one observable timestamp tick; rewriting that hot interval
        // row for every child adds no visible information. Keep the cached
        // inode current and persist at most one parent touch per second.
        if (parse_time_text(row.mtime).tv_sec == parse_time_text(now).tv_sec &&
            parse_time_text(row.ctime).tv_sec == parse_time_text(now).tv_sec) {
            row.mtime = now;
            row.ctime = now;
            cache_inode(row);
            return;
        }
        row.mtime = now;
        row.ctime = now;
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(row), false);
        cache_inode(row);
    }

    std::vector<std::string> branches() {
        return store_->branches();
    }

    void create_branch(const std::string &branch) {
        try {
            store_->create_branch(branch, branch_id_);
        } catch (const std::runtime_error &err) {
            throw FsError(EIO, err.what());
        }
    }

    static bool is_control(const std::string &path) {
        return path == "/.chronos" ||
               path == "/.chronos/current" ||
               path == "/.chronos/branches" ||
               path.rfind("/.chronos/branches/", 0) == 0 ||
               path == "/.chronos/merge-preview" ||
               path.rfind("/.chronos/merge-preview/", 0) == 0 ||
               path == "/.chronos/merge-apply" ||
               path.rfind("/.chronos/merge-apply/", 0) == 0;
    }
    static std::string control_branch_name(const std::string &path) {
        const std::string prefix = "/.chronos/branches/";
        return path.rfind(prefix, 0) == 0 ? path.substr(prefix.size()) : std::string();
    }
    void control_stat(const std::string &path, struct stat *st) {
        if (!control_branch_name(path).empty()) {
            auto names = branches();
            std::string name = control_branch_name(path);
            if (std::find(names.begin(), names.end(), name) == names.end()) throw FsError(ENOENT, "branch not found");
        }
        ControlMergePath preview_spec = parse_control_merge_path(path, "/.chronos/merge-preview/");
        ControlMergePath apply_spec = parse_control_merge_path(path, "/.chronos/merge-apply/");
        if ((path.rfind("/.chronos/merge-preview/", 0) == 0 && !preview_spec.valid) ||
            (path.rfind("/.chronos/merge-apply/", 0) == 0 && !apply_spec.valid)) {
            throw FsError(ENOENT, "invalid merge control path");
        }
        if (preview_spec.valid) validate_branch_pair(preview_spec);
        if (apply_spec.valid) validate_branch_pair(apply_spec);
        std::memset(st, 0, sizeof(struct stat));
        st->st_uid = getuid();
        st->st_gid = getgid();
        st->st_blksize = 4096;
        if (path == "/.chronos/current") {
            st->st_mode = S_IFREG | 0644;
            st->st_size = branch_id_.size() + 1;
        } else if (preview_spec.valid) {
            st->st_mode = S_IFREG | 0444;
            st->st_size = static_cast<off_t>(merge_preview_json(path).size());
        } else if (apply_spec.valid) {
            st->st_mode = S_IFREG | 0222;
            st->st_size = 0;
        } else {
            st->st_mode = S_IFDIR | 0755;
            st->st_nlink = 2;
        }
    }
    void fill_stat(const Inode &inode, struct stat *st) {
        std::memset(st, 0, sizeof(struct stat));
        st->st_ino = inode.id;
        st->st_uid = inode.uid;
        st->st_gid = inode.gid;
        st->st_size = inode.size;
        st->st_nlink = inode.nlink;
        st->st_blksize = 4096;
        st->st_blocks = (inode.size + 511) / 512;
        if (inode.timestamps_cached) {
            st->st_atim = {inode.cached_atime, 0};
            st->st_mtim = {inode.cached_mtime, 0};
            st->st_ctim = {inode.cached_ctime, 0};
        } else {
            st->st_atim = parse_time_text(inode.atime);
            st->st_mtim = parse_time_text(inode.mtime);
            st->st_ctim = parse_time_text(inode.ctime);
        }
        if (inode.kind == "directory") st->st_mode = S_IFDIR | inode.mode;
        else if (inode.kind == "symlink") st->st_mode = S_IFLNK | inode.mode;
        else st->st_mode = S_IFREG | inode.mode;
    }

    std::shared_ptr<chronos::native::NativeBranchStore> store_;
    std::string branch_id_;
    chronos::native::NativeBranchSession session_;
    std::int64_t block_size_;
    std::string object_dir_;
    std::unordered_set<std::string> pending_object_paths_;
    std::uint64_t transaction_savepoint_id_ = 0;
    std::int64_t next_reserved_inode_id_ = 0;
    std::int64_t reserved_inode_id_end_ = 0;
    ankerl::unordered_dense::map<std::int64_t, Inode> inode_cache_;
    ankerl::unordered_dense::map<std::string, std::int64_t> path_inode_cache_;
    std::unordered_map<std::string, std::unordered_set<std::string>> path_children_cache_;
    std::unordered_map<std::string, std::pair<bool, std::int64_t>> dirent_cache_;
    std::unordered_set<std::int64_t> loaded_dirents_;
    std::unordered_map<std::int64_t, std::vector<DirectoryEntry>>
        directory_children_cache_;
    std::unordered_set<std::int64_t> private_created_inodes_;
};

struct SharedChronosFSBranch {
    struct CacheInvalidation {
        std::uint64_t generation;
        bool clear_all;
        std::vector<std::string> paths;
    };

    static constexpr std::size_t kMaxInvalidations = 4096;

    std::shared_mutex mutex;
    std::atomic<std::uint64_t> cache_generation{1};
    std::deque<CacheInvalidation> cache_invalidations;
    std::unique_ptr<NativeChronosFS> filesystem;
    std::mutex batch_mutex;
    std::condition_variable batch_condition;
    std::chrono::steady_clock::time_point last_batched_write;
    std::size_t batched_operations = 0;
    std::atomic<bool> batch_pending{false};
    bool batch_shutdown = false;
    std::exception_ptr batch_error;
    std::thread batch_thread;

    SharedChronosFSBranch(
        const std::string &database_url,
        const std::string &branch_id,
        std::int64_t block_size)
        : filesystem(std::make_unique<NativeChronosFS>(
              database_url,
              branch_id,
              block_size)),
          batch_thread(&SharedChronosFSBranch::run_batch_committer, this) {}

    ~SharedChronosFSBranch() {
        {
            std::lock_guard<std::mutex> guard(batch_mutex);
            batch_shutdown = true;
        }
        batch_condition.notify_all();
        if (batch_thread.joinable()) batch_thread.join();
        std::unique_lock<std::shared_mutex> guard(mutex);
        try {
            commit_write_batch_locked();
        } catch (...) {
        }
    }

    void begin_write_batch_locked() {
        {
            std::lock_guard<std::mutex> guard(batch_mutex);
            if (batch_error) std::rethrow_exception(batch_error);
        }
        filesystem->begin_write_batch_public();
        bool commit_now = false;
        {
            std::lock_guard<std::mutex> guard(batch_mutex);
            batch_pending.store(true, std::memory_order_release);
            last_batched_write = std::chrono::steady_clock::now();
            commit_now = ++batched_operations >= 512;
        }
        if (commit_now) {
            commit_write_batch_locked();
        } else {
            batch_condition.notify_one();
        }
    }

    void commit_write_batch_locked() {
        bool pending = false;
        {
            std::lock_guard<std::mutex> guard(batch_mutex);
            if (batch_error) std::rethrow_exception(batch_error);
            pending =
                batch_pending.exchange(false, std::memory_order_acq_rel);
            batched_operations = 0;
        }
        if (!pending) return;
        try {
            filesystem->commit_write_batch_public();
        } catch (...) {
            filesystem->rollback_write_batch_public();
            throw;
        }
    }

    void commit_write_batch() {
        std::unique_lock<std::shared_mutex> guard(mutex);
        commit_write_batch_locked();
    }

    void run_batch_committer() {
        constexpr auto kWriteBatchIdle = std::chrono::milliseconds(2);
        while (true) {
            std::unique_lock<std::mutex> timer_lock(batch_mutex);
            batch_condition.wait(timer_lock, [this] {
                return batch_pending.load(std::memory_order_acquire) ||
                    batch_shutdown;
            });
            if (batch_shutdown) {
                timer_lock.unlock();
                try {
                    commit_write_batch();
                } catch (...) {
                }
                return;
            }
            const auto observed_write = last_batched_write;
            if (batch_condition.wait_until(
                    timer_lock,
                    observed_write + kWriteBatchIdle,
                    [this, observed_write] {
                        return batch_shutdown ||
                            last_batched_write != observed_write;
                    })) {
                continue;
            }
            timer_lock.unlock();
            try {
                commit_write_batch();
            } catch (...) {
                std::lock_guard<std::mutex> guard(batch_mutex);
                batch_error = std::current_exception();
                batch_pending.store(false, std::memory_order_release);
                batched_operations = 0;
            }
        }
    }

    bool has_pending_write_batch() const {
        return batch_pending.load(std::memory_order_acquire);
    }

    void record_path_invalidation(std::vector<std::string> paths) {
        const std::uint64_t generation =
            cache_generation.fetch_add(1, std::memory_order_release) + 1;
        cache_invalidations.push_back(
            CacheInvalidation{generation, false, std::move(paths)});
        while (cache_invalidations.size() > kMaxInvalidations) {
            cache_invalidations.pop_front();
        }
    }

    void record_full_invalidation() {
        const std::uint64_t generation =
            cache_generation.fetch_add(1, std::memory_order_release) + 1;
        cache_invalidations.push_back(CacheInvalidation{generation, true, {}});
        while (cache_invalidations.size() > kMaxInvalidations) {
            cache_invalidations.pop_front();
        }
    }

    void refresh_reader_cache(
        NativeChronosFS &reader,
        std::uint64_t &reader_generation) const {
        const std::uint64_t current =
            cache_generation.load(std::memory_order_acquire);
        if (reader_generation == current) return;

        if (cache_invalidations.empty() ||
            reader_generation + 1 < cache_invalidations.front().generation) {
            reader.clear_cache_public();
            reader_generation = current;
            return;
        }

        for (const auto &invalidation : cache_invalidations) {
            if (invalidation.generation <= reader_generation) continue;
            if (invalidation.clear_all) {
                reader.clear_cache_public();
            } else {
                reader.invalidate_cached_paths_public(invalidation.paths);
            }
        }
        reader_generation = current;
    }
};

struct FuseState;

struct SharedChronosFSBackend {
    std::string database_url;
    std::int64_t block_size;
    std::mutex registry_mutex;
    std::mutex mounts_mutex;
    std::shared_mutex topology_mutex;
    std::unordered_map<std::string, std::weak_ptr<SharedChronosFSBranch>> branches;
    std::unordered_set<FuseState *> mounts;

    std::shared_ptr<SharedChronosFSBranch> branch_for(const std::string &branch_id) {
        std::lock_guard<std::mutex> guard(registry_mutex);
        auto found = branches.find(branch_id);
        if (found != branches.end()) {
            if (auto existing = found->second.lock()) return existing;
            branches.erase(found);
        }
        auto state = std::make_shared<SharedChronosFSBranch>(
            database_url,
            branch_id,
            block_size);
        branches.emplace(branch_id, state);
        return state;
    }

    void flush_all_write_batches() {
        std::vector<std::shared_ptr<SharedChronosFSBranch>> snapshot;
        {
            std::lock_guard<std::mutex> guard(registry_mutex);
            snapshot.reserve(branches.size());
            for (auto it = branches.begin(); it != branches.end();) {
                if (auto state = it->second.lock()) {
                    snapshot.push_back(std::move(state));
                    ++it;
                } else {
                    it = branches.erase(it);
                }
            }
        }
        for (const auto &state : snapshot) {
            state->commit_write_batch();
        }
    }

    void clear_all_caches() {
        std::vector<std::shared_ptr<SharedChronosFSBranch>> snapshot;
        {
            std::lock_guard<std::mutex> guard(registry_mutex);
            snapshot.reserve(branches.size());
            for (auto it = branches.begin(); it != branches.end();) {
                if (auto state = it->second.lock()) {
                    snapshot.push_back(std::move(state));
                    ++it;
                } else {
                    it = branches.erase(it);
                }
            }
        }
        for (const auto &state : snapshot) {
            std::unique_lock<std::shared_mutex> guard(state->mutex);
            state->commit_write_batch_locked();
            state->filesystem->clear_cache_public();
            state->filesystem->clear_private_inode_hints_public();
            state->record_full_invalidation();
        }
    }

    void register_mount(FuseState *state);
    void unregister_mount(FuseState *state);
    void invalidate_paths(
        const std::string &branch_id,
        const std::vector<std::string> &paths);
};

struct FuseReadSlot {
    std::mutex mutex;
    std::string branch_id;
    std::uint64_t cache_generation = 0;
    std::unique_ptr<NativeChronosFS> filesystem;
};

struct FuseState {
    std::shared_ptr<SharedChronosFSBackend> backend;
    std::string branch_id;
    std::shared_ptr<SharedChronosFSBranch> branch;
    std::vector<std::unique_ptr<FuseReadSlot>> read_slots;
    std::mutex mutable_paths_mutex;
    std::unordered_set<std::string> mutable_paths;
    struct fuse *fuse = nullptr;
};

void SharedChronosFSBackend::register_mount(FuseState *state) {
    std::lock_guard<std::mutex> guard(mounts_mutex);
    mounts.insert(state);
}

void SharedChronosFSBackend::unregister_mount(FuseState *state) {
    std::lock_guard<std::mutex> guard(mounts_mutex);
    mounts.erase(state);
}

void SharedChronosFSBackend::invalidate_paths(
    const std::string &branch_id,
    const std::vector<std::string> &paths) {
    std::lock_guard<std::mutex> guard(mounts_mutex);
    for (FuseState *state : mounts) {
        if (state->branch_id != branch_id || state->fuse == nullptr) continue;
        for (const auto &path : paths) {
            if (path.empty()) continue;
            {
                std::lock_guard<std::mutex> mutable_guard(
                    state->mutable_paths_mutex);
                state->mutable_paths.insert(path);
            }
            fuse_invalidate_path(state->fuse, path.c_str());
        }
    }
}

std::unordered_map<std::string, std::weak_ptr<SharedChronosFSBackend>> &shared_backends() {
    static std::unordered_map<std::string, std::weak_ptr<SharedChronosFSBackend>> backends;
    return backends;
}

std::mutex &shared_backends_mutex() {
    static std::mutex mutex;
    return mutex;
}

std::string backend_key(const std::string &database_url, std::int64_t block_size) {
    return database_url + '\0' + std::to_string(block_size);
}

std::shared_ptr<SharedChronosFSBackend> shared_backend_for(
    const std::string &database_url,
    std::int64_t block_size) {
    std::lock_guard<std::mutex> guard(shared_backends_mutex());
    auto key = backend_key(database_url, block_size);
    auto found = shared_backends().find(key);
    if (found != shared_backends().end()) {
        if (auto existing = found->second.lock()) return existing;
    }
    auto backend = std::make_shared<SharedChronosFSBackend>();
    backend->database_url = database_url;
    backend->block_size = block_size;
    shared_backends()[std::move(key)] = backend;
    return backend;
}

void flush_chronosfs_native(
    const std::string &database_url,
    std::int64_t block_size) {
    std::shared_ptr<SharedChronosFSBackend> backend;
    {
        std::lock_guard<std::mutex> guard(shared_backends_mutex());
        auto found =
            shared_backends().find(backend_key(database_url, block_size));
        if (found != shared_backends().end()) {
            backend = found->second.lock();
        }
    }
    if (backend) backend->flush_all_write_batches();
}

py::dict inode_dict(const Inode &inode) {
    py::dict out;
    out["inode_id"] = inode.id;
    out["kind"] = inode.kind;
    out["mode"] = static_cast<std::int64_t>(inode.mode);
    out["uid"] = static_cast<std::int64_t>(inode.uid);
    out["gid"] = static_cast<std::int64_t>(inode.gid);
    out["size"] = inode.size;
    out["nlink"] = static_cast<std::int64_t>(inode.nlink);
    if (inode.symlink_target.empty()) out["symlink_target"] = py::none();
    else out["symlink_target"] = inode.symlink_target;
    out["atime"] = inode.atime;
    out["mtime"] = inode.mtime;
    out["ctime"] = inode.ctime;
    return out;
}

IntervalBlob blob_from_py(const py::object &data) {
    if (py::isinstance<py::bytes>(data) || py::isinstance<py::bytearray>(data)) {
        py::bytes bytes = py::reinterpret_borrow<py::bytes>(data);
        std::string raw = bytes;
        return IntervalBlob(raw.begin(), raw.end());
    }
    std::string text = py::str(data);
    return IntervalBlob(text.begin(), text.end());
}

class NativeChronosFSStoreApi {
  public:
    NativeChronosFSStoreApi(std::string database_url, std::int64_t block_size)
        : database_url_(std::move(database_url)),
          store_(std::make_shared<chronos::native::NativeBranchStore>(database_url_)),
          block_size_(block_size),
          object_dir_(chronosfs_object_dir(database_url_)),
          gc_thread_(&NativeChronosFSStoreApi::run_gc_worker, this) {
        if (store_->dialect() == "sqlite") {
            // ChronosFS writes one logical filesystem operation through many
            // interval rows. SQLite's connection-local auto-checkpoint can
            // otherwise make an unrelated later metadata write copy the
            // entire accumulated WAL synchronously. Checkpoint lifecycle is
            // managed outside the foreground operation path.
            store_->execute_sql("PRAGMA wal_autocheckpoint=0", {});
        }
    }

    ~NativeChronosFSStoreApi() {
        {
            std::lock_guard<std::mutex> guard(gc_mutex_);
            gc_shutdown_ = true;
        }
        gc_condition_.notify_all();
        if (gc_thread_.joinable()) gc_thread_.join();
    }

    void ensure() {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for("main").ensure();
    }

    std::vector<std::string> branches() {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return store_->branches();
    }

    void create_branch(const std::string &branch_id, const std::string &from_branch) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        store_->create_branch(branch_id, from_branch);
        clear_all_caches();
    }

    void delete_branch(const std::string &branch_id) {
        {
            std::lock_guard<std::recursive_mutex> guard(mutex_);
            store_->delete_branch(branch_id);
            filesystems_.erase(branch_id);
            clear_all_caches();
        }
        request_gc();
    }

    void wait_for_gc() {
        std::unique_lock<std::mutex> lock(gc_mutex_);
        gc_condition_.wait(lock, [this]() {
            return gc_stopped_ || (!gc_pending_ && !gc_running_);
        });
        if (gc_error_) std::rethrow_exception(gc_error_);
    }

    std::int64_t merge_apply(const std::string &source, const std::string &target) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        const auto started = std::chrono::steady_clock::now();
        const std::vector<std::int64_t> orphan_inodes =
            fs_for(source).orphan_inode_ids();
        const auto collected = std::chrono::steady_clock::now();
        std::int64_t applied =
            store_->merge_apply_excluding_first_key_values(
                source,
                target,
                "chronosfs_file_blocks",
                orphan_inodes
            );
        const auto merged = std::chrono::steady_clock::now();
        if (const char *profile = std::getenv("CHRONOS_WORKSPACE_PROFILE");
            profile != nullptr && std::string(profile) == "1") {
            const auto milliseconds = [](auto begin, auto end) {
                return std::chrono::duration<double, std::milli>(
                           end - begin)
                    .count();
            };
            std::cerr
                << "{\"chronosfs_merge_profile\":{\"collect_orphans_ms\":"
                << milliseconds(started, collected)
                << ",\"orphan_inodes\":" << orphan_inodes.size()
                << ",\"interval_merge_ms\":"
                << milliseconds(collected, merged)
                << "},\"source\":\"" << source
                << "\",\"target\":\"" << target << "\"}\n";
        }
        clear_all_caches();
        return applied;
    }

    py::dict stat(const std::string &branch, const std::string &path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return inode_dict(fs_for(branch).stat_path(path));
    }

    py::dict stat_inode(const std::string &branch, std::int64_t inode_id) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return inode_dict(fs_for(branch).stat_inode_id(inode_id));
    }

    py::dict lookup_child(const std::string &branch, std::int64_t parent_inode_id, const std::string &name) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return inode_dict(fs_for(branch).lookup_child_inode(parent_inode_id, name));
    }

    bool exists(const std::string &branch, const std::string &path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).exists_path(path);
    }

    std::vector<std::string> listdir(const std::string &branch, const std::string &path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).listdir(path);
    }

    std::vector<std::string> listdir_inode(const std::string &branch, std::int64_t inode_id) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).listdir_inode_id(inode_id);
    }

    py::bytes read_file(const std::string &branch, const std::string &path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        auto data = fs_for(branch).read_file_path(path);
        return py::bytes(reinterpret_cast<const char *>(data.data()), data.size());
    }

    py::bytes read_inode_range(const std::string &branch, std::int64_t inode_id, std::int64_t offset, std::int64_t size) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        auto data = fs_for(branch).read_inode_range_public(inode_id, offset, size);
        return py::bytes(reinterpret_cast<const char *>(data.data()), data.size());
    }

    std::int64_t write_file(const std::string &branch, const std::string &path, const py::object &data, mode_t mode, bool parents) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        auto &fs = fs_for(branch);
        fs.write_file_path(path, blob_from_py(data), mode, parents);
        return fs.stat_path(path).id;
    }

    void write_files(
        const std::string &branch,
        const std::vector<std::string> &paths,
        const py::list &data,
        mode_t mode,
        bool parents) {
        if (paths.size() != data.size()) {
            throw py::value_error("path and data counts differ");
        }
        std::vector<IntervalBlob> payloads;
        payloads.reserve(paths.size());
        for (const auto &item : data) {
            payloads.push_back(blob_from_py(py::reinterpret_borrow<py::object>(item)));
        }
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).write_files_public(paths, payloads, mode, parents);
    }

    void import_tree(const std::string &branch, const std::string &source_path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).import_tree_public(source_path);
        clear_all_caches();
    }

    void write_at(const std::string &branch, const std::string &path, std::int64_t offset, const py::object &data) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).write_at_path(path, offset, blob_from_py(data));
    }

    void write_inode_at(const std::string &branch, std::int64_t inode_id, std::int64_t offset, const py::object &data) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).write_inode_at_public(inode_id, offset, blob_from_py(data));
    }

    void truncate(const std::string &branch, const std::string &path, std::int64_t size) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).truncate(path, size);
    }

    void truncate_inode(const std::string &branch, std::int64_t inode_id, std::int64_t size) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).truncate_inode_public(inode_id, size);
    }

    void chmod(const std::string &branch, const std::string &path, mode_t mode) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).chmod_path(path, mode);
    }

    void chmod_inode(const std::string &branch, std::int64_t inode_id, mode_t mode) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).chmod_inode_public(inode_id, mode);
    }

    std::int64_t mkdir(const std::string &branch, const std::string &path, mode_t mode, bool parents) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).mkdir_path(path, mode, parents);
    }

    std::int64_t create_file_at(const std::string &branch, std::int64_t parent_inode_id, const std::string &name, mode_t mode) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).create_file_at_public(parent_inode_id, name, mode);
    }

    std::int64_t mkdir_at(const std::string &branch, std::int64_t parent_inode_id, const std::string &name, mode_t mode) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).mkdir_at_public(parent_inode_id, name, mode);
    }

    std::int64_t symlink(const std::string &branch, const std::string &target, const std::string &link_path, bool parents) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).symlink_path_public(target, link_path, parents);
    }

    std::int64_t symlink_at(const std::string &branch, std::int64_t parent_inode_id, const std::string &name, const std::string &target) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).symlink_at_public(parent_inode_id, name, target);
    }

    std::string readlink(const std::string &branch, const std::string &path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        return fs_for(branch).readlink_path(path);
    }

    void unlink(const std::string &branch, const std::string &path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).unlink_path(path, false);
    }

    void unlink_at(const std::string &branch, std::int64_t parent_inode_id, const std::string &name) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).unlink_at_public(parent_inode_id, name, false);
    }

    void rmdir(const std::string &branch, const std::string &path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).unlink_path(path, true);
    }

    void rmdir_at(const std::string &branch, std::int64_t parent_inode_id, const std::string &name) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).unlink_at_public(parent_inode_id, name, true);
    }

    void rename(const std::string &branch, const std::string &old_path, const std::string &new_path) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).rename_path(old_path, new_path);
    }

    void rename_at(
        const std::string &branch,
        std::int64_t old_parent_inode_id,
        const std::string &old_name,
        std::int64_t new_parent_inode_id,
        const std::string &new_name) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).rename_at_public(old_parent_inode_id, old_name, new_parent_inode_id, new_name);
    }

    void clear_cache(const std::string &branch) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fs_for(branch).clear_cache_public();
    }

  private:
    void collect_unreferenced_objects(
        chronos::native::NativeBranchStore &gc_store) {
        if (object_dir_.empty()) return;

        std::unordered_set<std::string> physical_tables;
        auto current_tables = gc_store.query_sql(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = "
            "'chronosfs_inodes'",
            {});
        for (const auto &row : current_tables) {
            if (!row.empty()) physical_tables.insert(as_string(row[0]));
        }
        try {
            auto version_tables = gc_store.query_sql(
                "SELECT physical_table "
                "FROM _chronos_branch_table_schema_versions "
                "WHERE backend = 'interval' AND table_name = "
                "'chronosfs_inodes'",
                {});
            for (const auto &row : version_tables) {
                if (!row.empty()) physical_tables.insert(as_string(row[0]));
            }
        } catch (...) {
            // Schema branching is optional, and older databases may not have
            // its registry table.
        }

        std::unordered_set<std::string> referenced_paths;
        for (const auto &table : physical_tables) {
            if (table.empty()) continue;
            auto rows = gc_store.query_sql(
                "SELECT DISTINCT source_path FROM " + quote_ident(table) +
                    " WHERE source_path IS NOT NULL AND source_path <> '' "
                    "AND deleted = FALSE",
                {});
            for (const auto &row : rows) {
                if (!row.empty()) referenced_paths.insert(as_string(row[0]));
            }
        }

        const std::filesystem::path object_root =
            std::filesystem::path(object_dir_).lexically_normal();
        auto registered =
            gc_store.query_sql("SELECT path FROM _chronosfs_objects", {});
        for (const auto &row : registered) {
            if (row.empty()) continue;
            const std::string path = as_string(row[0]);
            if (path.empty() ||
                referenced_paths.find(path) != referenced_paths.end()) {
                continue;
            }

            const std::filesystem::path candidate =
                std::filesystem::path(path).lexically_normal();
            if (candidate.parent_path() != object_root) {
                // The registry is private to generated ChronosFS objects.
                // Refuse to unlink an unexpected path even if the registry is
                // corrupted.
                continue;
            }
            if (::unlink(candidate.c_str()) != 0 && errno != ENOENT) {
                throw FsError(
                    errno,
                    "could not remove unreferenced ChronosFS object");
            }
            gc_store.execute_sql(
                "DELETE FROM _chronosfs_objects WHERE path = ?",
                {path});
        }
    }

    void request_gc() {
        {
            std::lock_guard<std::mutex> guard(gc_mutex_);
            if (!gc_stopped_) gc_pending_ = true;
        }
        gc_condition_.notify_one();
    }

    void run_gc_worker() {
        try {
            chronos::native::NativeBranchStore gc_store(database_url_);
            while (true) {
                {
                    std::unique_lock<std::mutex> lock(gc_mutex_);
                    gc_condition_.wait(lock, [this]() {
                        return gc_pending_ || gc_shutdown_;
                    });
                    if (gc_shutdown_ && !gc_pending_) return;
                    gc_pending_ = false;
                    gc_running_ = true;
                }
                try {
                    gc_store.collect_interval_garbage();
                    collect_unreferenced_objects(gc_store);
                    gc_store.execute_sql(
                        "DELETE FROM _chronosfs_orphan_inodes "
                        "WHERE branch_id NOT IN ("
                        "SELECT branch_id "
                        "FROM _chronos_branch_interval_branches)",
                        {});
                    gc_store.execute_sql(
                        "DELETE FROM _chronosfs_private_orphan_inodes "
                        "WHERE branch_id NOT IN ("
                        "SELECT branch_id "
                        "FROM _chronos_branch_interval_branches)",
                        {});
                } catch (...) {
                    std::lock_guard<std::mutex> guard(gc_mutex_);
                    gc_error_ = std::current_exception();
                }
                {
                    std::lock_guard<std::mutex> guard(gc_mutex_);
                    gc_running_ = false;
                }
                gc_condition_.notify_all();
            }
        } catch (...) {
            {
                std::lock_guard<std::mutex> guard(gc_mutex_);
                gc_error_ = std::current_exception();
                gc_pending_ = false;
                gc_running_ = false;
                gc_stopped_ = true;
            }
            gc_condition_.notify_all();
        }
    }

    NativeChronosFS &fs_for(const std::string &branch) {
        auto found = filesystems_.find(branch);
        if (found != filesystems_.end()) return *found->second;
        auto inserted = filesystems_.emplace(
            branch,
            std::make_unique<NativeChronosFS>(store_, branch, block_size_));
        return *inserted.first->second;
    }

    void clear_all_caches() {
        for (auto it = filesystems_.begin(); it != filesystems_.end();) {
            try {
                it->second->clear_cache_public();
                it->second->clear_private_inode_hints_public();
                ++it;
            } catch (const std::runtime_error &err) {
                if (std::string(err.what()).find("branch not found") != std::string::npos) {
                    it = filesystems_.erase(it);
                    continue;
                }
                throw;
            }
        }
    }

    std::string database_url_;
    std::shared_ptr<chronos::native::NativeBranchStore> store_;
    std::int64_t block_size_;
    std::string object_dir_;
    std::recursive_mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<NativeChronosFS>> filesystems_;
    std::mutex gc_mutex_;
    std::condition_variable gc_condition_;
    bool gc_pending_ = false;
    bool gc_running_ = false;
    bool gc_shutdown_ = false;
    bool gc_stopped_ = false;
    std::exception_ptr gc_error_;
    std::thread gc_thread_;
};

struct LockedReadFS {
    std::shared_lock<std::shared_mutex> topology_lock;
    std::shared_lock<std::shared_mutex> branch_lock;
    std::unique_lock<std::shared_mutex> writer_branch_lock;
    std::unique_lock<std::mutex> slot_lock;
    NativeChronosFS &fs;
};

LockedReadFS locked_read_fs_for_slot(std::size_t slot_index) {
    auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
    std::shared_lock<std::shared_mutex> topology_lock(state->backend->topology_mutex);
    auto branch = state->branch;
    std::shared_lock<std::shared_mutex> branch_lock(branch->mutex);
    if (branch->has_pending_write_batch()) {
        branch_lock.unlock();
        std::unique_lock<std::shared_mutex> writer_branch_lock(branch->mutex);
        if (branch->has_pending_write_batch()) {
            return LockedReadFS{
                std::move(topology_lock),
                {},
                std::move(writer_branch_lock),
                {},
                *branch->filesystem};
        }
        writer_branch_lock.unlock();
        branch_lock.lock();
    }
    slot_index %= state->read_slots.size();
    FuseReadSlot &slot = *state->read_slots[slot_index];
    std::unique_lock<std::mutex> slot_lock(slot.mutex);
    const std::uint64_t generation =
        branch->cache_generation.load(std::memory_order_acquire);
    if (!slot.filesystem || slot.branch_id != state->branch_id) {
        slot.filesystem = std::make_unique<NativeChronosFS>(
            state->backend->database_url,
            state->branch_id,
            state->backend->block_size);
        slot.branch_id = state->branch_id;
        slot.cache_generation = generation;
    } else if (slot.cache_generation != generation) {
        branch->refresh_reader_cache(*slot.filesystem, slot.cache_generation);
    }
    return LockedReadFS{
        std::move(topology_lock),
        std::move(branch_lock),
        {},
        std::move(slot_lock),
        *slot.filesystem};
}

LockedReadFS locked_read_fs() {
    return locked_read_fs_for_slot(
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
}

LockedReadFS locked_read_fs_for(const std::string &locality_key) {
    return locked_read_fs_for_slot(std::hash<std::string>{}(locality_key));
}

struct LockedWriteFS {
    std::shared_lock<std::shared_mutex> topology_lock;
    std::unique_lock<std::shared_mutex> branch_lock;
    NativeChronosFS &fs;
};

LockedWriteFS locked_write_fs(
    std::vector<std::string> invalidated_paths,
    bool begin_batch = true) {
    auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
    std::shared_lock<std::shared_mutex> topology_lock(state->backend->topology_mutex);
    std::unique_lock<std::shared_mutex> branch_lock(state->branch->mutex);
    if (begin_batch) state->branch->begin_write_batch_locked();
    state->branch->record_path_invalidation(std::move(invalidated_paths));
    return LockedWriteFS{
        std::move(topology_lock),
        std::move(branch_lock),
        *state->branch->filesystem};
}

LockedWriteFS locked_sync_fs() {
    auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
    std::shared_lock<std::shared_mutex> topology_lock(state->backend->topology_mutex);
    std::unique_lock<std::shared_mutex> branch_lock(state->branch->mutex);
    state->branch->commit_write_batch_locked();
    return LockedWriteFS{
        std::move(topology_lock),
        std::move(branch_lock),
        *state->branch->filesystem};
}

struct ExclusivelyLockedFS {
    std::unique_lock<std::shared_mutex> topology_lock;
    std::unique_lock<std::shared_mutex> branch_lock;
    SharedChronosFSBackend &backend;
    NativeChronosFS &fs;
};

ExclusivelyLockedFS exclusively_locked_fs() {
    auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
    std::unique_lock<std::shared_mutex> topology_lock(state->backend->topology_mutex);
    std::unique_lock<std::shared_mutex> branch_lock(state->branch->mutex);
    state->branch->commit_write_batch_locked();
    state->branch->record_full_invalidation();
    return ExclusivelyLockedFS{
        std::move(topology_lock),
        std::move(branch_lock),
        *state->backend,
        *state->branch->filesystem};
}
int error_code(const FsError &err) { return -err.code; }
std::string path_of(const char *path) { return path && *path ? std::string(path) : "/"; }
std::string parent_path_of(const std::string &path) {
    if (path.empty() || path == "/") return "/";
    const auto slash = path.find_last_of('/');
    if (slash == std::string::npos || slash == 0) return "/";
    return path.substr(0, slash);
}
void invalidate_paths(const std::vector<std::string> &paths) {
    auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
    state->backend->invalidate_paths(state->branch_id, paths);
}
bool is_current_branch_control(const std::string &path) {
    return path == "/.chronos/current";
}
bool is_merge_apply_control(const std::string &path) {
    return path.rfind("/.chronos/merge-apply/", 0) == 0;
}
bool is_branch_create_control(const std::string &path) {
    return path.rfind("/.chronos/branches/", 0) == 0;
}
std::string branch_name_from_control_write(const char *data, std::int64_t size) {
    std::string branch(data, data + size);
    while (!branch.empty() &&
           (branch.back() == '\n' || branch.back() == '\r' || branch.back() == '\0')) {
        branch.pop_back();
    }
    return branch;
}
OpenHandle *handle_of(struct fuse_file_info *fi) {
    if (!fi || fi->fh == 0) return nullptr;
    return reinterpret_cast<OpenHandle *>(static_cast<std::uintptr_t>(fi->fh));
}
std::string path_of_handle(const char *path, OpenHandle *handle) {
    if (path && *path) return std::string(path);
    return handle ? handle->path : std::string();
}
void set_handle(struct fuse_file_info *fi, OpenHandle *handle) {
    if (fi) fi->fh = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(handle));
}

int op_getattr(const char *path, struct stat *st, struct fuse_file_info *) {
    try {
        const std::string requested_path = path_of(path);
        auto locked = locked_read_fs_for(parent_path_of(requested_path));
        return locked.fs.getattr(requested_path, st) ? 0 : -ENOENT;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_open(const char *path, struct fuse_file_info *fi) {
    try {
        const int flags = fi ? fi->flags : 0;
        const std::string requested_path = path_of(path);
        auto *state = static_cast<FuseState *>(
            fuse_get_context()->private_data);
        const bool writable =
            (flags & O_ACCMODE) != O_RDONLY || (flags & O_TRUNC);
        {
            std::lock_guard<std::mutex> guard(state->mutable_paths_mutex);
            if (writable) state->mutable_paths.insert(requested_path);
            if (fi &&
                state->mutable_paths.find(requested_path) !=
                    state->mutable_paths.end()) {
                // A changed file may still have pre-write pages or an old EOF
                // in the kernel cache.  Bypass those pages for this path after
                // its first mutation; unchanged inherited files retain normal
                // page-cache behavior.
                fi->direct_io = 1;
            }
        }
        if (writable) {
            auto locked = locked_write_fs(
                {requested_path},
                (flags & O_TRUNC) != 0);
            set_handle(fi, locked.fs.open_handle(requested_path, flags));
        } else {
            auto locked = locked_read_fs_for(parent_path_of(requested_path));
            set_handle(fi, locked.fs.open_handle(requested_path, flags));
        }
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_readdir(
    const char *path,
    void *buf,
    fuse_fill_dir_t filler,
    off_t,
    struct fuse_file_info *,
    enum fuse_readdir_flags flags) {
    try {
        const std::string requested_path = path_of(path);
        auto locked = locked_read_fs_for(requested_path);
        const auto fill_flags = static_cast<fuse_fill_dir_flags>(
            (flags & FUSE_READDIR_PLUS) ? FUSE_FILL_DIR_PLUS : 0);
        if (filler(buf, ".", nullptr, 0, static_cast<fuse_fill_dir_flags>(0)) != 0) return 0;
        if (filler(buf, "..", nullptr, 0, static_cast<fuse_fill_dir_flags>(0)) != 0) return 0;
        if (requested_path == "/.chronos" ||
            requested_path == "/.chronos/branches" ||
            requested_path == "/.chronos/merge-preview" ||
            requested_path == "/.chronos/merge-apply") {
            for (const auto &name : locked.fs.listdir(requested_path)) {
                if (filler(
                        buf,
                        name.c_str(),
                        nullptr,
                        0,
                        static_cast<fuse_fill_dir_flags>(0)) != 0) {
                    break;
                }
            }
            return 0;
        }
        for (const auto &entry : locked.fs.listdir_entries(requested_path)) {
            struct stat st {};
            locked.fs.fill_stat_public(entry.inode, &st);
            if (filler(buf, entry.name.c_str(), &st, 0, fill_flags) != 0) break;
        }
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_read(const char *path, char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
    try {
        auto locked = locked_read_fs();
        auto data = locked.fs.read_handle(handle_of(fi), path_of(path), off, size);
        std::memcpy(buf, data.data(), data.size());
        return static_cast<int>(data.size());
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
    try {
        const std::string requested_path = path_of(path);
        if (is_current_branch_control(requested_path)) {
            if (off != 0) throw FsError(EINVAL, "current branch writes must start at zero");
            auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
            auto current_branch = state->branch;
            std::unique_lock<std::shared_mutex> topology_lock(state->backend->topology_mutex);
            std::unique_lock<std::shared_mutex> branch_lock(current_branch->mutex);
            current_branch->commit_write_batch_locked();
            const std::string next = branch_name_from_control_write(buf, size);
            const auto names = current_branch->filesystem->branch_names_public();
            if (std::find(names.begin(), names.end(), next) == names.end()) {
                throw FsError(ENOENT, "branch not found");
            }
            auto next_branch = state->backend->branch_for(next);
            state->branch_id = next;
            state->branch = std::move(next_branch);
            return static_cast<int>(size);
        }
        if (is_merge_apply_control(requested_path)) {
            auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
            {
                auto locked = exclusively_locked_fs();
                locked.fs.write_handle(handle_of(fi), requested_path, buf, size, off);
            }
            state->backend->clear_all_caches();
            return static_cast<int>(size);
        }
        const bool immediate = handle_of(fi) == nullptr;
        {
            auto locked = locked_write_fs({requested_path}, immediate);
            locked.fs.write_handle(handle_of(fi), requested_path, buf, size, off);
        }
        if (immediate) invalidate_paths({requested_path});
        return static_cast<int>(size);
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    try {
        const std::string requested_path = path_of(path);
        {
            auto locked = locked_write_fs({requested_path});
            set_handle(fi, locked.fs.create_handle(requested_path, mode));
        }
        invalidate_paths({requested_path, parent_path_of(requested_path)});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_flush(const char *path, struct fuse_file_info *fi) {
    try {
        OpenHandle *handle = handle_of(fi);
        if (handle == nullptr || handle->dirty_blocks.empty()) return 0;
        const std::string requested_path = path_of_handle(path, handle);
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.flush_handle(handle);
        }
        invalidate_paths({requested_path});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_fsync(const char *path, int, struct fuse_file_info *fi) {
    const int flush_result = op_flush(path, fi);
    if (flush_result != 0) return flush_result;
    try {
        auto locked = locked_sync_fs();
        locked.fs.sync_storage();
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_release(const char *path, struct fuse_file_info *fi) {
    try {
        std::unique_ptr<OpenHandle> handle(handle_of(fi));
        if (fi) fi->fh = 0;
        const bool writable =
            fi != nullptr &&
            ((fi->flags & O_ACCMODE) != O_RDONLY || (fi->flags & O_TRUNC));
        const std::string requested_path =
            path_of_handle(path, handle.get());
        if (handle && !handle->dirty_blocks.empty()) {
            {
                auto locked = locked_write_fs({requested_path});
                locked.fs.flush_handle(handle.get());
            }
        }
        // Invalidation from FLUSH can race with the kernel's final close of
        // the writable handle.  Invalidate again from RELEASE, after all
        // buffered blocks are committed, so the next opener cannot reuse
        // page-cache contents from the previous file version.
        if (handle && writable) invalidate_paths({requested_path});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    try {
        const std::string requested_path = path_of(path);
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.truncate_handle(handle_of(fi), requested_path, size);
        }
        invalidate_paths({requested_path});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_mkdir(const char *path, mode_t mode) {
    try {
        const std::string requested_path = path_of(path);
        if (is_branch_create_control(requested_path)) {
            auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
            {
                auto locked = exclusively_locked_fs();
                locked.fs.mkdir(requested_path, mode);
            }
            state->backend->clear_all_caches();
            return 0;
        }
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.mkdir(requested_path, mode);
        }
        invalidate_paths({requested_path, parent_path_of(requested_path)});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_unlink(const char *path) {
    try {
        const std::string requested_path = path_of(path);
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.unlink_path(requested_path, false);
        }
        invalidate_paths({requested_path, parent_path_of(requested_path)});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_rmdir(const char *path) {
    try {
        const std::string requested_path = path_of(path);
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.unlink_path(requested_path, true);
        }
        invalidate_paths({requested_path, parent_path_of(requested_path)});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_rename(const char *from, const char *to, unsigned int flags) {
    if (flags) return -EINVAL;
    try {
        const std::string source_path = path_of(from);
        const std::string target_path = path_of(to);
        {
            auto locked = locked_write_fs({source_path, target_path});
            locked.fs.rename_path(source_path, target_path);
        }
        invalidate_paths(
            {source_path,
             target_path,
             parent_path_of(source_path),
             parent_path_of(target_path)});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_chmod(const char *path, mode_t mode, struct fuse_file_info *) {
    try {
        const std::string requested_path = path_of(path);
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.chmod_path(requested_path, mode);
        }
        invalidate_paths({requested_path});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *) {
    try {
        const std::string requested_path = path_of(path);
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.utimens_path(requested_path, tv);
        }
        invalidate_paths({requested_path});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_symlink(const char *target, const char *linkpath) {
    try {
        const std::string requested_path = path_of(linkpath);
        {
            auto locked = locked_write_fs({requested_path});
            locked.fs.symlink_path(target, requested_path);
        }
        invalidate_paths({requested_path, parent_path_of(requested_path)});
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_readlink(const char *path, char *buf, size_t size) {
    try {
        const std::string requested_path = path_of(path);
        auto locked = locked_read_fs_for(parent_path_of(requested_path));
        auto target = locked.fs.readlink_path(requested_path);
        if (size) {
            auto take = std::min(size - 1, target.size());
            std::memcpy(buf, target.data(), take);
            buf[take] = '\0';
        }
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

void *op_init(struct fuse_conn_info *, struct fuse_config *) {
    auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
    state->fuse = fuse_get_context()->fuse;
    state->backend->register_mount(state);
    return state;
}

void op_destroy(void *private_data) {
    auto *state = static_cast<FuseState *>(private_data);
    state->backend->unregister_mount(state);
    state->fuse = nullptr;
}

int mount_chronosfs_native(
    const std::string &database_url,
    const std::string &mountpoint,
    const std::string &branch_id,
    std::int64_t block_size,
    const std::vector<std::string> &options) {
    static fuse_operations ops = [] {
        fuse_operations op{};
        op.getattr = op_getattr;
        op.readdir = op_readdir;
        op.open = op_open;
        op.flush = op_flush;
        op.fsync = op_fsync;
        op.release = op_release;
        op.read = op_read;
        op.write = op_write;
        op.create = op_create;
        op.truncate = op_truncate;
        op.mkdir = op_mkdir;
        op.unlink = op_unlink;
        op.rmdir = op_rmdir;
        op.rename = op_rename;
        op.chmod = op_chmod;
        op.utimens = op_utimens;
        op.symlink = op_symlink;
        op.readlink = op_readlink;
        op.init = op_init;
        op.destroy = op_destroy;
        return op;
    }();
    auto backend = shared_backend_for(database_url, block_size);
    auto branch = backend->branch_for(branch_id);
    {
        // A branch name may have been deleted and recreated while another
        // process kept the shared mount daemon alive. Refresh the captured
        // branch session before attaching a new mount so it cannot continue
        // writing through the retired branch interval.
        std::unique_lock<std::shared_mutex> guard(branch->mutex);
        branch->commit_write_batch_locked();
        branch->filesystem->clear_cache_public();
        branch->record_full_invalidation();
    }
    FuseState state{
        backend,
        branch_id,
        std::move(branch),
        {},
        {},
        {},
        nullptr};
    const std::size_t read_slots = std::clamp<std::size_t>(
        std::thread::hardware_concurrency(),
        2,
        8);
    state.read_slots.reserve(read_slots);
    for (std::size_t index = 0; index < read_slots; ++index) {
        state.read_slots.push_back(std::make_unique<FuseReadSlot>());
    }
    std::vector<std::string> args{"chronosfs", "-f", "-o"};
    std::string opts = "fsname=chronosfs";
    for (const auto &option : options) opts += "," + option;
    args.push_back(opts);
    args.push_back(mountpoint);
    std::vector<char *> argv;
    for (auto &arg : args) argv.push_back(arg.data());
    py::gil_scoped_release release;
    return fuse_main(static_cast<int>(argv.size()), argv.data(), &ops, &state);
}

} // namespace

namespace chronos::native {

void bind_chronosfs_fuse(py::module_ &m) {
    py::class_<NativeChronosFSStoreApi>(m, "NativeChronosFSStore")
        .def(py::init<std::string, std::int64_t>(), py::arg("database_url"), py::arg("block_size"))
        .def("ensure", &NativeChronosFSStoreApi::ensure)
        .def("branches", &NativeChronosFSStoreApi::branches)
        .def("create_branch", &NativeChronosFSStoreApi::create_branch, py::arg("branch_id"), py::arg("from_branch"))
        .def("delete_branch", &NativeChronosFSStoreApi::delete_branch, py::arg("branch_id"))
        .def("wait_for_gc", &NativeChronosFSStoreApi::wait_for_gc, py::call_guard<py::gil_scoped_release>())
        .def("merge_apply", &NativeChronosFSStoreApi::merge_apply, py::arg("source"), py::arg("target"))
        .def("stat", &NativeChronosFSStoreApi::stat, py::arg("branch"), py::arg("path"))
        .def("stat_inode", &NativeChronosFSStoreApi::stat_inode, py::arg("branch"), py::arg("inode_id"))
        .def("lookup_child", &NativeChronosFSStoreApi::lookup_child, py::arg("branch"), py::arg("parent_inode_id"), py::arg("name"))
        .def("exists", &NativeChronosFSStoreApi::exists, py::arg("branch"), py::arg("path"))
        .def("listdir", &NativeChronosFSStoreApi::listdir, py::arg("branch"), py::arg("path"))
        .def("listdir_inode", &NativeChronosFSStoreApi::listdir_inode, py::arg("branch"), py::arg("inode_id"))
        .def("read_file", &NativeChronosFSStoreApi::read_file, py::arg("branch"), py::arg("path"))
        .def("read_inode_range", &NativeChronosFSStoreApi::read_inode_range, py::arg("branch"), py::arg("inode_id"), py::arg("offset"), py::arg("size"))
        .def("write_file", &NativeChronosFSStoreApi::write_file, py::arg("branch"), py::arg("path"), py::arg("data"), py::arg("mode") = 0644, py::arg("parents") = false)
        .def("write_files", &NativeChronosFSStoreApi::write_files, py::arg("branch"), py::arg("paths"), py::arg("data"), py::arg("mode") = 0644, py::arg("parents") = false)
        .def("import_tree", &NativeChronosFSStoreApi::import_tree, py::arg("branch"), py::arg("source_path"))
        .def("write_at", &NativeChronosFSStoreApi::write_at, py::arg("branch"), py::arg("path"), py::arg("offset"), py::arg("data"))
        .def("write_inode_at", &NativeChronosFSStoreApi::write_inode_at, py::arg("branch"), py::arg("inode_id"), py::arg("offset"), py::arg("data"))
        .def("truncate", &NativeChronosFSStoreApi::truncate, py::arg("branch"), py::arg("path"), py::arg("size"))
        .def("truncate_inode", &NativeChronosFSStoreApi::truncate_inode, py::arg("branch"), py::arg("inode_id"), py::arg("size"))
        .def("chmod", &NativeChronosFSStoreApi::chmod, py::arg("branch"), py::arg("path"), py::arg("mode"))
        .def("chmod_inode", &NativeChronosFSStoreApi::chmod_inode, py::arg("branch"), py::arg("inode_id"), py::arg("mode"))
        .def("mkdir", &NativeChronosFSStoreApi::mkdir, py::arg("branch"), py::arg("path"), py::arg("mode") = 0755, py::arg("parents") = false)
        .def("create_file_at", &NativeChronosFSStoreApi::create_file_at, py::arg("branch"), py::arg("parent_inode_id"), py::arg("name"), py::arg("mode") = 0644)
        .def("mkdir_at", &NativeChronosFSStoreApi::mkdir_at, py::arg("branch"), py::arg("parent_inode_id"), py::arg("name"), py::arg("mode") = 0755)
        .def("symlink", &NativeChronosFSStoreApi::symlink, py::arg("branch"), py::arg("target"), py::arg("link_path"), py::arg("parents") = false)
        .def("symlink_at", &NativeChronosFSStoreApi::symlink_at, py::arg("branch"), py::arg("parent_inode_id"), py::arg("name"), py::arg("target"))
        .def("readlink", &NativeChronosFSStoreApi::readlink, py::arg("branch"), py::arg("path"))
        .def("unlink", &NativeChronosFSStoreApi::unlink, py::arg("branch"), py::arg("path"))
        .def("unlink_at", &NativeChronosFSStoreApi::unlink_at, py::arg("branch"), py::arg("parent_inode_id"), py::arg("name"))
        .def("rmdir", &NativeChronosFSStoreApi::rmdir, py::arg("branch"), py::arg("path"))
        .def("rmdir_at", &NativeChronosFSStoreApi::rmdir_at, py::arg("branch"), py::arg("parent_inode_id"), py::arg("name"))
        .def("rename", &NativeChronosFSStoreApi::rename, py::arg("branch"), py::arg("old_path"), py::arg("new_path"))
        .def("rename_at", &NativeChronosFSStoreApi::rename_at, py::arg("branch"), py::arg("old_parent_inode_id"), py::arg("old_name"), py::arg("new_parent_inode_id"), py::arg("new_name"))
        .def("clear_cache", &NativeChronosFSStoreApi::clear_cache, py::arg("branch"));

    m.def(
        "flush_chronosfs_native",
        &flush_chronosfs_native,
        py::arg("database_url"),
        py::arg("block_size"));
    m.def(
        "mount_chronosfs_native",
        &mount_chronosfs_native,
        py::arg("database_url"),
        py::arg("mountpoint"),
        py::arg("branch_id"),
        py::arg("block_size"),
        py::arg("options") = std::vector<std::string>{});
}

} // namespace chronos::native
