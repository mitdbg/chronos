#define FUSE_USE_VERSION 31

#include "chronosfs_fuse.hpp"

#include "interval_data_plane.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fuse3/fuse.h>
#include <pybind11/stl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace py = pybind11;

namespace {

using chronos::native::IntervalBlob;
using chronos::native::IntervalRows;
using chronos::native::IntervalValue;

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
//       Fixed-size file-content records keyed by (inode_id, block_index).  This
//       is the record-level COW layer: small overwrites splice only touched
//       blocks instead of copying whole files.
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
};

struct OpenHandle {
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

class NativeChronosFS {
  public:
    NativeChronosFS(std::string database_url, std::string branch_id, std::int64_t block_size)
        : store_(std::make_shared<chronos::native::NativeBranchStore>(database_url)),
          branch_id_(std::move(branch_id)),
          session_(store_->checkout(branch_id_)),
          block_size_(block_size) {
        if (block_size_ <= 0) throw FsError(EINVAL, "block size must be positive");
    }

    NativeChronosFS(std::shared_ptr<chronos::native::NativeBranchStore> store, std::string branch_id, std::int64_t block_size)
        : store_(std::move(store)),
          branch_id_(std::move(branch_id)),
          session_(store_->checkout(branch_id_)),
          block_size_(block_size) {
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
                "block_index BIGINT NOT NULL",
                "data " + blob_sql_type(*store_) + " NOT NULL",
                "valid_length INTEGER NOT NULL",
            },
            {"inode_id", "block_index"});

        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS _chronosfs_inode_allocator "
            "(id INTEGER PRIMARY KEY, next_inode_id BIGINT NOT NULL)");
        store_->execute_sql(
            "CREATE TABLE IF NOT EXISTS _chronosfs_metadata "
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL)");
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
                }});
        }
    }

    void getattr(const std::string &path, struct stat *st) {
        if (is_control(path)) return control_stat(path, st);
        fill_stat(inode_for_path(path), st);
    }

    std::vector<std::string> listdir(const std::string &path) {
        if (path == "/.chronos") return {"branches", "current"};
        if (path == "/.chronos/branches") return branches();
        Inode dir = inode_for_path(path);
        if (dir.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        auto rows = visible("chronosfs_dirents", {"name", "inode_id"}, "parent_inode_id = ?", {dir.id}, "ORDER BY name");
        std::vector<std::string> names;
        std::vector<std::int64_t> inode_ids;
        inode_ids.reserve(rows.size());
        for (auto &row : rows) {
            std::string name = as_string(row[0]);
            std::int64_t inode_id = as_int(row[1]);
            names.push_back(name);
            inode_ids.push_back(inode_id);
            dirent_cache_[dirent_key(dir.id, name)] = {true, inode_id};
            if (path == "/") path_inode_cache_["/" + name] = inode_id;
            else path_inode_cache_[path + "/" + name] = inode_id;
        }
        cache_inodes_by_id(inode_ids);
        return names;
    }

    IntervalBlob read(const std::string &path, std::int64_t offset, std::int64_t size) {
        if (path == "/.chronos/current") {
            std::string current = branch_id_ + "\n";
            if (offset >= static_cast<std::int64_t>(current.size())) return {};
            auto take = std::min<std::int64_t>(size, current.size() - offset);
            return IntervalBlob(current.begin() + offset, current.begin() + offset + take);
        }
        Inode inode = inode_for_path(path);
        if (inode.kind == "symlink") return IntervalBlob(inode.symlink_target.begin(), inode.symlink_target.end());
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        return read_inode_range(inode.id, offset, size, inode.size);
    }

    IntervalBlob read_handle(const OpenHandle *handle, const std::string &path, std::int64_t offset, std::int64_t size) {
        if (!handle) return read(path, offset, size);
        const Inode &inode = handle->inode;
        if (inode.kind == "symlink") return IntervalBlob(inode.symlink_target.begin(), inode.symlink_target.end());
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        return read_inode_range(inode.id, offset, size, inode.size);
    }

    void write(const std::string &path, const char *data, std::int64_t size, std::int64_t offset) {
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
        IntervalBlob payload(
            reinterpret_cast<const unsigned char *>(data),
            reinterpret_cast<const unsigned char *>(data + size));
        write_inode_at(handle->inode, offset, payload);
        handle->inode.size = std::max<std::int64_t>(handle->inode.size, offset + size);
    }

    void truncate_handle(OpenHandle *handle, const std::string &path, std::int64_t size) {
        if (!handle) return truncate(path, size);
        if (handle->inode.kind != "file") throw FsError(EISDIR, "not a file");
        truncate_inode(handle->inode, size);
        handle->inode.size = size;
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
                 now},
                false);
            upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
            touch(parent.id);
            inode = Inode{id, "file", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now};
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
        return new OpenHandle{std::move(inode)};
    }

    OpenHandle *create_handle(const std::string &path, mode_t mode) {
        return new OpenHandle{create(path, mode)};
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
                 now},
                false);
            upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
            touch(parent.id);
            inode = Inode{id, "directory", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now};
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
        Inode parent = inode_for_path(parent_path);
        auto entry = dirent(parent.id, name);
        if (!entry.first) throw FsError(ENOENT, "path not found");
        Inode inode = inode_by_id(entry.second);
        if (inode.kind == "directory" && !allow_dir) throw FsError(EISDIR, "is directory");
        if (inode.kind == "directory" && !listdir(path).empty()) throw FsError(ENOTEMPTY, "not empty");
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, entry.second, now_text()}, true);
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), true);
        touch(parent.id);
        forget_path_tree(path);
        inode_cache_.erase(entry.second);
        dirent_cache_.erase(dirent_key(parent.id, name));
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
             now},
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
        return read_inode_range(inode.id, 0, inode.size, inode.size);
    }

    IntervalBlob read_inode_range_public(std::int64_t inode_id, std::int64_t offset, std::int64_t size) {
        if (offset < 0 || size < 0) throw FsError(EINVAL, "negative read range");
        Inode inode = inode_by_id(inode_id);
        if (inode.kind != "file") throw FsError(EISDIR, "not a file");
        return read_inode_range(inode.id, offset, size, inode.size);
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
        Inode parent = inode_by_id(parent_inode_id);
        auto entry = dirent(parent.id, name);
        if (!entry.first) throw FsError(ENOENT, "path not found");
        Inode inode = inode_by_id(entry.second);
        if (inode.kind == "directory" && !allow_dir) throw FsError(EISDIR, "is directory");
        if (inode.kind == "directory" && !listdir_inode_id(inode.id).empty()) throw FsError(ENOTEMPTY, "not empty");
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, entry.second, now_text()}, true);
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), true);
        touch(parent.id);
        inode_cache_.erase(entry.second);
        dirent_cache_[dirent_key(parent.id, name)] = {false, 0};
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

    void clear_cache_public() {
        session_ = store_->checkout(branch_id_);
        clear_metadata_cache();
    }

  private:
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

    // chronosfs_file_blocks is the record-level COW data table. Each block row
    // is independently interval-versioned, so small overwrites do not copy the
    // full file as file-level COW systems do.
    static std::vector<std::string> block_cols() {
        return {
            "inode_id",
            "block_index",
            "data",
            "valid_length",
        };
    }

    void ensure_schema() {
        // ChronosFS is stored as three ordinary logical tables and then
        // registered with the native interval store:
        //   chronosfs_inodes      POSIX inode metadata and file size
        //   chronosfs_dirents     directory edges from parent/name to child
        //   chronosfs_file_blocks fixed-size file-content records
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
            "ctime TEXT NOT NULL)");
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
            "block_index BIGINT NOT NULL, "
            "data " + blob_type + " NOT NULL, "
            "valid_length INTEGER NOT NULL, "
            "PRIMARY KEY (inode_id, block_index))");
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
                path_inode_cache_[current_path] = current;
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
        Inode inode{id, "file", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now};
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
        touch(parent.id);
        cache_inode(inode);
        dirent_cache_[dirent_key(parent.id, name)] = {true, inode.id};
        return inode;
    }

    Inode mkdir_child(const Inode &parent, const std::string &name, mode_t mode) {
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
        std::int64_t id = allocate_inode();
        std::string now = now_text();
        Inode inode{id, "directory", static_cast<mode_t>(mode & 07777), getuid(), getgid(), 0, 1, "", now, now, now};
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
        touch(parent.id);
        cache_inode(inode);
        dirent_cache_[dirent_key(parent.id, name)] = {true, inode.id};
        return inode;
    }

    Inode symlink_child(const Inode &parent, const std::string &name, const std::string &target) {
        if (parent.kind != "directory") throw FsError(ENOTDIR, "not a directory");
        if (dirent(parent.id, name).first) throw FsError(EEXIST, "path exists");
        std::int64_t id = allocate_inode();
        std::string now = now_text();
        Inode inode{id, "symlink", 0777, getuid(), getgid(), static_cast<std::int64_t>(target.size()), 1, target, now, now, now};
        upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
        upsert("chronosfs_dirents", dirent_cols(), {"parent_inode_id", "name"}, {parent.id, name, id, now}, false);
        touch(parent.id);
        cache_inode(inode);
        dirent_cache_[dirent_key(parent.id, name)] = {true, inode.id};
        return inode;
    }

    void truncate_inode(Inode inode, std::int64_t size) {
        if (size < 0) throw FsError(EINVAL, "negative truncate size");
        const std::int64_t old_size = inode.size;
        transaction([&] {
            std::string now = now_text();
            if (size < old_size) {
                const std::int64_t first_removed_block =
                    size == 0 ? 0 : ((size - 1) / block_size_) + 1;
                auto removed = visible(
                    "chronosfs_file_blocks",
                    block_cols(),
                    "inode_id = ? AND block_index >= ?",
                    {inode.id, first_removed_block});
                if (!removed.empty()) {
                    upsert_many(
                        "chronosfs_file_blocks",
                        block_cols(),
                        {"inode_id", "block_index"},
                        removed,
                        true);
                }
            }
            inode.size = size;
            inode.mtime = now;
            inode.ctime = now;
            upsert("chronosfs_inodes", inode_cols(), {"inode_id"}, inode_values(inode), false);
            if (size > 0 && size < old_size && (size % block_size_) != 0) {
                std::int64_t last = (size - 1) / block_size_;
                std::int64_t valid = size - last * block_size_;
                IntervalBlob data = read_inode_range(inode.id, last * block_size_, valid, old_size);
                data.resize(static_cast<std::size_t>(valid), 0);
                upsert("chronosfs_file_blocks", block_cols(), {"inode_id", "block_index"}, {inode.id, last, data, valid}, false);
            }
        });
        cache_inode(inode);
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
            fn();
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
        };
    }

    Inode inode_for_path(const std::string &path) {
        if (path == "/" || path.empty()) return inode_by_id(1);
        auto cached = path_inode_cache_.find(path);
        if (cached != path_inode_cache_.end()) return inode_by_id(cached->second);
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
                if (!entry.first) throw FsError(ENOENT, "path not found");
                current = entry.second;
                path_inode_cache_[current_path] = current;
            }
            if (slash == std::string::npos) break;
            start = slash + 1;
        }
        return inode_by_id(current);
    }

    std::pair<bool, std::int64_t> dirent(std::int64_t parent, const std::string &name) {
        std::string key = dirent_key(parent, name);
        auto cached = dirent_cache_.find(key);
        if (cached != dirent_cache_.end()) return cached->second;
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
        inode_cache_[inode.id] = inode;
    }

    void remember_created_path(const std::string &path, std::int64_t parent, const std::string &name, const Inode &inode) {
        cache_inode(inode);
        path_inode_cache_[path] = inode.id;
        dirent_cache_[dirent_key(parent, name)] = {true, inode.id};
    }

    void forget_path_tree(const std::string &path) {
        for (auto it = path_inode_cache_.begin(); it != path_inode_cache_.end();) {
            const std::string &cached = it->first;
            bool same = cached == path;
            bool child = path != "/" && cached.size() > path.size() &&
                         cached.compare(0, path.size(), path) == 0 && cached[path.size()] == '/';
            if (same || child) it = path_inode_cache_.erase(it);
            else ++it;
        }
    }

    void clear_metadata_cache() {
        inode_cache_.clear();
        path_inode_cache_.clear();
        dirent_cache_.clear();
    }

    std::int64_t allocate_inode() {
        if (next_reserved_inode_id_ < reserved_inode_id_end_) {
            return next_reserved_inode_id_++;
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
        return next_reserved_inode_id_++;
    }

    IntervalBlob read_inode_range(
        std::int64_t inode,
        std::int64_t offset,
        std::int64_t size,
        std::int64_t file_size) {
        if (size <= 0 || offset >= file_size) return {};
        std::int64_t end = std::min(file_size, offset + size);
        std::int64_t first = offset / block_size_;
        std::int64_t last = (end - 1) / block_size_;
        std::unordered_map<std::int64_t, std::pair<IntervalBlob, std::int64_t>> blocks;
        // Reads stay sparse.  We fetch only touched blocks and synthesize holes
        // as zero bytes; there is no full-file materialization or FUSE-layer
        // data cache.
        auto rows = visible(
            "chronosfs_file_blocks",
            {"block_index", "data", "valid_length"},
            "inode_id = ? AND block_index >= ? AND block_index <= ?",
            {inode, first, last});
        for (auto &row : rows) {
            std::int64_t block = as_int(row[0]);
            blocks[block] = std::make_pair(as_blob(row[1]), as_int(row[2]));
        }
        IntervalBlob out;
        for (std::int64_t cursor = offset; cursor < end;) {
            std::int64_t block = cursor / block_size_;
            std::int64_t block_offset = cursor % block_size_;
            std::int64_t want = std::min(end - cursor, block_size_ - block_offset);
            auto found = blocks.find(block);
            if (found == blocks.end() || block_offset >= found->second.second) {
                out.insert(out.end(), want, 0);
            } else {
                auto &data = found->second.first;
                std::int64_t available =
                    std::min<std::int64_t>(found->second.second, data.size()) - block_offset;
                std::int64_t bytes = std::min<std::int64_t>(
                    want,
                    std::max<std::int64_t>(0, available));
                if (bytes > 0) out.insert(out.end(), data.begin() + block_offset, data.begin() + block_offset + bytes);
                if (bytes < want) out.insert(out.end(), want - bytes, 0);
            }
            cursor += want;
        }
        return out;
    }

    void write_inode_at(
        Inode inode,
        std::int64_t offset,
        const IntervalBlob &payload) {
        if (payload.empty()) return;
        transaction([&] {
            IntervalRows replacement_blocks;
            std::vector<std::int64_t> partial_blocks;
            // Full-block overwrites are blind writes: the old block content is
            // irrelevant.  Partial overwrites must fetch only the touched
            // existing blocks so unchanged bytes in those blocks can be carried
            // forward into the replacement records.
            for (std::int64_t cursor = 0; cursor < static_cast<std::int64_t>(payload.size());) {
                std::int64_t absolute = offset + cursor;
                std::int64_t block = absolute / block_size_;
                std::int64_t block_offset = absolute % block_size_;
                std::int64_t take = std::min<std::int64_t>(payload.size() - cursor, block_size_ - block_offset);
                const bool full_block_write = block_offset == 0 && take == block_size_;
                const bool block_has_existing_bytes = block * block_size_ < inode.size;
                if (!full_block_write && block_has_existing_bytes) {
                    partial_blocks.push_back(block);
                }
                cursor += take;
            }

            std::unordered_map<std::int64_t, std::pair<IntervalBlob, std::int64_t>> blocks;
            if (!partial_blocks.empty()) {
                std::vector<IntervalValue> params{inode.id};
                for (std::int64_t block : partial_blocks) params.push_back(block);
                auto rows = visible(
                    "chronosfs_file_blocks",
                    {"block_index", "data", "valid_length"},
                    "inode_id = ? AND block_index IN (" + sql_placeholders(partial_blocks.size()) + ")",
                    params);
                blocks.reserve(rows.size());
                for (auto &row : rows) {
                    std::int64_t block = as_int(row[0]);
                    blocks[block] = std::make_pair(as_blob(row[1]), as_int(row[2]));
                }
            }

            for (std::int64_t cursor = 0; cursor < static_cast<std::int64_t>(payload.size());) {
                std::int64_t absolute = offset + cursor;
                std::int64_t block = absolute / block_size_;
                std::int64_t block_offset = absolute % block_size_;
                std::int64_t take = std::min<std::int64_t>(payload.size() - cursor, block_size_ - block_offset);
                IntervalBlob current;
                auto found = blocks.find(block);
                if (found != blocks.end()) current = found->second.first;
                current.resize(block_size_, 0);
                std::copy(payload.begin() + cursor, payload.begin() + cursor + take, current.begin() + block_offset);
                std::int64_t prior_valid = std::min<std::int64_t>(
                    block_size_,
                    std::max<std::int64_t>(0, inode.size - block * block_size_));
                std::int64_t valid = std::max<std::int64_t>(block_offset + take, prior_valid);
                current.resize(valid);
                replacement_blocks.push_back({inode.id, block, current, valid});
                cursor += take;
            }
            upsert_many("chronosfs_file_blocks", block_cols(), {"inode_id", "block_index"}, replacement_blocks, false);

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
               path.rfind("/.chronos/branches/", 0) == 0;
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
        std::memset(st, 0, sizeof(struct stat));
        st->st_uid = getuid();
        st->st_gid = getgid();
        st->st_blksize = 4096;
        if (path == "/.chronos/current") {
            st->st_mode = S_IFREG | 0644;
            st->st_size = branch_id_.size() + 1;
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
        st->st_atim = parse_time_text(inode.atime);
        st->st_mtim = parse_time_text(inode.mtime);
        st->st_ctim = parse_time_text(inode.ctime);
        if (inode.kind == "directory") st->st_mode = S_IFDIR | inode.mode;
        else if (inode.kind == "symlink") st->st_mode = S_IFLNK | inode.mode;
        else st->st_mode = S_IFREG | inode.mode;
    }

    std::shared_ptr<chronos::native::NativeBranchStore> store_;
    std::string branch_id_;
    chronos::native::NativeBranchSession session_;
    std::int64_t block_size_;
    std::int64_t next_reserved_inode_id_ = 0;
    std::int64_t reserved_inode_id_end_ = 0;
    std::unordered_map<std::int64_t, Inode> inode_cache_;
    std::unordered_map<std::string, std::int64_t> path_inode_cache_;
    std::unordered_map<std::string, std::pair<bool, std::int64_t>> dirent_cache_;
};

struct SharedChronosFSBackend {
    std::shared_ptr<NativeChronosFS> fs;
    std::shared_ptr<std::recursive_mutex> mutex;
};

struct FuseState {
    std::shared_ptr<SharedChronosFSBackend> backend;
};

std::unordered_map<std::string, std::weak_ptr<SharedChronosFSBackend>> &shared_backends() {
    static std::unordered_map<std::string, std::weak_ptr<SharedChronosFSBackend>> backends;
    return backends;
}

std::mutex &shared_backends_mutex() {
    static std::mutex mutex;
    return mutex;
}

std::string backend_key(const std::string &database_url, const std::string &branch_id, std::int64_t block_size) {
    return database_url + '\0' + branch_id + '\0' + std::to_string(block_size);
}

std::shared_ptr<SharedChronosFSBackend> shared_backend_for(
    const std::string &database_url,
    const std::string &branch_id,
    std::int64_t block_size) {
    std::lock_guard<std::mutex> guard(shared_backends_mutex());
    auto key = backend_key(database_url, branch_id, block_size);
    auto found = shared_backends().find(key);
    if (found != shared_backends().end()) {
        if (auto existing = found->second.lock()) return existing;
    }
    auto backend = std::make_shared<SharedChronosFSBackend>();
    backend->fs = std::make_shared<NativeChronosFS>(database_url, branch_id, block_size);
    backend->mutex = std::make_shared<std::recursive_mutex>();
    shared_backends()[std::move(key)] = backend;
    return backend;
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
        : store_(std::make_shared<chronos::native::NativeBranchStore>(std::move(database_url))),
          block_size_(block_size) {}

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
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        store_->delete_branch(branch_id);
        store_->collect_interval_garbage();
        filesystems_.erase(branch_id);
        clear_all_caches();
    }

    std::int64_t merge_apply(const std::string &source, const std::string &target) {
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        std::int64_t applied = store_->merge_apply(source, target);
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

    std::shared_ptr<chronos::native::NativeBranchStore> store_;
    std::int64_t block_size_;
    std::recursive_mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<NativeChronosFS>> filesystems_;
};

struct LockedFS {
    std::unique_lock<std::recursive_mutex> lock;
    NativeChronosFS &fs;
};

LockedFS locked_fs() {
    auto *state = static_cast<FuseState *>(fuse_get_context()->private_data);
    return LockedFS{std::unique_lock<std::recursive_mutex>(*state->backend->mutex), *state->backend->fs};
}
int error_code(const FsError &err) { return -err.code; }
std::string path_of(const char *path) { return path && *path ? std::string(path) : "/"; }
OpenHandle *handle_of(struct fuse_file_info *fi) {
    if (!fi || fi->fh == 0) return nullptr;
    return reinterpret_cast<OpenHandle *>(static_cast<std::uintptr_t>(fi->fh));
}
void set_handle(struct fuse_file_info *fi, OpenHandle *handle) {
    if (fi) fi->fh = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(handle));
}

int op_getattr(const char *path, struct stat *st, struct fuse_file_info *) {
    try {
        auto locked = locked_fs();
        locked.fs.getattr(path_of(path), st);
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_open(const char *path, struct fuse_file_info *fi) {
    try {
        auto locked = locked_fs();
        set_handle(fi, locked.fs.open_handle(path_of(path), fi ? fi->flags : 0));
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
    enum fuse_readdir_flags) {
    try {
        auto locked = locked_fs();
        filler(buf, ".", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
        filler(buf, "..", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
        for (const auto &name : locked.fs.listdir(path_of(path))) {
            filler(buf, name.c_str(), nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
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
        auto locked = locked_fs();
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
        auto locked = locked_fs();
        locked.fs.write_handle(handle_of(fi), path_of(path), buf, size, off);
        return static_cast<int>(size);
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    try {
        auto locked = locked_fs();
        set_handle(fi, locked.fs.create_handle(path_of(path), mode));
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_release(const char *, struct fuse_file_info *fi) {
    try {
        delete handle_of(fi);
        if (fi) fi->fh = 0;
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    try {
        auto locked = locked_fs();
        locked.fs.truncate_handle(handle_of(fi), path_of(path), size);
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_mkdir(const char *path, mode_t mode) {
    try {
        auto locked = locked_fs();
        locked.fs.mkdir(path_of(path), mode);
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_unlink(const char *path) {
    try {
        auto locked = locked_fs();
        locked.fs.unlink_path(path_of(path), false);
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_rmdir(const char *path) {
    try {
        auto locked = locked_fs();
        locked.fs.unlink_path(path_of(path), true);
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
        auto locked = locked_fs();
        locked.fs.rename_path(path_of(from), path_of(to));
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_chmod(const char *path, mode_t mode, struct fuse_file_info *) {
    try {
        auto locked = locked_fs();
        locked.fs.chmod_path(path_of(path), mode);
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *) {
    try {
        auto locked = locked_fs();
        locked.fs.utimens_path(path_of(path), tv);
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_symlink(const char *target, const char *linkpath) {
    try {
        auto locked = locked_fs();
        locked.fs.symlink_path(target, path_of(linkpath));
        return 0;
    } catch (const FsError &e) {
        return error_code(e);
    } catch (...) {
        return -EIO;
    }
}

int op_readlink(const char *path, char *buf, size_t size) {
    try {
        auto locked = locked_fs();
        auto target = locked.fs.readlink_path(path_of(path));
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
        return op;
    }();
    FuseState state{shared_backend_for(database_url, branch_id, block_size)};
    std::vector<std::string> args{"chronosfs", "-f", "-s", "-o"};
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
        "mount_chronosfs_native",
        &mount_chronosfs_native,
        py::arg("database_url"),
        py::arg("mountpoint"),
        py::arg("branch_id"),
        py::arg("block_size"),
        py::arg("options") = std::vector<std::string>{});
}

} // namespace chronos::native
