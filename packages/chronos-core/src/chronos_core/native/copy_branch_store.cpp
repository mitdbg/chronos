#include "copy_branch_store.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace chronos::native {

using namespace detail;

namespace {

struct CopyIndexMeta {
    std::string name;
    std::string table;
    std::vector<std::string> columns;
};

std::uint32_t copy_sha1_rotl(std::uint32_t value, int bits) {
    return (value << bits) | (value >> (32 - bits));
}

std::string copy_sha1_hexdigest(const std::string &value) {
    std::vector<unsigned char> message(value.begin(), value.end());
    const std::uint64_t bit_length = static_cast<std::uint64_t>(message.size()) * 8ULL;
    message.push_back(0x80);
    while ((message.size() % 64) != 56) {
        message.push_back(0);
    }
    for (int shift = 56; shift >= 0; shift -= 8) {
        message.push_back(static_cast<unsigned char>((bit_length >> shift) & 0xffU));
    }

    std::uint32_t h0 = 0x67452301U;
    std::uint32_t h1 = 0xefcdab89U;
    std::uint32_t h2 = 0x98badcfeU;
    std::uint32_t h3 = 0x10325476U;
    std::uint32_t h4 = 0xc3d2e1f0U;

    for (std::size_t offset = 0; offset < message.size(); offset += 64) {
        std::array<std::uint32_t, 80> words{};
        for (std::size_t i = 0; i < 16; ++i) {
            const std::size_t base = offset + i * 4;
            words[i] =
                (static_cast<std::uint32_t>(message[base]) << 24) |
                (static_cast<std::uint32_t>(message[base + 1]) << 16) |
                (static_cast<std::uint32_t>(message[base + 2]) << 8) |
                static_cast<std::uint32_t>(message[base + 3]);
        }
        for (std::size_t i = 16; i < 80; ++i) {
            words[i] = copy_sha1_rotl(words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16], 1);
        }

        std::uint32_t a = h0;
        std::uint32_t b = h1;
        std::uint32_t c = h2;
        std::uint32_t d = h3;
        std::uint32_t e = h4;

        for (std::size_t i = 0; i < 80; ++i) {
            std::uint32_t f = 0;
            std::uint32_t k = 0;
            if (i < 20) {
                f = (b & c) | ((~b) & d);
                k = 0x5a827999U;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ed9eba1U;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8f1bbcdcU;
            } else {
                f = b ^ c ^ d;
                k = 0xca62c1d6U;
            }
            const std::uint32_t temp = copy_sha1_rotl(a, 5) + f + e + k + words[i];
            e = d;
            d = c;
            c = copy_sha1_rotl(b, 30);
            b = a;
            a = temp;
        }

        h0 += a;
        h1 += b;
        h2 += c;
        h3 += d;
        h4 += e;
    }

    std::ostringstream out;
    out << std::hex << std::setfill('0')
        << std::setw(8) << h0
        << std::setw(8) << h1
        << std::setw(8) << h2
        << std::setw(8) << h3
        << std::setw(8) << h4;
    return out.str();
}

std::string copy_identifier_token(const std::string &value) {
    std::string token;
    token.reserve(value.size());
    for (char ch : value) {
        const bool keep =
            (ch >= 'A' && ch <= 'Z') ||
            (ch >= 'a' && ch <= 'z') ||
            (ch >= '0' && ch <= '9') ||
            ch == '_';
        if (keep) {
            token.push_back(ch);
        } else if (!token.empty() && token.back() != '_') {
            token.push_back('_');
        }
    }
    while (!token.empty() && token.front() == '_') token.erase(token.begin());
    while (!token.empty() && token.back() == '_') token.pop_back();
    if (token.empty()) token = "value";
    if (token.size() > 48) token.resize(48);
    return token + "_" + copy_sha1_hexdigest(value).substr(0, 10);
}

std::string copy_branch_table_name(const std::string &branch_id, const std::string &table) {
    return "_chronos_b_copy_" + copy_identifier_token(branch_id) + "_" + copy_identifier_token(table);
}

std::string copy_checkpoint_table_name(const std::string &checkpoint, const std::string &table) {
    return "_chronos_b_copy_cp_" + copy_identifier_token(checkpoint) + "_" + copy_identifier_token(table);
}

std::string copy_default_index_name(const std::string &table, const std::vector<std::string> &columns) {
    std::string out = "idx_" + table + "_";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i) out += "_";
        out += columns[i];
    }
    return out;
}

std::string copy_postgres_column_type(const std::vector<Value> &row) {
    const std::string data_type = native_as_string(row[1]);
    const std::string udt_name = native_as_string(row[2]);
    if (data_type == "USER-DEFINED" || data_type == "ARRAY") return udt_name;
    if (data_type == "character varying") {
        return std::holds_alternative<std::monostate>(row[3])
            ? "VARCHAR"
            : "VARCHAR(" + native_as_string(row[3]) + ")";
    }
    if (data_type == "character") {
        return std::holds_alternative<std::monostate>(row[3])
            ? "CHAR"
            : "CHAR(" + native_as_string(row[3]) + ")";
    }
    if (data_type == "numeric") {
        if (!std::holds_alternative<std::monostate>(row[4]) &&
            !std::holds_alternative<std::monostate>(row[5])) {
            return "NUMERIC(" + native_as_string(row[4]) + ", " + native_as_string(row[5]) + ")";
        }
        return "NUMERIC";
    }
    if (data_type == "timestamp without time zone") return "TIMESTAMP";
    if (data_type == "timestamp with time zone") return "TIMESTAMPTZ";
    std::string upper = data_type;
    std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return upper;
}

std::pair<std::vector<std::string>, std::vector<std::string>> copy_table_defs(
    NativeSqlDriver &driver,
    const std::string &table
) {
    std::vector<std::string> columns;
    std::vector<std::string> defs;
    if (driver.dialect() == "postgres") {
        auto [schema, table_name] = split_postgres_table_name(table);
        auto rows = driver.query(
            "SELECT column_name, data_type, udt_name, character_maximum_length, "
            "       numeric_precision, numeric_scale, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            {schema, table_name}
        );
        columns.reserve(rows.size());
        defs.reserve(rows.size());
        for (const auto &row : rows) {
            const std::string name = native_as_string(row[0]);
            columns.push_back(name);
            defs.push_back(quote_ident(name) + " " + copy_postgres_column_type(row));
        }
        return {columns, defs};
    }
    auto rows = driver.query("PRAGMA table_info(" + quote_table_name(table) + ")");
    columns.reserve(rows.size());
    defs.reserve(rows.size());
    for (const auto &row : rows) {
        const std::string name = native_as_string(row[1]);
        const std::string type = native_as_string(row[2]).empty() ? "TEXT" : native_as_string(row[2]);
        columns.push_back(name);
        defs.push_back(quote_ident(name) + " " + type);
    }
    return {columns, defs};
}

bool copy_starts_with_delete(const std::string &sql) {
    return starts_with_sql_keyword(sql, "DELETE");
}

std::string copy_rewrite_delete_target(
    const std::string &sql,
    const std::unordered_map<std::string, std::string> &replacements
) {
    if (!copy_starts_with_delete(sql)) return rewrite_visible_tables(sql, replacements);

    std::string out;
    out.reserve(sql.size() * 2);
    bool single_quote = false;
    bool double_quote = false;
    bool expect_delete_target = false;
    for (std::size_t i = 0; i < sql.size();) {
        const char ch = sql[i];
        if (ch == '\'' && !double_quote) {
            single_quote = !single_quote;
            out.push_back(ch);
            ++i;
            continue;
        }
        if (ch == '"' && !single_quote) {
            std::size_t j = i + 1;
            std::string ident;
            while (j < sql.size()) {
                if (sql[j] == '"') {
                    if (j + 1 < sql.size() && sql[j + 1] == '"') {
                        ident.push_back('"');
                        j += 2;
                        continue;
                    }
                    break;
                }
                ident.push_back(sql[j]);
                ++j;
            }
            if (expect_delete_target && j < sql.size()) {
                if (auto replacement = lookup_replacement(replacements, ident)) {
                    out += quote_ident(*replacement);
                } else {
                    out.append(sql, i, j - i + 1);
                }
                expect_delete_target = false;
                i = j + 1;
            } else {
                out.append(sql, i, j < sql.size() ? j - i + 1 : j - i);
                i = j < sql.size() ? j + 1 : j;
            }
            continue;
        }
        if (single_quote || double_quote || !is_identifier_start(ch)) {
            out.push_back(ch);
            ++i;
            continue;
        }

        std::size_t j = i + 1;
        while (j < sql.size() && is_identifier_part(sql[j])) ++j;
        std::string ident = sql.substr(i, j - i);
        std::string upper = ident;
        std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) {
            return static_cast<char>(std::toupper(c));
        });
        if (expect_delete_target) {
            std::string lookup_name = ident;
            std::size_t consumed = j;
            if (j < sql.size() && sql[j] == '.' && j + 1 < sql.size() && is_identifier_start(sql[j + 1])) {
                std::size_t k = j + 2;
                while (k < sql.size() && is_identifier_part(sql[k])) ++k;
                lookup_name = ident + "." + sql.substr(j + 1, k - j - 1);
                consumed = k;
            }
            if (auto replacement = lookup_replacement(replacements, lookup_name)) {
                out += quote_ident(*replacement);
            } else {
                out += lookup_name;
            }
            expect_delete_target = false;
            i = consumed;
            continue;
        }
        out += ident;
        if (upper == "FROM") {
            expect_delete_target = true;
        }
        i = j;
    }
    return out;
}

std::string copy_rewrite_sql(
    const std::string &sql,
    const std::unordered_map<std::string, std::string> &replacements
) {
    if (copy_starts_with_delete(sql)) {
        return copy_rewrite_delete_target(sql, replacements);
    }
    return rewrite_visible_tables(sql, replacements);
}

std::string copy_key_where_sql(const std::vector<std::string> &pk_columns) {
    std::string out;
    for (std::size_t i = 0; i < pk_columns.size(); ++i) {
        if (i) out += " AND ";
        out += quote_ident(pk_columns[i]) + " = ?";
    }
    return out;
}

std::vector<Value> copy_key_values(
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const std::vector<Value> &row
) {
    std::vector<Value> values;
    values.reserve(pk_columns.size());
    for (const auto &pk : pk_columns) {
        values.push_back(row[column_index(columns, pk)]);
    }
    return values;
}

void copy_raise_if_readonly(bool readonly) {
    if (readonly) {
        throw std::runtime_error("checkpoint sessions are read-only");
    }
}

} // namespace

class NativeCopyBranchStoreImpl {
  public:
    explicit NativeCopyBranchStoreImpl(const std::string &database_url)
        : driver_(open_native_sql_driver(database_url)) {}
    explicit NativeCopyBranchStoreImpl(sqlite3 *db)
        : driver_(std::make_unique<NativeSQLiteDriver>(db)) {}
    const std::string &dialect() const { return driver_->dialect(); }
    bool in_transaction() const { return driver_->in_transaction(); }
    void commit() {
        if (driver_->in_transaction()) driver_->execute("COMMIT");
    }
    void rollback() {
        if (!driver_->in_transaction()) return;
        try {
            driver_->execute("ROLLBACK");
        } catch (...) {
        }
    }

    void ensure_metadata() {
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_tables ("
            "table_name TEXT PRIMARY KEY, "
            "physical_table TEXT NOT NULL, "
            "pk_columns TEXT NOT NULL, "
            "columns TEXT NOT NULL, "
            "column_defs TEXT NOT NULL, "
            "backend TEXT NOT NULL)"
        );
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_indexes ("
            "backend TEXT NOT NULL, "
            "index_name TEXT NOT NULL, "
            "table_name TEXT NOT NULL, "
            "columns TEXT NOT NULL, "
            "PRIMARY KEY (backend, index_name))"
        );
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_copy_branches ("
            "branch_id TEXT PRIMARY KEY, "
            "created_at TEXT NOT NULL, "
            "metadata TEXT NOT NULL)"
        );
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_copy_checkpoints ("
            "checkpoint_id TEXT PRIMARY KEY, "
            "branch_id TEXT NOT NULL, "
            "created_at TEXT NOT NULL, "
            "metadata TEXT NOT NULL)"
        );
        auto main = driver_->query(
            "SELECT 1 FROM _chronos_branch_copy_branches WHERE branch_id = 'main' LIMIT 1"
        );
        if (main.empty()) {
            driver_->execute(
                "INSERT INTO _chronos_branch_copy_branches "
                "(branch_id, created_at, metadata) VALUES ('main', ?, '{}')",
                {current_timestamp_string()}
            );
        }
    }

    void register_table(const std::string &table, const std::vector<std::string> &primary_key) {
        driver_->refresh_catalog();
        if (table_registered(table)) return;
        auto [columns, defs] = copy_table_defs(*driver_, table);
        if (columns.empty()) {
            throw std::runtime_error("chronos_copy_table_not_registered:" + table);
        }
        std::unordered_set<std::string> column_set(columns.begin(), columns.end());
        for (const auto &pk : primary_key) {
            if (column_set.find(pk) == column_set.end()) {
                throw std::runtime_error("primary key columns missing from " + table + ": " + pk);
            }
        }
        const std::string physical = copy_branch_table_name("main", table);
        create_physical_table(physical, defs, primary_key);
        const std::string cols = comma_join_quoted(columns);
        driver_->execute(
            "INSERT INTO " + quote_ident(physical) + " (" + cols + ") "
            "SELECT " + cols + " FROM " + quote_table_name(table)
        );
        driver_->execute(
            "INSERT INTO _chronos_branch_tables "
            "(table_name, physical_table, pk_columns, columns, column_defs, backend) "
            "VALUES (?, ?, ?, ?, ?, 'copy')",
            {
                table,
                physical,
                json_string_array(primary_key),
                json_string_array(columns),
                json_string_array(defs),
            }
        );
        NativeTableMeta meta{table, physical, columns, primary_key, defs, "", "", false};
        create_existing_indexes_for_table(meta, "main", false);
    }

    std::string create_index(
        const std::string &table,
        const std::vector<std::string> &columns,
        const std::string &requested_name
    ) {
        if (columns.empty()) {
            throw std::invalid_argument("index columns cannot be empty");
        }
        NativeTableMeta meta = load_table_meta(table);
        std::unordered_set<std::string> visible_columns(meta.columns.begin(), meta.columns.end());
        for (const auto &column : columns) {
            if (visible_columns.find(column) == visible_columns.end()) {
                throw std::runtime_error("index columns missing from " + table + ": " + column);
            }
        }
        const std::string index_name = requested_name.empty()
            ? copy_default_index_name(table, columns)
            : requested_name;
        const std::string columns_json = json_string_array(columns);
        auto existing = driver_->query(
            "SELECT table_name, columns FROM _chronos_branch_indexes "
            "WHERE backend = 'copy' AND index_name = ?",
            {index_name}
        );
        if (existing.empty()) {
            driver_->execute(
                "INSERT INTO _chronos_branch_indexes "
                "(backend, index_name, table_name, columns) "
                "VALUES ('copy', ?, ?, ?)",
                {index_name, table, columns_json}
            );
        } else if (
            native_as_string(existing[0][0]) != table ||
            native_as_string(existing[0][1]) != columns_json
        ) {
            throw std::runtime_error("index already exists with different definition: " + index_name);
        }
        CopyIndexMeta index{index_name, table, columns};
        for (const auto &branch : branch_ids()) {
            create_physical_index(meta, index, branch, false);
        }
        for (const auto &checkpoint : checkpoint_ids()) {
            create_physical_index(meta, index, checkpoint, true);
        }
        return index_name;
    }

    void create_branch(
        const std::string &branch_id,
        const std::string &from_branch,
        const std::string &metadata_json
    ) {
        if (branch_exists(branch_id)) {
            throw std::runtime_error("chronos_copy_branch_exists:" + branch_id);
        }
        if (!branch_exists(from_branch)) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + from_branch);
        }
        for (const auto &meta : load_table_metas()) {
            copy_table(
                copy_branch_table_name(from_branch, meta.logical_name),
                copy_branch_table_name(branch_id, meta.logical_name),
                meta
            );
            create_existing_indexes_for_table(meta, branch_id, false);
        }
        driver_->execute(
            "INSERT INTO _chronos_branch_copy_branches "
            "(branch_id, created_at, metadata) VALUES (?, ?, ?)",
            {branch_id, current_timestamp_string(), metadata_json.empty() ? "{}" : metadata_json}
        );
    }

    void create_branch_from_checkpoint(
        const std::string &branch_id,
        const std::string &checkpoint
    ) {
        if (branch_exists(branch_id)) {
            throw std::runtime_error("chronos_copy_branch_exists:" + branch_id);
        }
        if (!checkpoint_exists(checkpoint)) {
            throw std::runtime_error("chronos_copy_checkpoint_not_found:" + checkpoint);
        }
        for (const auto &meta : load_table_metas()) {
            copy_table(
                copy_checkpoint_table_name(checkpoint, meta.logical_name),
                copy_branch_table_name(branch_id, meta.logical_name),
                meta
            );
            create_existing_indexes_for_table(meta, branch_id, false);
        }
        driver_->execute(
            "INSERT INTO _chronos_branch_copy_branches "
            "(branch_id, created_at, metadata) VALUES (?, ?, '{}')",
            {branch_id, current_timestamp_string()}
        );
    }

    void delete_branch(const std::string &branch_id) {
        if (branch_id == "main") {
            throw std::runtime_error("main cannot be deleted");
        }
        if (!branch_exists(branch_id)) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + branch_id);
        }
        for (const auto &meta : load_table_metas()) {
            driver_->execute("DROP TABLE IF EXISTS " + quote_ident(copy_branch_table_name(branch_id, meta.logical_name)));
        }
        driver_->execute(
            "DELETE FROM _chronos_branch_copy_branches WHERE branch_id = ?",
            {branch_id}
        );
    }

    NativeBranchInfo update_branch_metadata(
        const std::string &branch_id,
        const std::string &metadata_json
    ) {
        if (!branch_exists(branch_id)) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + branch_id);
        }
        driver_->execute(
            "UPDATE _chronos_branch_copy_branches SET metadata = ? WHERE branch_id = ?",
            {metadata_json.empty() ? "{}" : metadata_json, branch_id}
        );
        return get_branch_info(branch_id);
    }

    NativeBranchInfo get_branch_info(const std::string &branch_id) {
        auto rows = driver_->query(
            "SELECT branch_id, created_at, metadata "
            "FROM _chronos_branch_copy_branches WHERE branch_id = ?",
            {branch_id}
        );
        if (rows.empty()) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + branch_id);
        }
        return {
            native_as_string(rows[0][0]),
            native_as_string(rows[0][0]),
            native_as_string(rows[0][1]),
            native_as_string(rows[0][2]),
        };
    }

    std::vector<NativeBranchInfo> list_branch_infos() {
        auto rows = driver_->query(
            "SELECT branch_id, created_at, metadata "
            "FROM _chronos_branch_copy_branches ORDER BY branch_id"
        );
        std::vector<NativeBranchInfo> out;
        out.reserve(rows.size());
        for (const auto &row : rows) {
            const std::string branch_id = native_as_string(row[0]);
            out.push_back({branch_id, branch_id, native_as_string(row[1]), native_as_string(row[2])});
        }
        return out;
    }

    NativeCheckpointInfo create_checkpoint(
        const std::string &checkpoint,
        const std::string &branch,
        const std::string &metadata_json
    ) {
        if (checkpoint_exists(checkpoint)) {
            throw std::runtime_error("chronos_copy_branch_exists:" + checkpoint);
        }
        if (!branch_exists(branch)) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + branch);
        }
        for (const auto &meta : load_table_metas()) {
            copy_table(
                copy_branch_table_name(branch, meta.logical_name),
                copy_checkpoint_table_name(checkpoint, meta.logical_name),
                meta
            );
            create_existing_indexes_for_table(meta, checkpoint, true);
        }
        const std::string now = current_timestamp_string();
        driver_->execute(
            "INSERT INTO _chronos_branch_copy_checkpoints "
            "(checkpoint_id, branch_id, created_at, metadata) VALUES (?, ?, ?, ?)",
            {checkpoint, branch, now, metadata_json.empty() ? "{}" : metadata_json}
        );
        return {checkpoint, branch, checkpoint, now, metadata_json.empty() ? "{}" : metadata_json};
    }

    NativeCheckpointInfo get_checkpoint_info(const std::string &checkpoint) {
        auto rows = driver_->query(
            "SELECT checkpoint_id, branch_id, created_at, metadata "
            "FROM _chronos_branch_copy_checkpoints WHERE checkpoint_id = ?",
            {checkpoint}
        );
        if (rows.empty()) {
            throw std::runtime_error("chronos_copy_checkpoint_not_found:" + checkpoint);
        }
        return {
            native_as_string(rows[0][0]),
            native_as_string(rows[0][1]),
            native_as_string(rows[0][0]),
            native_as_string(rows[0][2]),
            native_as_string(rows[0][3]),
        };
    }

    std::vector<NativeCheckpointInfo> list_checkpoint_infos(const std::string &branch) {
        std::vector<std::vector<Value>> rows;
        if (branch.empty()) {
            rows = driver_->query(
                "SELECT checkpoint_id, branch_id, created_at, metadata "
                "FROM _chronos_branch_copy_checkpoints ORDER BY created_at, checkpoint_id"
            );
        } else {
            rows = driver_->query(
                "SELECT checkpoint_id, branch_id, created_at, metadata "
                "FROM _chronos_branch_copy_checkpoints WHERE branch_id = ? "
                "ORDER BY created_at, checkpoint_id",
                {branch}
            );
        }
        std::vector<NativeCheckpointInfo> out;
        out.reserve(rows.size());
        for (const auto &row : rows) {
            out.push_back({
                native_as_string(row[0]),
                native_as_string(row[1]),
                native_as_string(row[0]),
                native_as_string(row[2]),
                native_as_string(row[3]),
            });
        }
        return out;
    }

    NativeCopyBranchSession checkout(const std::string &branch_id);
    NativeCopyBranchSession checkout_checkpoint(const std::string &checkpoint);

    QueryResult visible_rows(const std::string &branch_id, const std::string &table) {
        if (!branch_exists(branch_id)) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + branch_id);
        }
        NativeTableMeta meta = load_table_meta(table);
        return driver_->query_result(
            "SELECT * FROM " + quote_ident(copy_branch_table_name(branch_id, table))
        );
    }

    NativeTableInfo table_info(const std::string &branch_id, const std::string &table) {
        if (!branch_exists(branch_id)) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + branch_id);
        }
        NativeTableMeta meta = load_table_meta(table);
        return {
            meta.logical_name,
            copy_branch_table_name(branch_id, table),
            meta.pk_columns,
            meta.columns,
            meta.column_defs,
        };
    }

    std::vector<std::string> table_names() {
        std::vector<std::string> out;
        for (const auto &meta : load_table_metas()) {
            out.push_back(meta.logical_name);
        }
        return out;
    }

    std::unordered_map<std::string, std::string> replacements_for_branch(const std::string &branch_id) {
        if (!branch_exists(branch_id)) {
            throw std::runtime_error("chronos_copy_branch_not_found:" + branch_id);
        }
        std::unordered_map<std::string, std::string> replacements;
        for (const auto &meta : load_table_metas()) {
            replacements.emplace(meta.logical_name, copy_branch_table_name(branch_id, meta.logical_name));
        }
        return replacements;
    }

    std::unordered_map<std::string, std::string> replacements_for_checkpoint(const std::string &checkpoint) {
        if (!checkpoint_exists(checkpoint)) {
            throw std::runtime_error("chronos_copy_checkpoint_not_found:" + checkpoint);
        }
        std::unordered_map<std::string, std::string> replacements;
        for (const auto &meta : load_table_metas()) {
            replacements.emplace(meta.logical_name, copy_checkpoint_table_name(checkpoint, meta.logical_name));
        }
        return replacements;
    }

    NativeSqlDriver &driver() { return *driver_; }

  private:
    bool table_registered(const std::string &table) {
        return !driver_->query(
            "SELECT 1 FROM _chronos_branch_tables WHERE backend = 'copy' AND table_name = ? LIMIT 1",
            {table}
        ).empty();
    }

    NativeTableMeta load_table_meta(const std::string &table) {
        auto rows = driver_->query(
            "SELECT table_name, physical_table, pk_columns, columns, column_defs "
            "FROM _chronos_branch_tables WHERE backend = 'copy' AND table_name = ? LIMIT 1",
            {table}
        );
        if (rows.empty()) {
            throw std::runtime_error("chronos_copy_table_not_registered:" + table);
        }
        return {
            native_as_string(rows[0][0]),
            native_as_string(rows[0][1]),
            parse_json_string_array(native_as_string(rows[0][3])),
            parse_json_string_array(native_as_string(rows[0][2])),
            parse_json_string_array(native_as_string(rows[0][4])),
            "",
            "",
            false,
        };
    }

    std::vector<NativeTableMeta> load_table_metas() {
        auto rows = driver_->query(
            "SELECT table_name, physical_table, pk_columns, columns, column_defs "
            "FROM _chronos_branch_tables WHERE backend = 'copy' ORDER BY table_name"
        );
        std::vector<NativeTableMeta> metas;
        metas.reserve(rows.size());
        for (const auto &row : rows) {
            metas.push_back({
                native_as_string(row[0]),
                native_as_string(row[1]),
                parse_json_string_array(native_as_string(row[3])),
                parse_json_string_array(native_as_string(row[2])),
                parse_json_string_array(native_as_string(row[4])),
                "",
                "",
                false,
            });
        }
        return metas;
    }

    std::vector<CopyIndexMeta> load_indexes_for_table(const std::string &table) {
        auto rows = driver_->query(
            "SELECT index_name, table_name, columns "
            "FROM _chronos_branch_indexes WHERE backend = 'copy' AND table_name = ? "
            "ORDER BY index_name",
            {table}
        );
        std::vector<CopyIndexMeta> indexes;
        indexes.reserve(rows.size());
        for (const auto &row : rows) {
            indexes.push_back({
                native_as_string(row[0]),
                native_as_string(row[1]),
                parse_json_string_array(native_as_string(row[2])),
            });
        }
        return indexes;
    }

    bool branch_exists(const std::string &branch_id) {
        return !driver_->query(
            "SELECT 1 FROM _chronos_branch_copy_branches WHERE branch_id = ? LIMIT 1",
            {branch_id}
        ).empty();
    }

    bool checkpoint_exists(const std::string &checkpoint) {
        return !driver_->query(
            "SELECT 1 FROM _chronos_branch_copy_checkpoints WHERE checkpoint_id = ? LIMIT 1",
            {checkpoint}
        ).empty();
    }

    std::vector<std::string> branch_ids() {
        auto rows = driver_->query("SELECT branch_id FROM _chronos_branch_copy_branches ORDER BY branch_id");
        std::vector<std::string> out;
        out.reserve(rows.size());
        for (const auto &row : rows) out.push_back(native_as_string(row[0]));
        return out;
    }

    std::vector<std::string> checkpoint_ids() {
        auto rows = driver_->query("SELECT checkpoint_id FROM _chronos_branch_copy_checkpoints ORDER BY checkpoint_id");
        std::vector<std::string> out;
        out.reserve(rows.size());
        for (const auto &row : rows) out.push_back(native_as_string(row[0]));
        return out;
    }

    void create_physical_table(
        const std::string &physical,
        const std::vector<std::string> &defs,
        const std::vector<std::string> &primary_key
    ) {
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS " + quote_ident(physical) +
            " (" + comma_join(defs) + ", PRIMARY KEY (" + comma_join_quoted(primary_key) + "))"
        );
    }

    static std::string comma_join(const std::vector<std::string> &items) {
        std::string out;
        for (std::size_t i = 0; i < items.size(); ++i) {
            if (i) out += ", ";
            out += items[i];
        }
        return out;
    }

    void copy_table(
        const std::string &source,
        const std::string &dest,
        const NativeTableMeta &meta
    ) {
        create_physical_table(dest, meta.column_defs, meta.pk_columns);
        const std::string cols = comma_join_quoted(meta.columns);
        driver_->execute(
            "INSERT INTO " + quote_ident(dest) + " (" + cols + ") "
            "SELECT " + cols + " FROM " + quote_ident(source)
        );
    }

    void create_existing_indexes_for_table(
        const NativeTableMeta &meta,
        const std::string &owner_id,
        bool checkpoint
    ) {
        for (const auto &index : load_indexes_for_table(meta.logical_name)) {
            create_physical_index(meta, index, owner_id, checkpoint);
        }
    }

    void create_physical_index(
        const NativeTableMeta &meta,
        const CopyIndexMeta &index,
        const std::string &owner_id,
        bool checkpoint
    ) {
        std::unordered_set<std::string> visible_columns(meta.columns.begin(), meta.columns.end());
        for (const auto &column : index.columns) {
            if (visible_columns.find(column) == visible_columns.end()) return;
        }
        const std::string physical = checkpoint
            ? copy_checkpoint_table_name(owner_id, meta.logical_name)
            : copy_branch_table_name(owner_id, meta.logical_name);
        const std::string index_name = checkpoint
            ? "_chronos_idx_copy_cp_" + copy_identifier_token(owner_id) + "_" + copy_identifier_token(index.name)
            : "_chronos_idx_copy_" + copy_identifier_token(owner_id) + "_" + copy_identifier_token(index.name);
        driver_->execute(
            "CREATE INDEX IF NOT EXISTS " + quote_ident(index_name) +
            " ON " + quote_ident(physical) +
            " (" + comma_join_quoted(index.columns) + ")"
        );
    }

    std::unique_ptr<NativeSqlDriver> driver_;
};

class NativeCopyBranchSessionImpl {
  public:
    NativeCopyBranchSessionImpl(
        NativeCopyBranchStoreImpl &store,
        std::string branch_id,
        bool readonly,
        std::unordered_map<std::string, std::string> replacements
    ) : store_(store),
        branch_id_(std::move(branch_id)),
        readonly_(readonly),
        replacements_(std::move(replacements)) {}

    QueryResult query(const std::string &sql, const std::vector<Value> &params) {
        return store_.driver().query_result(rewrite_query(sql), params);
    }

    QueryResult explain(const std::string &sql, const std::vector<Value> &params) {
        return store_.driver().query_result("EXPLAIN " + rewrite_query(sql), params);
    }

    std::string rewrite_query(const std::string &sql) {
        auto found = rewrite_cache_.find(sql);
        if (found != rewrite_cache_.end()) return found->second;
        if (rewrite_cache_.size() > 512) rewrite_cache_.clear();
        auto inserted = rewrite_cache_.emplace(sql, copy_rewrite_sql(sql, replacements_));
        return inserted.first->second;
    }

    std::int64_t execute(const std::string &sql, const std::vector<Value> &params) {
        copy_raise_if_readonly(readonly_);
        if (starts_with_sql_keyword(sql, "CREATE") ||
            starts_with_sql_keyword(sql, "ALTER") ||
            starts_with_sql_keyword(sql, "DROP")) {
            throw std::runtime_error("branch-local schema changes are disabled");
        }
        try {
            return store_.driver().execute_changes(rewrite_query(sql), params);
        } catch (const std::exception &exc) {
            std::string message = exc.what();
            std::string lowered = message;
            std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            if (lowered.find("duplicate key") != std::string::npos ||
                lowered.find("unique constraint") != std::string::npos) {
                throw std::runtime_error("chronos_copy_duplicate_key:" + message);
            }
            throw;
        }
    }

    void upsert_rows(
        const std::string &logical_table,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows
    ) {
        copy_raise_if_readonly(readonly_);
        if (rows.empty()) return;
        auto target = replacements_.find(logical_table);
        if (target == replacements_.end()) {
            throw std::runtime_error("chronos_copy_table_not_registered:" + logical_table);
        }
        const std::string physical = target->second;
        const std::string insert_sql =
            "INSERT INTO " + quote_ident(physical) +
            " (" + comma_join_quoted(columns) + ") VALUES (" + placeholders(columns.size()) + ")";
        std::vector<std::string> set_columns;
        for (const auto &column : columns) {
            if (std::find(pk_columns.begin(), pk_columns.end(), column) == pk_columns.end()) {
                set_columns.push_back(column);
            }
        }
        std::string update_sql;
        if (!set_columns.empty()) {
            update_sql =
                "UPDATE " + quote_ident(physical) + " SET ";
            for (std::size_t i = 0; i < set_columns.size(); ++i) {
                if (i) update_sql += ", ";
                update_sql += quote_ident(set_columns[i]) + " = ?";
            }
            update_sql += " WHERE " + copy_key_where_sql(pk_columns);
        }
        const std::string exists_sql =
            "SELECT 1 FROM " + quote_ident(physical) +
            " WHERE " + copy_key_where_sql(pk_columns) + " LIMIT 1";
        for (const auto &row : rows) {
            const std::vector<Value> key_values = copy_key_values(columns, pk_columns, row);
            if (store_.driver().query(exists_sql, key_values).empty()) {
                store_.driver().execute(insert_sql, row);
                continue;
            }
            if (set_columns.empty()) continue;
            std::vector<Value> update_params;
            update_params.reserve(set_columns.size() + key_values.size());
            for (const auto &column : set_columns) {
                update_params.push_back(row[column_index(columns, column)]);
            }
            update_params.insert(update_params.end(), key_values.begin(), key_values.end());
            store_.driver().execute(update_sql, update_params);
        }
    }

    void delete_rows(
        const std::string &logical_table,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows
    ) {
        copy_raise_if_readonly(readonly_);
        if (rows.empty()) return;
        auto target = replacements_.find(logical_table);
        if (target == replacements_.end()) {
            throw std::runtime_error("chronos_copy_table_not_registered:" + logical_table);
        }
        const std::string delete_sql =
            "DELETE FROM " + quote_ident(target->second) +
            " WHERE " + copy_key_where_sql(pk_columns);
        for (const auto &row : rows) {
            store_.driver().execute(delete_sql, copy_key_values(columns, pk_columns, row));
        }
    }

    void begin() {
        if (!store_.driver().in_transaction()) {
            store_.driver().execute("BEGIN");
        }
    }
    void commit() {
        if (store_.driver().in_transaction()) {
            store_.driver().execute("COMMIT");
        }
    }
    void rollback() {
        if (store_.driver().in_transaction()) {
            try {
                store_.driver().execute("ROLLBACK");
            } catch (...) {
            }
        }
    }
    bool in_transaction() const { return store_.driver().in_transaction(); }

  private:
    NativeCopyBranchStoreImpl &store_;
    std::string branch_id_;
    bool readonly_ = false;
    std::unordered_map<std::string, std::string> replacements_;
    std::unordered_map<std::string, std::string> rewrite_cache_;
};

NativeCopyBranchSession NativeCopyBranchStoreImpl::checkout(const std::string &branch_id) {
    return NativeCopyBranchSession(
        std::make_unique<NativeCopyBranchSessionImpl>(
            *this,
            branch_id,
            false,
            replacements_for_branch(branch_id)
        )
    );
}

NativeCopyBranchSession NativeCopyBranchStoreImpl::checkout_checkpoint(const std::string &checkpoint) {
    NativeCheckpointInfo info = get_checkpoint_info(checkpoint);
    return NativeCopyBranchSession(
        std::make_unique<NativeCopyBranchSessionImpl>(
            *this,
            info.branch_id,
            true,
            replacements_for_checkpoint(checkpoint)
        )
    );
}

NativeCopyBranchSession::NativeCopyBranchSession(std::unique_ptr<NativeCopyBranchSessionImpl> impl)
    : impl_(std::move(impl)) {}
NativeCopyBranchSession::~NativeCopyBranchSession() = default;
NativeCopyBranchSession::NativeCopyBranchSession(NativeCopyBranchSession &&) noexcept = default;
NativeCopyBranchSession &NativeCopyBranchSession::operator=(NativeCopyBranchSession &&) noexcept = default;

IntervalQueryResult NativeCopyBranchSession::query(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    QueryResult result = impl_->query(sql, params);
    return {std::move(result.columns), std::move(result.rows)};
}

IntervalQueryResult NativeCopyBranchSession::explain(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    QueryResult result = impl_->explain(sql, params);
    return {std::move(result.columns), std::move(result.rows)};
}

std::string NativeCopyBranchSession::rewrite_query(const std::string &sql) {
    return impl_->rewrite_query(sql);
}

std::int64_t NativeCopyBranchSession::execute(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    return impl_->execute(sql, params);
}

void NativeCopyBranchSession::upsert_rows(
    const std::string &logical_table,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const IntervalRows &rows
) {
    impl_->upsert_rows(logical_table, columns, pk_columns, rows);
}

void NativeCopyBranchSession::delete_rows(
    const std::string &logical_table,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const IntervalRows &rows
) {
    impl_->delete_rows(logical_table, columns, pk_columns, rows);
}

void NativeCopyBranchSession::begin() { impl_->begin(); }
void NativeCopyBranchSession::commit() { impl_->commit(); }
void NativeCopyBranchSession::rollback() { impl_->rollback(); }
bool NativeCopyBranchSession::in_transaction() const { return impl_->in_transaction(); }

NativeCopyBranchStore::NativeCopyBranchStore(const std::string &database_url)
    : impl_(std::make_unique<NativeCopyBranchStoreImpl>(database_url)) {}
NativeCopyBranchStore::NativeCopyBranchStore(sqlite3 *db)
    : impl_(std::make_unique<NativeCopyBranchStoreImpl>(db)) {}
NativeCopyBranchStore::~NativeCopyBranchStore() = default;
NativeCopyBranchStore::NativeCopyBranchStore(NativeCopyBranchStore &&) noexcept = default;
NativeCopyBranchStore &NativeCopyBranchStore::operator=(NativeCopyBranchStore &&) noexcept = default;

const std::string &NativeCopyBranchStore::dialect() const { return impl_->dialect(); }
void NativeCopyBranchStore::ensure() { impl_->ensure_metadata(); }
void NativeCopyBranchStore::register_table(
    const std::string &table,
    const std::vector<std::string> &primary_key
) {
    impl_->register_table(table, primary_key);
}
std::string NativeCopyBranchStore::create_index(
    const std::string &table,
    const std::vector<std::string> &columns,
    const std::string &name
) {
    return impl_->create_index(table, columns, name);
}
NativeCopyBranchSession NativeCopyBranchStore::checkout(const std::string &branch_id) {
    return impl_->checkout(branch_id);
}
NativeCopyBranchSession NativeCopyBranchStore::checkout_checkpoint(const std::string &checkpoint) {
    return impl_->checkout_checkpoint(checkpoint);
}
void NativeCopyBranchStore::create_branch(
    const std::string &branch_id,
    const std::string &from_branch,
    const std::string &metadata_json
) {
    impl_->create_branch(branch_id, from_branch, metadata_json);
}
void NativeCopyBranchStore::create_branch_from_checkpoint(
    const std::string &branch_id,
    const std::string &checkpoint
) {
    impl_->create_branch_from_checkpoint(branch_id, checkpoint);
}
void NativeCopyBranchStore::delete_branch(const std::string &branch_id) {
    impl_->delete_branch(branch_id);
}
NativeBranchInfo NativeCopyBranchStore::update_branch_metadata(
    const std::string &branch_id,
    const std::string &metadata_json
) {
    return impl_->update_branch_metadata(branch_id, metadata_json);
}
NativeBranchInfo NativeCopyBranchStore::get_branch(const std::string &branch_id) {
    return impl_->get_branch_info(branch_id);
}
std::vector<NativeBranchInfo> NativeCopyBranchStore::list_branches() {
    return impl_->list_branch_infos();
}
NativeCheckpointInfo NativeCopyBranchStore::create_checkpoint(
    const std::string &checkpoint,
    const std::string &branch,
    const std::string &metadata_json
) {
    return impl_->create_checkpoint(checkpoint, branch, metadata_json);
}
NativeCheckpointInfo NativeCopyBranchStore::get_checkpoint(const std::string &checkpoint) {
    return impl_->get_checkpoint_info(checkpoint);
}
std::vector<NativeCheckpointInfo> NativeCopyBranchStore::list_checkpoints(const std::string &branch) {
    return impl_->list_checkpoint_infos(branch);
}
IntervalQueryResult NativeCopyBranchStore::visible_rows(
    const std::string &branch_id,
    const std::string &table
) {
    QueryResult result = impl_->visible_rows(branch_id, table);
    return {std::move(result.columns), std::move(result.rows)};
}
NativeTableInfo NativeCopyBranchStore::table_info(
    const std::string &branch_id,
    const std::string &table
) {
    return impl_->table_info(branch_id, table);
}
std::vector<std::string> NativeCopyBranchStore::table_names() {
    return impl_->table_names();
}
void NativeCopyBranchStore::commit() { impl_->commit(); }
void NativeCopyBranchStore::rollback() { impl_->rollback(); }
bool NativeCopyBranchStore::in_transaction() const { return impl_->in_transaction(); }

} // namespace chronos::native
