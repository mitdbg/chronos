#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <libpq-fe.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <Python.h>
#include <sqlite3.h>

#include "interval_data_plane.hpp"

namespace py = pybind11;

namespace {

using Blob = std::vector<unsigned char>;
using Value = std::variant<std::monostate, std::int64_t, double, std::string, Blob>;
using NativeRows = std::vector<std::vector<Value>>;

struct BoundSql {
    std::string sql;
    std::vector<Value> positional_params;
};

struct SqlBindPlan {
    std::string sql;
    std::vector<std::string> param_names;
};

struct BulkUpsertStats {
    std::int64_t selected = 0;
    std::int64_t deleted_rows = 0;
    std::int64_t inserted = 0;
};

struct NativeRowKey {
    std::vector<Value> values;

    bool operator==(const NativeRowKey &other) const {
        return values == other.values;
    }
};

typedef struct {
    PyObject_HEAD
    sqlite3 *db;
} PySqliteConnectionPrefix;

std::string quote_ident(const std::string &identifier) {
    std::string out = "\"";
    for (char ch : identifier) {
        if (ch == '"') {
            out += "\"\"";
        } else {
            out += ch;
        }
    }
    out += "\"";
    return out;
}

std::string placeholders(std::size_t count) {
    std::string out;
    for (std::size_t i = 0; i < count; ++i) {
        if (i) {
            out += ", ";
        }
        out += "?";
    }
    return out;
}

Value py_to_value(const py::handle &obj) {
    if (obj.is_none()) {
        return std::monostate{};
    }
    if (py::isinstance<py::bool_>(obj)) {
        return static_cast<std::int64_t>(obj.cast<bool>() ? 1 : 0);
    }
    if (py::isinstance<py::int_>(obj)) {
        return obj.cast<std::int64_t>();
    }
    if (py::isinstance<py::float_>(obj)) {
        return obj.cast<double>();
    }
    if (py::isinstance<py::bytes>(obj)) {
        std::string bytes = obj.cast<std::string>();
        return Blob(bytes.begin(), bytes.end());
    }
    if (py::isinstance<py::str>(obj)) {
        return obj.cast<std::string>();
    }
    return py::str(obj).cast<std::string>();
}

std::size_t hash_combine(std::size_t seed, std::size_t value) {
    return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

std::size_t value_hash(const Value &value) {
    if (std::holds_alternative<std::monostate>(value)) {
        return 0x6eed0e9da4d94a4fULL;
    }
    if (auto ptr = std::get_if<std::int64_t>(&value)) {
        return hash_combine(0x01ULL, std::hash<std::int64_t>{}(*ptr));
    }
    if (auto ptr = std::get_if<double>(&value)) {
        return hash_combine(0x02ULL, std::hash<double>{}(*ptr));
    }
    if (auto ptr = std::get_if<std::string>(&value)) {
        return hash_combine(0x03ULL, std::hash<std::string>{}(*ptr));
    }
    const auto &blob = std::get<Blob>(value);
    std::size_t hash = 1469598103934665603ULL;
    for (unsigned char byte : blob) {
        hash ^= static_cast<std::size_t>(byte);
        hash *= 1099511628211ULL;
    }
    return hash_combine(0x04ULL, hash);
}

struct NativeRowKeyHash {
    std::size_t operator()(const NativeRowKey &key) const {
        std::size_t seed = 0x54d6c1b4a51f2d7bULL;
        for (const auto &value : key.values) {
            seed = hash_combine(seed, value_hash(value));
        }
        return seed;
    }
};

py::object value_to_py(const Value &value) {
    if (std::holds_alternative<std::monostate>(value)) {
        return py::none();
    }
    if (auto ptr = std::get_if<std::int64_t>(&value)) {
        return py::int_(*ptr);
    }
    if (auto ptr = std::get_if<double>(&value)) {
        return py::float_(*ptr);
    }
    if (auto ptr = std::get_if<std::string>(&value)) {
        return py::str(*ptr);
    }
    const auto &blob = std::get<Blob>(value);
    return py::bytes(reinterpret_cast<const char *>(blob.data()), blob.size());
}

NativeRows rows_to_native_values(const py::list &rows, const std::vector<std::string> &columns) {
    NativeRows native_rows;
    native_rows.reserve(static_cast<std::size_t>(py::len(rows)));
    for (const auto item : rows) {
        py::dict row = py::reinterpret_borrow<py::dict>(item);
        std::vector<Value> values;
        values.reserve(columns.size());
        for (const auto &column : columns) {
            py::object key = py::str(column);
            if (row.contains(key)) {
                values.push_back(py_to_value(row[key]));
            } else {
                values.push_back(std::monostate{});
            }
        }
        native_rows.push_back(std::move(values));
    }
    return native_rows;
}

std::vector<Value> list_to_values(const py::list &items) {
    std::vector<Value> values;
    values.reserve(static_cast<std::size_t>(py::len(items)));
    for (const auto item : items) {
        values.push_back(py_to_value(item));
    }
    return values;
}

bool is_identifier_start(char ch) {
    return std::isalpha(static_cast<unsigned char>(ch)) || ch == '_';
}

bool is_identifier_part(char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

SqlBindPlan plan_named_sql(const std::string &sql) {
    SqlBindPlan plan;
    plan.sql.reserve(sql.size());
    bool single_quote = false;
    bool double_quote = false;
    for (std::size_t i = 0; i < sql.size(); ++i) {
        const char ch = sql[i];
        if (ch == '\'' && !double_quote) {
            single_quote = !single_quote;
            plan.sql.push_back(ch);
            continue;
        }
        if (ch == '"' && !single_quote) {
            double_quote = !double_quote;
            plan.sql.push_back(ch);
            continue;
        }
        if (!single_quote && !double_quote && ch == ':' && i + 1 < sql.size() && is_identifier_start(sql[i + 1])) {
            std::size_t j = i + 2;
            while (j < sql.size() && is_identifier_part(sql[j])) {
                ++j;
            }
            std::string name = sql.substr(i + 1, j - i - 1);
            plan.sql.push_back('?');
            plan.param_names.push_back(std::move(name));
            i = j - 1;
            continue;
        }
        plan.sql.push_back(ch);
    }
    return plan;
}

BoundSql bind_named_sql_plan(const SqlBindPlan &plan, const py::dict &params) {
    BoundSql bound;
    bound.sql = plan.sql;
    bound.positional_params.reserve(plan.param_names.size());
    for (const auto &name : plan.param_names) {
        py::str key(name);
        if (!params.contains(key)) {
            throw std::invalid_argument("missing SQL parameter: " + name);
        }
        bound.positional_params.push_back(py_to_value(params[key]));
    }
    return bound;
}

BoundSql bind_sql_params(const std::string &sql, const py::object &params) {
    if (py::isinstance<py::dict>(params)) {
        return bind_named_sql_plan(plan_named_sql(sql), params.cast<py::dict>());
    }
    BoundSql bound;
    bound.sql = sql;
    if (params.is_none()) {
        return bound;
    }
    py::sequence seq = params.cast<py::sequence>();
    bound.positional_params.reserve(static_cast<std::size_t>(py::len(seq)));
    for (const auto item : seq) {
        bound.positional_params.push_back(py_to_value(item));
    }
    return bound;
}

std::optional<std::string> lookup_replacement(const py::dict &replacements, const std::string &name) {
    py::str key(name);
    if (replacements.contains(key)) {
        return replacements[key].cast<std::string>();
    }
    return std::nullopt;
}

std::string rewrite_visible_tables(const std::string &sql, const py::dict &replacements) {
    std::string out;
    out.reserve(sql.size() * 2);
    bool single_quote = false;
    bool double_quote = false;
    bool expect_table = false;
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
            if (expect_table && j < sql.size()) {
                if (auto replacement = lookup_replacement(replacements, ident)) {
                    std::string ltrimmed = *replacement;
                    ltrimmed.erase(0, ltrimmed.find_first_not_of(" \n\r\t"));
                    std::string head = ltrimmed.substr(0, std::min<std::size_t>(6, ltrimmed.size()));
                    std::transform(head.begin(), head.end(), head.begin(), [](unsigned char c) {
                        return static_cast<char>(std::toupper(c));
                    });
                    if (head == "SELECT" || head.rfind("WITH", 0) == 0) {
                        out += "(" + *replacement + ")";
                    } else {
                        out += quote_ident(*replacement);
                    }
                } else {
                    out.append(sql, i, j - i + 1);
                }
                expect_table = false;
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
        while (j < sql.size() && is_identifier_part(sql[j])) {
            ++j;
        }
        std::string ident = sql.substr(i, j - i);
        std::string upper = ident;
        std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) {
            return static_cast<char>(std::toupper(c));
        });

        if (expect_table) {
            if (auto replacement = lookup_replacement(replacements, ident)) {
                const std::string trimmed = replacement->substr(0, replacement->find_last_not_of(" \n\r\t") + 1);
                std::string ltrimmed = trimmed;
                ltrimmed.erase(0, ltrimmed.find_first_not_of(" \n\r\t"));
                std::string head = ltrimmed.substr(0, std::min<std::size_t>(6, ltrimmed.size()));
                std::transform(head.begin(), head.end(), head.begin(), [](unsigned char c) {
                    return static_cast<char>(std::toupper(c));
                });
                if (head == "SELECT" || head.rfind("WITH", 0) == 0) {
                    out += "(" + *replacement + ")";
                } else {
                    out += quote_ident(*replacement);
                }
            } else {
                out += ident;
            }
            expect_table = false;
            i = j;
            continue;
        }

        out += ident;
        if (upper == "FROM" || upper == "JOIN" || upper == "INTO" || upper == "UPDATE") {
            expect_table = true;
        }
        i = j;
    }
    return out;
}

std::size_t column_index(const std::vector<std::string> &columns, const std::string &column) {
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (columns[i] == column) {
            return i;
        }
    }
    throw std::invalid_argument("primary key column is not present in columns: " + column);
}

std::vector<std::size_t> pk_column_indices(
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns
) {
    std::vector<std::size_t> indices;
    indices.reserve(pk_columns.size());
    for (const auto &column : pk_columns) {
        indices.push_back(column_index(columns, column));
    }
    return indices;
}

NativeRows dedupe_rows_by_pk(const NativeRows &rows, const std::vector<std::size_t> &pk_indices) {
    NativeRows deduped;
    deduped.reserve(rows.size());
    std::unordered_map<NativeRowKey, std::size_t, NativeRowKeyHash> positions;
    positions.reserve(rows.size() * 2 + 1);
    for (const auto &row : rows) {
        NativeRowKey key;
        key.values.reserve(pk_indices.size());
        for (std::size_t index : pk_indices) {
            key.values.push_back(row[index]);
        }
        auto found = positions.find(key);
        if (found == positions.end()) {
            positions.emplace(std::move(key), deduped.size());
            deduped.push_back(row);
        } else {
            deduped[found->second] = row;
        }
    }
    return deduped;
}

class SqliteError : public std::runtime_error {
  public:
    explicit SqliteError(sqlite3 *db) : std::runtime_error(sqlite3_errmsg(db)) {}
    explicit SqliteError(const std::string &message) : std::runtime_error(message) {}
};

void execute_sql(sqlite3 *db, const std::string &sql) {
    char *error = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error);
    if (rc != SQLITE_OK) {
        std::string message = error ? error : sqlite3_errmsg(db);
        sqlite3_free(error);
        throw SqliteError(message);
    }
}

int execute_changes(sqlite3 *db, const std::string &sql) {
    execute_sql(db, sql);
    return sqlite3_changes(db);
}

class SqliteStatement {
  public:
    SqliteStatement(sqlite3 *db, const std::string &sql) : db_(db), stmt_(nullptr) {
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK) {
            throw SqliteError(db_);
        }
    }

    ~SqliteStatement() {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    SqliteStatement(const SqliteStatement &) = delete;
    SqliteStatement &operator=(const SqliteStatement &) = delete;

    sqlite3_stmt *get() { return stmt_; }

    void reset() {
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
    }

    void bind(int index, const Value &value) {
        int rc = SQLITE_OK;
        if (std::holds_alternative<std::monostate>(value)) {
            rc = sqlite3_bind_null(stmt_, index);
        } else if (auto ptr = std::get_if<std::int64_t>(&value)) {
            rc = sqlite3_bind_int64(stmt_, index, *ptr);
        } else if (auto ptr = std::get_if<double>(&value)) {
            rc = sqlite3_bind_double(stmt_, index, *ptr);
        } else if (auto ptr = std::get_if<std::string>(&value)) {
            rc = sqlite3_bind_text(stmt_, index, ptr->c_str(), static_cast<int>(ptr->size()), SQLITE_TRANSIENT);
        } else {
            const auto &blob = std::get<Blob>(value);
            rc = sqlite3_bind_blob(stmt_, index, blob.data(), static_cast<int>(blob.size()), SQLITE_TRANSIENT);
        }
        if (rc != SQLITE_OK) {
            throw SqliteError(db_);
        }
    }

    void bind_int64(int index, std::int64_t value) {
        if (sqlite3_bind_int64(stmt_, index, value) != SQLITE_OK) {
            throw SqliteError(db_);
        }
    }

    bool step_row() {
        int rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW) {
            return true;
        }
        if (rc == SQLITE_DONE) {
            return false;
        }
        throw SqliteError(db_);
    }

    void step_done() {
        int rc = sqlite3_step(stmt_);
        if (rc != SQLITE_DONE) {
            throw SqliteError(db_);
        }
    }

  private:
    sqlite3 *db_;
    sqlite3_stmt *stmt_;
};

Value column_value(sqlite3_stmt *stmt, int index) {
    int type = sqlite3_column_type(stmt, index);
    switch (type) {
    case SQLITE_NULL:
        return std::monostate{};
    case SQLITE_INTEGER:
        return static_cast<std::int64_t>(sqlite3_column_int64(stmt, index));
    case SQLITE_FLOAT:
        return sqlite3_column_double(stmt, index);
    case SQLITE_BLOB: {
        const auto *data = static_cast<const unsigned char *>(sqlite3_column_blob(stmt, index));
        int size = sqlite3_column_bytes(stmt, index);
        if (size <= 0) {
            return Blob{};
        }
        return Blob(data, data + size);
    }
    case SQLITE_TEXT:
    default: {
        const auto *data = reinterpret_cast<const char *>(sqlite3_column_text(stmt, index));
        int size = sqlite3_column_bytes(stmt, index);
        return std::string(data, size);
    }
    }
}

py::list rows_from_statement(SqliteStatement &stmt) {
    py::list rows;
    const int column_count = sqlite3_column_count(stmt.get());
    while (stmt.step_row()) {
        py::dict row;
        for (int i = 0; i < column_count; ++i) {
            row[sqlite3_column_name(stmt.get(), i)] = value_to_py(column_value(stmt.get(), i));
        }
        rows.append(row);
    }
    return rows;
}

class SQLiteConnector {
  public:
    explicit SQLiteConnector(const std::string &path) : db_(nullptr) {
        if (sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
            sqlite3 *failed = db_;
            db_ = nullptr;
            throw SqliteError(failed);
        }
    }

    ~SQLiteConnector() {
        if (db_) {
            sqlite3_close(db_);
        }
    }

    SQLiteConnector(const SQLiteConnector &) = delete;
    SQLiteConnector &operator=(const SQLiteConnector &) = delete;

    void execute(const std::string &sql) {
        execute_sql(db_, sql);
    }

    int execute_changes(const std::string &sql) {
        return ::execute_changes(db_, sql);
    }

    py::list query(const std::string &sql) {
        SqliteStatement stmt(db_, sql);
        py::list rows;
        int column_count = sqlite3_column_count(stmt.get());
        while (stmt.step_row()) {
            py::dict row;
            for (int i = 0; i < column_count; ++i) {
                row[sqlite3_column_name(stmt.get(), i)] = value_to_py(column_value(stmt.get(), i));
            }
            rows.append(row);
        }
        return rows;
    }

    sqlite3 *raw() { return db_; }

  private:
    sqlite3 *db_;
};

class PgError : public std::runtime_error {
  public:
    explicit PgError(const std::string &message) : std::runtime_error(message) {}
};

class PostgresConnector {
  public:
    explicit PostgresConnector(const std::string &conninfo) : conn_(PQconnectdb(conninfo.c_str())) {
        if (!conn_ || PQstatus(conn_) != CONNECTION_OK) {
            std::string message = conn_ ? PQerrorMessage(conn_) : "could not allocate PostgreSQL connection";
            if (conn_) {
                PQfinish(conn_);
                conn_ = nullptr;
            }
            throw PgError(message);
        }
    }

    ~PostgresConnector() {
        if (conn_) {
            PQfinish(conn_);
        }
    }

    PostgresConnector(const PostgresConnector &) = delete;
    PostgresConnector &operator=(const PostgresConnector &) = delete;

    void execute(const std::string &sql) {
        PGresult *result = PQexec(conn_, sql.c_str());
        ExecStatusType status = PQresultStatus(result);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            std::string message = PQerrorMessage(conn_);
            PQclear(result);
            throw PgError(message);
        }
        PQclear(result);
    }

    int server_version() const { return PQserverVersion(conn_); }

  private:
    PGconn *conn_;
};

std::string comma_join_quoted(const std::vector<std::string> &columns, const std::string &prefix = "") {
    std::string out;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i) {
            out += ", ";
        }
        if (!prefix.empty()) {
            out += prefix + ".";
        }
        out += quote_ident(columns[i]);
    }
    return out;
}

std::string pk_join_sql(
    const std::vector<std::string> &pk_columns,
    const std::string &left_alias,
    const std::string &right_alias
) {
    std::string out;
    for (std::size_t i = 0; i < pk_columns.size(); ++i) {
        if (i) {
            out += " AND ";
        }
        std::string quoted = quote_ident(pk_columns[i]);
        out += left_alias + "." + quoted + " = " + right_alias + "." + quoted;
    }
    return out;
}

std::string temp_name(const std::string &prefix) {
    static std::atomic<std::uint64_t> counter{0};
    return "__chronos_native_" + prefix + "_" + std::to_string(++counter);
}

std::int64_t scalar_count(SQLiteConnector &conn, const std::string &sql) {
    SqliteStatement stmt(conn.raw(), sql);
    if (!stmt.step_row()) {
        return 0;
    }
    return sqlite3_column_int64(stmt.get(), 0);
}

std::int64_t scalar_count(sqlite3 *db, const std::string &sql) {
    SqliteStatement stmt(db, sql);
    if (!stmt.step_row()) {
        return 0;
    }
    return sqlite3_column_int64(stmt.get(), 0);
}

struct PhysicalRow {
    std::int64_t rowid;
    std::vector<Value> values;
    std::int64_t live_lo;
    std::int64_t live_hi;
    std::int64_t writer_segment_id;
    std::int64_t deleted;
};

sqlite3 *sqlite_db_from_python_connection(const py::handle &connection) {
    PyObject *obj = connection.ptr();
    const char *type_name = Py_TYPE(obj)->tp_name;
    if (std::string(type_name) != "sqlite3.Connection") {
        throw std::invalid_argument("expected sqlite3.Connection, got " + std::string(type_name));
    }
    auto *prefix = reinterpret_cast<PySqliteConnectionPrefix *>(obj);
    if (prefix->db == nullptr) {
        throw SqliteError("sqlite3 connection is closed");
    }
    return prefix->db;
}

std::string key_where_sql(const std::vector<std::string> &pk_columns) {
    std::string key_where;
    for (std::size_t i = 0; i < pk_columns.size(); ++i) {
        if (i) {
            key_where += " AND ";
        }
        key_where += quote_ident(pk_columns[i]) + " = ?";
    }
    return key_where;
}

void bind_key_values(SqliteStatement &stmt, const std::vector<Value> &key_values, int &bind_index) {
    for (const auto &value : key_values) {
        stmt.bind(bind_index++, value);
    }
}

py::dict stats_to_py_dict(
    const BulkUpsertStats &native_stats,
    std::size_t input_rows,
    const std::string &strategy
);

BulkUpsertStats sqlite_adapter_bulk_upsert(
    sqlite3 *db,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const NativeRows &rows,
    std::int64_t live_lo,
    std::int64_t live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction
) {
    if (columns.empty()) {
        throw std::invalid_argument("columns must not be empty");
    }
    if (pk_columns.empty()) {
        throw std::invalid_argument("pk_columns must not be empty");
    }

    BulkUpsertStats stats;
    const std::string quoted_table = quote_ident(physical_name);
    const std::vector<std::size_t> pk_indices = pk_column_indices(columns, pk_columns);
    // Chronos upserts are last-writer-wins within one logical batch.  Deduping
    // by primary key in native memory avoids running the interval splice more
    // than once for the same key and keeps the Python caller from owning any
    // part of the physical CoW algorithm.
    NativeRows deduped_storage;
    const NativeRows *splice_rows = &rows;
    if (rows.size() > 1) {
        deduped_storage = dedupe_rows_by_pk(rows, pk_indices);
        splice_rows = &deduped_storage;
    }

    std::string key_where;
    for (std::size_t i = 0; i < pk_columns.size(); ++i) {
        if (i) {
            key_where += " AND ";
        }
        key_where += quote_ident(pk_columns[i]) + " = ?";
    }

    const std::string data_cols = comma_join_quoted(columns);
    const std::string all_insert_cols = data_cols + ", \"live_lo\", \"live_hi\", \"writer_segment_id\", \"deleted\"";
    const std::string select_sql =
        "SELECT rowid, " + data_cols + ", live_lo, live_hi, writer_segment_id, deleted "
        "FROM " + quoted_table + " "
        "WHERE " + key_where + " AND live_lo < ? AND ? < live_hi "
        "ORDER BY live_lo";
    const std::string delete_sql =
        "DELETE FROM " + quoted_table + " WHERE rowid = ?";
    const std::string insert_sql =
        "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") "
        "VALUES (" + placeholders(columns.size() + 4) + ")";

    struct PhysicalRow {
        std::int64_t rowid;
        std::vector<Value> values;
        std::int64_t live_lo;
        std::int64_t live_hi;
        std::int64_t writer_segment_id;
        std::int64_t deleted;
    };

    auto bind_insert = [&](SqliteStatement &stmt,
                           const std::vector<Value> &values,
                           std::int64_t row_live_lo,
                           std::int64_t row_live_hi,
                           std::int64_t row_writer_segment_id,
                           std::int64_t row_deleted) {
        stmt.reset();
        int bind_index = 1;
        for (const auto &value : values) {
            stmt.bind(bind_index++, value);
        }
        stmt.bind_int64(bind_index++, row_live_lo);
        stmt.bind_int64(bind_index++, row_live_hi);
        stmt.bind_int64(bind_index++, row_writer_segment_id);
        stmt.bind_int64(bind_index++, row_deleted);
        stmt.step_done();
        ++stats.inserted;
    };

    try {
        if (manage_transaction) {
            // Standalone calls own the SQLite transaction.  Calls from
            // BranchSession.transaction() pass manage_transaction=false so this
            // native splice is atomic with metadata/inode updates in the
            // caller's already-open transaction.
            execute_sql(db, "BEGIN IMMEDIATE");
        }
        SqliteStatement select_stmt(db, select_sql);
        SqliteStatement delete_stmt(db, delete_sql);
        SqliteStatement insert_stmt(db, insert_sql);

        for (const auto &row : *splice_rows) {
            // Physical rows overlap the branch-local write interval when:
            //   old.live_lo < branch.live_hi AND branch.live_lo < old.live_hi
            // They may be inherited parent rows, rows written by sibling
            // segments, or prior rows written by this branch.  Visibility reads
            // later need only test branch_point against [live_lo, live_hi).
            select_stmt.reset();
            int bind_index = 1;
            for (std::size_t index : pk_indices) {
                select_stmt.bind(bind_index++, row[index]);
            }
            select_stmt.bind_int64(bind_index++, live_hi);
            select_stmt.bind_int64(bind_index++, live_lo);

            std::vector<PhysicalRow> physical_rows;
            while (select_stmt.step_row()) {
                PhysicalRow physical;
                physical.rowid = sqlite3_column_int64(select_stmt.get(), 0);
                physical.values.reserve(columns.size());
                for (std::size_t i = 0; i < columns.size(); ++i) {
                    physical.values.push_back(column_value(select_stmt.get(), static_cast<int>(1 + i)));
                }
                int metadata_offset = static_cast<int>(1 + columns.size());
                physical.live_lo = sqlite3_column_int64(select_stmt.get(), metadata_offset);
                physical.live_hi = sqlite3_column_int64(select_stmt.get(), metadata_offset + 1);
                physical.writer_segment_id = sqlite3_column_int64(select_stmt.get(), metadata_offset + 2);
                physical.deleted = sqlite3_column_int64(select_stmt.get(), metadata_offset + 3);
                physical_rows.push_back(std::move(physical));
            }
            stats.selected += static_cast<std::int64_t>(physical_rows.size());

            if (physical_rows.empty()) {
                // No existing interval covers this key.  Insert one row for the
                // whole mutable segment interval; future checkpoints/children
                // select it by their branch point.
                bind_insert(insert_stmt, row, live_lo, live_hi, writer_segment_id, replacement_deleted ? 1 : 0);
                continue;
            }

            for (const auto &physical : physical_rows) {
                const std::int64_t overlap_lo = std::max(physical.live_lo, live_lo);
                const std::int64_t overlap_hi = std::min(physical.live_hi, live_hi);

                // Interval CoW splice:
                // 1. Remove the overlapping physical row.
                // 2. Reinsert the left remainder, if any, with original bytes
                //    and original writer metadata.
                // 3. Insert the branch-local replacement interval.
                // 4. Reinsert the right remainder, if any, unchanged.
                //
                // This preserves sharing outside the overwritten interval while
                // making branch-local reads a pure indexed visibility lookup.
                delete_stmt.reset();
                delete_stmt.bind_int64(1, physical.rowid);
                delete_stmt.step_done();
                ++stats.deleted_rows;

                if (physical.live_lo < overlap_lo) {
                    bind_insert(
                        insert_stmt,
                        physical.values,
                        physical.live_lo,
                        overlap_lo,
                        physical.writer_segment_id,
                        physical.deleted
                    );
                }
                bind_insert(
                    insert_stmt,
                    replacement_deleted ? physical.values : row,
                    overlap_lo,
                    overlap_hi,
                    writer_segment_id,
                    replacement_deleted ? 1 : 0
                );
                if (overlap_hi < physical.live_hi) {
                    bind_insert(
                        insert_stmt,
                        physical.values,
                        overlap_hi,
                        physical.live_hi,
                        physical.writer_segment_id,
                        physical.deleted
                    );
                }
            }
        }
        if (manage_transaction) {
            execute_sql(db, "COMMIT");
        }
    } catch (...) {
        if (manage_transaction) {
            try {
                execute_sql(db, "ROLLBACK");
            } catch (...) {
            }
        }
        throw;
    }
    return stats;
}

py::dict stats_to_py_dict(const BulkUpsertStats &native_stats, std::size_t input_rows, const std::string &strategy) {
    py::dict result;
    result["input_rows"] = input_rows;
    result["selected_rows"] = native_stats.selected;
    result["deleted_rows"] = native_stats.deleted_rows;
    result["inserted_rows"] = native_stats.inserted;
    result["strategy"] = strategy;
    return result;
}

class IntervalConnectionAdapter {
  public:
    virtual ~IntervalConnectionAdapter() = default;

    virtual py::list query(const std::string &sql, const py::object &params) = 0;

    virtual int execute(const std::string &sql, const py::object &params) = 0;

    virtual BulkUpsertStats bulk_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        std::int64_t live_lo,
        std::int64_t live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction
    ) = 0;
};

class SQLiteIntervalConnectionAdapter final : public IntervalConnectionAdapter {
  public:
    explicit SQLiteIntervalConnectionAdapter(const py::object &connection)
        : db_(sqlite_db_from_python_connection(connection)) {}

    py::list query(const std::string &sql, const py::object &params) override {
        BoundSql bound = bind_sql_params_cached(sql, params);
        SqliteStatement &stmt = statement_for(bound.sql);
        stmt.reset();
        int bind_index = 1;
        for (const auto &value : bound.positional_params) {
            stmt.bind(bind_index++, value);
        }
        return rows_from_statement(stmt);
    }

    int execute(const std::string &sql, const py::object &params) override {
        BoundSql bound = bind_sql_params_cached(sql, params);
        SqliteStatement stmt(db_, bound.sql);
        int bind_index = 1;
        for (const auto &value : bound.positional_params) {
            stmt.bind(bind_index++, value);
        }
        stmt.step_done();
        return sqlite3_changes(db_);
    }

    BulkUpsertStats bulk_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        std::int64_t live_lo,
        std::int64_t live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction
    ) override {
        return sqlite_adapter_bulk_upsert(
            db_,
            physical_name,
            columns,
            pk_columns,
            rows,
            live_lo,
            live_hi,
            writer_segment_id,
            replacement_deleted,
            manage_transaction
        );
    }

  private:
    BoundSql bind_sql_params_cached(const std::string &sql, const py::object &params) {
        if (py::isinstance<py::dict>(params)) {
            auto found = bind_plan_cache_.find(sql);
            if (found == bind_plan_cache_.end()) {
                if (bind_plan_cache_.size() > 512) {
                    bind_plan_cache_.clear();
                }
                found = bind_plan_cache_.emplace(sql, plan_named_sql(sql)).first;
            }
            return bind_named_sql_plan(found->second, params.cast<py::dict>());
        }
        return bind_sql_params(sql, params);
    }

    SqliteStatement &statement_for(const std::string &sql) {
        auto found = statement_cache_.find(sql);
        if (found != statement_cache_.end()) {
            return *found->second;
        }
        if (statement_cache_.size() > 256) {
            statement_cache_.clear();
        }
        auto inserted = statement_cache_.emplace(sql, std::make_unique<SqliteStatement>(db_, sql));
        return *inserted.first->second;
    }

    sqlite3 *db_;
    std::unordered_map<std::string, SqlBindPlan> bind_plan_cache_;
    std::unordered_map<std::string, std::unique_ptr<SqliteStatement>> statement_cache_;
};

bool supports_connection_dialect(const std::string &dialect) {
    return dialect == "sqlite";
}

std::unique_ptr<IntervalConnectionAdapter> make_interval_connection_adapter(
    const std::string &dialect,
    const py::object &connection
) {
    if (dialect == "sqlite") {
        return std::make_unique<SQLiteIntervalConnectionAdapter>(connection);
    }
    throw std::invalid_argument("native interval connection dialect is not supported: " + dialect);
}

py::dict interval_bulk_upsert_connection(
    const std::string &dialect,
    const py::object &connection,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const py::list &rows,
    std::int64_t live_lo,
    std::int64_t live_hi,
    std::int64_t writer_segment_id,
    bool manage_transaction
) {
    auto adapter = make_interval_connection_adapter(dialect, connection);
    NativeRows native_rows = rows_to_native_values(rows, columns);
    BulkUpsertStats stats;
    {
        py::gil_scoped_release release;
        stats = adapter->bulk_upsert(
            physical_name,
            columns,
            pk_columns,
            native_rows,
            live_lo,
            live_hi,
            writer_segment_id,
            false,
            manage_transaction
        );
    }
    return stats_to_py_dict(stats, native_rows.size(), "native_interval_bulk_upsert");
}

class NativeIntervalBackend {
  public:
    NativeIntervalBackend(const std::string &dialect, const py::object &connection)
        : dialect_(dialect),
          connection_(connection),
          adapter_(make_interval_connection_adapter(dialect_, connection_)) {}

    py::list query(const std::string &sql, const py::object &params) {
        return adapter_->query(sql, params);
    }

    py::list query_visible(
        const std::string &sql,
        const py::dict &params,
        const py::dict &replacements,
        std::int64_t branch_point
    ) {
        py::dict bound;
        for (const auto item : params) {
            bound[item.first] = item.second;
        }
        bound["_chronos_branch_point"] = branch_point;
        return adapter_->query(rewrite_visible_tables_cached(sql, replacements), bound);
    }

    int execute(const std::string &sql, const py::object &params) {
        return adapter_->execute(sql, params);
    }

    py::dict bulk_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const py::list &rows,
        std::int64_t live_lo,
        std::int64_t live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction
    ) {
        NativeRows native_rows = rows_to_native_values(rows, columns);
        BulkUpsertStats stats;
        {
            py::gil_scoped_release release;
            stats = adapter_->bulk_upsert(
                physical_name,
                columns,
                pk_columns,
                native_rows,
                live_lo,
                live_hi,
                writer_segment_id,
                replacement_deleted,
                manage_transaction
            );
        }
        return stats_to_py_dict(stats, native_rows.size(), "native_interval_bulk_upsert");
    }

  private:
    std::string rewrite_cache_key(const std::string &sql, const py::dict &replacements) {
        std::vector<std::pair<std::string, std::string>> items;
        items.reserve(static_cast<std::size_t>(py::len(replacements)));
        for (const auto item : replacements) {
            items.emplace_back(py::str(item.first).cast<std::string>(), py::str(item.second).cast<std::string>());
        }
        std::sort(items.begin(), items.end());
        std::string key;
        key.reserve(sql.size() + 64 * items.size());
        key += std::to_string(sql.size());
        key.push_back(':');
        key += sql;
        for (const auto &[name, replacement] : items) {
            key.push_back('\x1f');
            key += std::to_string(name.size());
            key.push_back(':');
            key += name;
            key.push_back('=');
            key += std::to_string(replacement.size());
            key.push_back(':');
            key += replacement;
        }
        return key;
    }

    const std::string &rewrite_visible_tables_cached(const std::string &sql, const py::dict &replacements) {
        const std::string key = rewrite_cache_key(sql, replacements);
        auto found = rewrite_cache_.find(key);
        if (found != rewrite_cache_.end()) {
            return found->second;
        }
        if (rewrite_cache_.size() > 512) {
            rewrite_cache_.clear();
        }
        auto inserted = rewrite_cache_.emplace(key, rewrite_visible_tables(sql, replacements));
        return inserted.first->second;
    }

    std::string dialect_;
    py::object connection_;
    std::unique_ptr<IntervalConnectionAdapter> adapter_;
    std::unordered_map<std::string, std::string> rewrite_cache_;
};

py::dict recommended_sql_parser() {
    py::dict result;
    result["name"] = "libpg_query";
    result["scope"] = "PostgreSQL-compatible parsing and AST extraction";
    result["rationale"] = "Chronos native hot paths generate fixed SQL; when a native SQL parser is needed, libpg_query tracks PostgreSQL grammar more closely than generic C++ parsers.";
    result["not_for_hot_path"] = true;
    return result;
}

} // namespace

namespace chronos::native {

void bind_interval_data_plane(py::module_ &m) {
    m.def("recommended_sql_parser", &recommended_sql_parser);
    m.def("sqlite3_libversion", []() { return std::string(sqlite3_libversion()); });
    m.def("libpq_version", []() { return PQlibVersion(); });
    m.def("supports_connection_dialect", &supports_connection_dialect, py::arg("dialect"));
    m.def("interval_bulk_upsert_connection", &interval_bulk_upsert_connection,
          py::arg("dialect"), py::arg("connection"), py::arg("physical_name"),
          py::arg("columns"), py::arg("pk_columns"), py::arg("rows"), py::arg("live_lo"),
          py::arg("live_hi"), py::arg("writer_segment_id"), py::arg("manage_transaction"));

    py::class_<NativeIntervalBackend>(m, "NativeIntervalBackend")
        .def(py::init<const std::string &, const py::object &>())
        .def("query", &NativeIntervalBackend::query)
        .def("query_visible", &NativeIntervalBackend::query_visible)
        .def("execute", &NativeIntervalBackend::execute)
        .def("bulk_upsert", &NativeIntervalBackend::bulk_upsert);

    py::class_<SQLiteConnector>(m, "SQLiteConnector")
        .def(py::init<const std::string &>())
        .def("execute", &SQLiteConnector::execute)
        .def("query", &SQLiteConnector::query);

    py::class_<PostgresConnector>(m, "PostgresConnector")
        .def(py::init<const std::string &>())
        .def("execute", &PostgresConnector::execute)
        .def("server_version", &PostgresConnector::server_version);
}

} // namespace chronos::native
