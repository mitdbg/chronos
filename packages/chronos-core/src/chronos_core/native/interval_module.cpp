#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <libpq-fe.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sqlite3.h>

namespace py = pybind11;

namespace {

using Blob = std::vector<unsigned char>;
using Value = std::variant<std::monostate, std::int64_t, double, std::string, Blob>;

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
    return obj.cast<std::string>();
}

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

class SqliteError : public std::runtime_error {
  public:
    explicit SqliteError(sqlite3 *db) : std::runtime_error(sqlite3_errmsg(db)) {}
    explicit SqliteError(const std::string &message) : std::runtime_error(message) {}
};

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
        char *error = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error);
        if (rc != SQLITE_OK) {
            std::string message = error ? error : sqlite3_errmsg(db_);
            sqlite3_free(error);
            throw SqliteError(message);
        }
    }

    int execute_changes(const std::string &sql) {
        execute(sql);
        return sqlite3_changes(db_);
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

py::dict sqlite_interval_bulk_upsert(
    const std::string &database_path,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const py::list &rows,
    std::int64_t live_lo,
    std::int64_t live_hi,
    std::int64_t writer_segment_id
) {
    if (columns.empty()) {
        throw std::invalid_argument("columns must not be empty");
    }
    if (pk_columns.empty()) {
        throw std::invalid_argument("pk_columns must not be empty");
    }

    SQLiteConnector conn(database_path);
    std::int64_t selected = 0;
    std::int64_t deleted_rows = 0;
    std::int64_t inserted = 0;
    const std::string input_table = temp_name("input");
    const std::string overlap_table = temp_name("overlap");
    const std::string quoted_table = quote_ident(physical_name);
    const std::string input_quoted = quote_ident(input_table);
    const std::string overlap_quoted = quote_ident(overlap_table);
    try {
        std::string data_cols = comma_join_quoted(columns);
        std::string all_insert_cols = data_cols + ", \"live_lo\", \"live_hi\", \"writer_segment_id\", \"deleted\"";
        std::string input_cols = data_cols;
        std::string physical_input_join = pk_join_sql(pk_columns, "p", "i");
        std::string overlap_input_join = pk_join_sql(pk_columns, "o", "i");

        std::string create_input = "CREATE TEMP TABLE " + input_quoted + " (";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (i) {
                create_input += ", ";
            }
            create_input += quote_ident(columns[i]);
        }
        create_input += ")";

        conn.execute("BEGIN IMMEDIATE");
        conn.execute(create_input);

        SqliteStatement input_insert(
            conn.raw(),
            "INSERT INTO " + input_quoted + " (" + input_cols + ") VALUES (" + placeholders(columns.size()) + ")"
        );
        for (const auto item : rows) {
            py::dict row = py::reinterpret_borrow<py::dict>(item);
            input_insert.reset();
            int bind_index = 1;
            for (const auto &column : columns) {
                py::object key = py::str(column);
                if (row.contains(key)) {
                    input_insert.bind(bind_index++, py_to_value(row[key]));
                } else {
                    input_insert.bind(bind_index++, std::monostate{});
                }
            }
            input_insert.step_done();
        }
        for (const auto &column : pk_columns) {
            py::object key = py::str(column);
            (void)key;
        }
        if (!pk_columns.empty()) {
            conn.execute("CREATE INDEX " + quote_ident(input_table + "_pk_idx") + " ON " + input_quoted + " (" +
                         comma_join_quoted(pk_columns) + ")");
        }

        conn.execute(
            "CREATE TEMP TABLE " + overlap_quoted + " AS "
            "SELECT p.rowid AS __chronos_rowid, p.* FROM " + quoted_table + " p "
            "JOIN " + input_quoted + " i ON " + physical_input_join + " "
            "WHERE p.live_lo < " + std::to_string(live_hi) + " AND " + std::to_string(live_lo) + " < p.live_hi"
        );
        selected = scalar_count(conn, "SELECT COUNT(*) FROM " + overlap_quoted);

        if (selected > 0) {
            deleted_rows = conn.execute_changes(
                "DELETE FROM " + quoted_table + " WHERE rowid IN (SELECT __chronos_rowid FROM " + overlap_quoted + ")"
            );

            inserted += conn.execute_changes(
                "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") "
                "SELECT " + comma_join_quoted(columns, "o") + ", "
                "o.live_lo, " + std::to_string(live_lo) + ", o.writer_segment_id, o.deleted "
                "FROM " + overlap_quoted + " o WHERE o.live_lo < " + std::to_string(live_lo)
            );

            inserted += conn.execute_changes(
                "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") "
                "SELECT " + comma_join_quoted(columns, "i") + ", "
                "CASE WHEN o.live_lo > " + std::to_string(live_lo) + " THEN o.live_lo ELSE " + std::to_string(live_lo) + " END, "
                "CASE WHEN o.live_hi < " + std::to_string(live_hi) + " THEN o.live_hi ELSE " + std::to_string(live_hi) + " END, "
                + std::to_string(writer_segment_id) + ", 0 "
                "FROM " + overlap_quoted + " o JOIN " + input_quoted + " i ON " + overlap_input_join
            );

            inserted += conn.execute_changes(
                "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") "
                "SELECT " + comma_join_quoted(columns, "o") + ", "
                + std::to_string(live_hi) + ", o.live_hi, o.writer_segment_id, o.deleted "
                "FROM " + overlap_quoted + " o WHERE " + std::to_string(live_hi) + " < o.live_hi"
            );
        }

        inserted += conn.execute_changes(
            "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") "
            "SELECT " + comma_join_quoted(columns, "i") + ", "
            + std::to_string(live_lo) + ", " + std::to_string(live_hi) + ", "
            + std::to_string(writer_segment_id) + ", 0 "
            "FROM " + input_quoted + " i WHERE NOT EXISTS ("
            "SELECT 1 FROM " + overlap_quoted + " o WHERE " + overlap_input_join + ")"
        );

        conn.execute("DROP TABLE " + overlap_quoted);
        conn.execute("DROP TABLE " + input_quoted);
        conn.execute("COMMIT");
    } catch (...) {
        try {
            conn.execute("ROLLBACK");
        } catch (...) {
        }
        try {
            conn.execute("DROP TABLE IF EXISTS " + overlap_quoted);
            conn.execute("DROP TABLE IF EXISTS " + input_quoted);
        } catch (...) {
        }
        throw;
    }

    py::dict stats;
    stats["input_rows"] = py::len(rows);
    stats["selected_rows"] = selected;
    stats["deleted_rows"] = deleted_rows;
    stats["inserted_rows"] = inserted;
    stats["strategy"] = "set_oriented_temp_tables";
    return stats;
}

py::dict recommended_sql_parser() {
    py::dict result;
    result["name"] = "libpg_query";
    result["scope"] = "PostgreSQL-compatible parsing and AST extraction";
    result["rationale"] = "Chronos native hot paths generate fixed SQL; when a native SQL parser is needed, libpg_query tracks PostgreSQL grammar more closely than generic C++ parsers.";
    result["not_for_hot_path"] = true;
    return result;
}

} // namespace

PYBIND11_MODULE(_native_interval, m) {
    m.doc() = "Native Chronos interval data-plane primitives";
    m.def("recommended_sql_parser", &recommended_sql_parser);
    m.def("sqlite3_libversion", []() { return std::string(sqlite3_libversion()); });
    m.def("libpq_version", []() { return PQlibVersion(); });
    m.def("sqlite_interval_bulk_upsert", &sqlite_interval_bulk_upsert, py::arg("database_path"),
          py::arg("physical_name"), py::arg("columns"), py::arg("pk_columns"), py::arg("rows"),
          py::arg("live_lo"), py::arg("live_hi"), py::arg("writer_segment_id"));

    py::class_<SQLiteConnector>(m, "SQLiteConnector")
        .def(py::init<const std::string &>())
        .def("execute", &SQLiteConnector::execute)
        .def("query", &SQLiteConnector::query);

    py::class_<PostgresConnector>(m, "PostgresConnector")
        .def(py::init<const std::string &>())
        .def("execute", &PostgresConnector::execute)
        .def("server_version", &PostgresConnector::server_version);
}
