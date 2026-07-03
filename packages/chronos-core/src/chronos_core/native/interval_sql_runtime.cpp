// Native SQL/storage runtime for the interval backend.
//
// This file is included into interval_data_plane.cpp as part of the single
// native translation unit. It groups low-level database support, shared
// interval splice mechanics, store-specific splice adapters, SQL drivers,
// and the native DML/SELECT planner in dependency order.

// -----------------------------------------------------------------------------
// SQLite support and prepared statement helpers
// Source: interval_sqlite_support.cpp
// -----------------------------------------------------------------------------
class SqliteError : public std::runtime_error {
  public:
    explicit SqliteError(sqlite3 *db) : std::runtime_error(sqlite3_errmsg(db)) {}
    explicit SqliteError(const std::string &message) : std::runtime_error(message) {}
};

enum class SqlProfileBucket {
    General,
    BulkSelect,
    BulkWrite,
    BulkTransaction,
};

enum class SqlProfileCall {
    DirectExecParams,
    PreparedExec,
    Prepare,
    SimpleExec,
};

thread_local chronos::native::NativeSqlProfile tls_sql_profile;
thread_local SqlProfileBucket tls_sql_profile_bucket = SqlProfileBucket::General;
thread_local bool tls_sql_profile_enabled = false;
thread_local std::vector<chronos::native::NativeSqlTraceEntry> tls_sql_trace;
thread_local bool tls_sql_trace_enabled = false;

std::string sql_profile_statement_keyword(const std::string &sql) {
    std::size_t pos = 0;
    while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) ++pos;
    std::string keyword;
    while (pos < sql.size() && std::isalpha(static_cast<unsigned char>(sql[pos]))) {
        keyword.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(sql[pos]))));
        ++pos;
    }
    return keyword;
}

bool sql_profile_is_query(const std::string &sql) {
    const std::string keyword = sql_profile_statement_keyword(sql);
    return keyword == "SELECT" || keyword == "WITH" || keyword == "EXPLAIN";
}

void record_sql_profile_bucket(chronos::native::NativeSqlProfile &profile) {
    switch (tls_sql_profile_bucket) {
    case SqlProfileBucket::BulkSelect:
        ++profile.bulk_select_round_trips;
        break;
    case SqlProfileBucket::BulkWrite:
        ++profile.bulk_write_round_trips;
        break;
    case SqlProfileBucket::BulkTransaction:
        ++profile.bulk_tx_round_trips;
        break;
    case SqlProfileBucket::General:
        break;
    }
}

void record_sql_profile_statement(const std::string &sql, SqlProfileCall call) {
    if (!tls_sql_profile_enabled) return;
    auto &profile = tls_sql_profile;
    ++profile.round_trips;
    if (sql_profile_is_query(sql)) ++profile.queries;
    else ++profile.executes;

    switch (call) {
    case SqlProfileCall::DirectExecParams:
        ++profile.direct_exec_params;
        break;
    case SqlProfileCall::PreparedExec:
        ++profile.prepared_execs;
        break;
    case SqlProfileCall::Prepare:
        ++profile.prepares;
        break;
    case SqlProfileCall::SimpleExec:
        ++profile.simple_execs;
        break;
    }
    record_sql_profile_bucket(profile);
}

void record_sql_profile_pipeline(std::int64_t statements, std::int64_t queries, std::int64_t executes) {
    if (!tls_sql_profile_enabled) return;
    auto &profile = tls_sql_profile;
    ++profile.round_trips;
    ++profile.pipeline_round_trips;
    profile.pipeline_statements += statements;
    profile.queries += queries;
    profile.executes += executes;
    record_sql_profile_bucket(profile);
}

void record_sql_profile_pipeline(std::int64_t statements) {
    record_sql_profile_pipeline(statements, statements, 0);
}

const char *sql_profile_call_name(SqlProfileCall call) {
    switch (call) {
    case SqlProfileCall::DirectExecParams:
        return "direct_exec_params";
    case SqlProfileCall::PreparedExec:
        return "prepared_exec";
    case SqlProfileCall::Prepare:
        return "prepare";
    case SqlProfileCall::SimpleExec:
        return "simple_exec";
    }
    return "unknown";
}

const char *sql_profile_bucket_name(SqlProfileBucket bucket) {
    switch (bucket) {
    case SqlProfileBucket::BulkSelect:
        return "bulk_select";
    case SqlProfileBucket::BulkWrite:
        return "bulk_write";
    case SqlProfileBucket::BulkTransaction:
        return "bulk_transaction";
    case SqlProfileBucket::General:
        return "general";
    }
    return "unknown";
}

double elapsed_ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

std::int64_t pg_result_rows(PGresult *result) {
    if (!result) return -1;
    const ExecStatusType status = PQresultStatus(result);
    if (status == PGRES_TUPLES_OK) return PQntuples(result);
    const char *tuples = PQcmdTuples(result);
    if (tuples && *tuples) return std::strtoll(tuples, nullptr, 10);
    return 0;
}

void record_sql_trace_statement(
    const std::string &sql,
    const std::string &call,
    SqlProfileBucket bucket,
    double elapsed_ms,
    std::int64_t rows,
    std::int64_t param_count
) {
    if (!tls_sql_trace_enabled) return;
    tls_sql_trace.push_back(chronos::native::NativeSqlTraceEntry{
        sql,
        call,
        sql_profile_bucket_name(bucket),
        elapsed_ms,
        rows,
        param_count,
    });
}

void record_sql_trace_statement(
    const std::string &sql,
    SqlProfileCall call,
    double elapsed_ms,
    std::int64_t rows,
    std::int64_t param_count
) {
    record_sql_trace_statement(
        sql,
        sql_profile_call_name(call),
        tls_sql_profile_bucket,
        elapsed_ms,
        rows,
        param_count
    );
}

class SqlProfileScope {
  public:
    explicit SqlProfileScope(SqlProfileBucket bucket) : previous_(tls_sql_profile_bucket) {
        tls_sql_profile_bucket = bucket;
    }
    ~SqlProfileScope() {
        tls_sql_profile_bucket = previous_;
    }

  private:
    SqlProfileBucket previous_;
};

void reset_sql_profile_impl() {
    tls_sql_profile = {};
}

chronos::native::NativeSqlProfile snapshot_sql_profile_impl() {
    return tls_sql_profile;
}

void set_sql_profile_enabled_impl(bool enabled) {
    tls_sql_profile_enabled = enabled;
}

void reset_sql_trace_impl() {
    tls_sql_trace.clear();
}

std::vector<chronos::native::NativeSqlTraceEntry> snapshot_sql_trace_impl() {
    return tls_sql_trace;
}

void set_sql_trace_enabled_impl(bool enabled) {
    tls_sql_trace_enabled = enabled;
}

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
        } else if (std::holds_alternative<DecimalValue>(value) ||
                   std::holds_alternative<DateValue>(value)) {
            const std::string text = value_text(value);
            rc = sqlite3_bind_text(stmt_, index, text.c_str(), static_cast<int>(text.size()), SQLITE_TRANSIENT);
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
            sqlite3_reset(stmt_);
            sqlite3_clear_bindings(stmt_);
            return false;
        }
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
        throw SqliteError(db_);
    }

    void step_done() {
        int rc = sqlite3_step(stmt_);
        if (rc != SQLITE_DONE) {
            sqlite3_reset(stmt_);
            sqlite3_clear_bindings(stmt_);
            throw SqliteError(db_);
        }
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
    }

  private:
    sqlite3 *db_;
    sqlite3_stmt *stmt_;
};

class SQLiteStatementCache {
  public:
    SqliteStatement &statement(sqlite3 *db, const std::string &sql) {
        auto found = statements_.find(sql);
        if (found != statements_.end()) {
            return *found->second;
        }
        if (statements_.size() >= 512) {
            statements_.clear();
        }
        auto inserted = statements_.emplace(sql, std::make_unique<SqliteStatement>(db, sql));
        return *inserted.first->second;
    }

  private:
    std::unordered_map<std::string, std::unique_ptr<SqliteStatement>> statements_;
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
        return chronos::native::detail::execute_changes(db_, sql);
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

// -----------------------------------------------------------------------------
// PostgreSQL libpq support and prepared statement helpers
// Source: interval_postgres_support.cpp
// -----------------------------------------------------------------------------
class PgError : public std::runtime_error {
  public:
    explicit PgError(const std::string &message) : std::runtime_error(message) {}
};

void chronos_pg_ignore_notice(void *, const char *) {}

class PgResult {
  public:
    PgResult(PGconn *conn, PGresult *result) : conn_(conn), result_(result) {
        if (!result_) {
            throw PgError(PQerrorMessage(conn_));
        }
    }

    ~PgResult() {
        if (result_) {
            PQclear(result_);
        }
    }

    PgResult(const PgResult &) = delete;
    PgResult &operator=(const PgResult &) = delete;
    PgResult(PgResult &&other) noexcept : conn_(other.conn_), result_(other.result_) {
        other.conn_ = nullptr;
        other.result_ = nullptr;
    }
    PgResult &operator=(PgResult &&other) noexcept {
        if (this != &other) {
            if (result_) {
                PQclear(result_);
            }
            conn_ = other.conn_;
            result_ = other.result_;
            other.conn_ = nullptr;
            other.result_ = nullptr;
        }
        return *this;
    }

    PGresult *get() { return result_; }

    void require(ExecStatusType expected) {
        ExecStatusType status = PQresultStatus(result_);
        if (status != expected) {
            throw PgError(PQerrorMessage(conn_));
        }
    }

    void require_query_or_command() {
        ExecStatusType status = PQresultStatus(result_);
        if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
            throw PgError(PQerrorMessage(conn_));
        }
    }

  private:
    PGconn *conn_;
    PGresult *result_;
};

PGconn *postgres_conn_from_python_connection(const py::handle &connection) {
    py::object pgconn = py::getattr(connection, "pgconn");
    auto ptr = py::getattr(pgconn, "pgconn_ptr").cast<std::uintptr_t>();
    if (ptr == 0) {
        throw PgError("psycopg PGconn pointer is null");
    }
    auto *conn = reinterpret_cast<PGconn *>(ptr);
    if (PQstatus(conn) != CONNECTION_OK) {
        throw PgError(PQerrorMessage(conn));
    }
    return conn;
}

std::string pg_placeholder_sql(const std::string &sql) {
    std::string out;
    out.reserve(sql.size() + 16);
    bool single_quote = false;
    bool double_quote = false;
    int index = 1;
    for (std::size_t i = 0; i < sql.size(); ++i) {
        const char ch = sql[i];
        if (ch == '\'' && !double_quote) {
            single_quote = !single_quote;
            out.push_back(ch);
            continue;
        }
        if (ch == '"' && !single_quote) {
            double_quote = !double_quote;
            out.push_back(ch);
            continue;
        }
        if (!single_quote && !double_quote && ch == '?') {
            out.push_back('$');
            out += std::to_string(index++);
            continue;
        }
        out.push_back(ch);
    }
    return out;
}

std::string pg_placeholders(std::size_t count, int start = 1) {
    std::string out;
    for (std::size_t i = 0; i < count; ++i) {
        if (i) {
            out += ", ";
        }
        out.push_back('$');
        out += std::to_string(start + static_cast<int>(i));
    }
    return out;
}

std::string value_to_pg_text(const Value &value) {
    if (std::holds_alternative<std::monostate>(value)) {
        return {};
    }
    if (auto ptr = std::get_if<std::int64_t>(&value)) {
        return std::to_string(*ptr);
    }
    if (auto ptr = std::get_if<double>(&value)) {
        std::ostringstream out;
        out.precision(17);
        out << *ptr;
        return out.str();
    }
    if (auto ptr = std::get_if<std::string>(&value)) {
        return *ptr;
    }
    if (std::holds_alternative<DecimalValue>(value) ||
        std::holds_alternative<DateValue>(value)) {
        return value_text(value);
    }
    const auto &blob = std::get<Blob>(value);
    static constexpr char hex[] = "0123456789abcdef";
    std::string out;
    out.reserve(2 + blob.size() * 2);
    out += "\\x";
    for (unsigned char byte : blob) {
        out.push_back(hex[byte >> 4]);
        out.push_back(hex[byte & 0x0f]);
    }
    return out;
}

struct PgParams {
    std::vector<std::string> storage;
    std::vector<const char *> values;
    std::vector<int> lengths;
    std::vector<int> formats;
};

PgParams make_pg_params(const std::vector<Value> &params) {
    PgParams bound;
    bound.storage.reserve(params.size());
    bound.values.reserve(params.size());
    bound.lengths.reserve(params.size());
    bound.formats.reserve(params.size());
    for (const auto &value : params) {
        if (std::holds_alternative<std::monostate>(value)) {
            bound.storage.emplace_back();
            bound.values.push_back(nullptr);
            bound.lengths.push_back(0);
            bound.formats.push_back(0);
            continue;
        }
        bound.storage.push_back(value_to_pg_text(value));
        bound.values.push_back(bound.storage.back().c_str());
        bound.lengths.push_back(static_cast<int>(bound.storage.back().size()));
        bound.formats.push_back(0);
    }
    return bound;
}

PgResult pg_exec_params(PGconn *conn, const std::string &sql, const std::vector<Value> &params) {
    record_sql_profile_statement(sql, SqlProfileCall::DirectExecParams);
    PgParams bound = make_pg_params(params);
    const auto start = std::chrono::steady_clock::now();
    PGresult *result = PQexecParams(
        conn,
        sql.c_str(),
        static_cast<int>(params.size()),
        nullptr,
        bound.values.empty() ? nullptr : bound.values.data(),
        bound.lengths.empty() ? nullptr : bound.lengths.data(),
        bound.formats.empty() ? nullptr : bound.formats.data(),
        0
    );
    record_sql_trace_statement(
        sql,
        SqlProfileCall::DirectExecParams,
        elapsed_ms_since(start),
        pg_result_rows(result),
        static_cast<std::int64_t>(params.size())
    );
    return PgResult(conn, result);
}

std::string pg_statement_keyword(const std::string &sql) {
    std::size_t pos = 0;
    while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) ++pos;
    std::string keyword;
    while (pos < sql.size() && std::isalpha(static_cast<unsigned char>(sql[pos]))) {
        keyword.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(sql[pos]))));
        ++pos;
    }
    return keyword;
}

bool pg_can_prepare_statement(const std::string &sql) {
    const std::string keyword = pg_statement_keyword(sql);
    return keyword == "SELECT" || keyword == "WITH" || keyword == "INSERT" ||
           keyword == "UPDATE" || keyword == "DELETE";
}

class PgPreparedStatementCache {
  public:
    PgPreparedStatementCache()
        : cache_id_(global_cache_id_.fetch_add(1, std::memory_order_relaxed)) {}

    PgResult exec(PGconn *conn, const std::string &sql, const std::vector<Value> &params) {
        auto found = statements_.find(sql);
        if (found == statements_.end()) {
            if (statements_.size() >= 512) {
                statements_.clear();
            }
            Prepared prepared;
            prepared.name = "__chronos_native_stmt_" + std::to_string(cache_id_) +
                "_" + std::to_string(next_id_++);
            prepared.param_count = params.size();
            record_sql_profile_statement(sql, SqlProfileCall::Prepare);
            const auto prepare_start = std::chrono::steady_clock::now();
            PGresult *prepare_raw = PQprepare(conn, prepared.name.c_str(), sql.c_str(), static_cast<int>(prepared.param_count), nullptr);
            record_sql_trace_statement(
                sql,
                SqlProfileCall::Prepare,
                elapsed_ms_since(prepare_start),
                pg_result_rows(prepare_raw),
                static_cast<std::int64_t>(prepared.param_count)
            );
            PgResult prepared_result(conn, prepare_raw);
            prepared_result.require(PGRES_COMMAND_OK);
            found = statements_.emplace(sql, std::move(prepared)).first;
        } else if (found->second.param_count != params.size()) {
            throw PgError("cached PostgreSQL statement parameter count changed");
        }

        PgParams bound = make_pg_params(params);
        record_sql_profile_statement(sql, SqlProfileCall::PreparedExec);
        const auto exec_start = std::chrono::steady_clock::now();
        PGresult *result = PQexecPrepared(
            conn,
            found->second.name.c_str(),
            static_cast<int>(params.size()),
            bound.values.empty() ? nullptr : bound.values.data(),
            bound.lengths.empty() ? nullptr : bound.lengths.data(),
            bound.formats.empty() ? nullptr : bound.formats.data(),
            0
        );
        record_sql_trace_statement(
            sql,
            SqlProfileCall::PreparedExec,
            elapsed_ms_since(exec_start),
            pg_result_rows(result),
            static_cast<std::int64_t>(params.size())
        );
        return PgResult(conn, result);
    }

  private:
    struct Prepared {
        std::string name;
        std::size_t param_count = 0;
    };

    std::unordered_map<std::string, Prepared> statements_;
    inline static std::atomic<std::uint64_t> global_cache_id_{1};
    std::uint64_t cache_id_ = 0;
    std::size_t next_id_ = 1;
};

Value pg_column_value(PGresult *result, int row, int column) {
    if (PQgetisnull(result, row, column)) {
        return std::monostate{};
    }
    const Oid oid = PQftype(result, column);
    const char *raw = PQgetvalue(result, row, column);
    const int len = PQgetlength(result, row, column);
    std::string text(raw, len);
    switch (oid) {
    case 16:
        return text == "t" ? std::int64_t{1} : std::int64_t{0};
    case 20:
    case 21:
    case 23:
        return static_cast<std::int64_t>(std::stoll(text));
    case 700:
    case 701:
    case 1700:
        return std::stod(text);
    case 17: {
        Blob blob;
        if (text.rfind("\\x", 0) == 0) {
            blob.reserve((text.size() - 2) / 2);
            for (std::size_t i = 2; i + 1 < text.size(); i += 2) {
                unsigned int byte = 0;
                std::stringstream ss;
                ss << std::hex << text.substr(i, 2);
                ss >> byte;
                blob.push_back(static_cast<unsigned char>(byte));
            }
        } else {
            blob.assign(text.begin(), text.end());
        }
        return blob;
    }
    case 114:
    case 3802:
        return text;
    default:
        return text;
    }
}

py::list rows_from_pg_result(PGresult *result) {
    py::list rows;
    const int row_count = PQntuples(result);
    const int column_count = PQnfields(result);
    for (int r = 0; r < row_count; ++r) {
        py::dict row;
        for (int c = 0; c < column_count; ++c) {
            row[PQfname(result, c)] = value_to_py(pg_column_value(result, r, c));
        }
        rows.append(row);
    }
    return rows;
}

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

// -----------------------------------------------------------------------------
// Shared interval splice primitives
// Source: interval_splice_core.cpp
// -----------------------------------------------------------------------------
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

std::string normalize_decimal(std::string value) {
    if (value.empty()) {
        return "0";
    }
    const std::size_t dot = value.find('.');
    if (dot != std::string::npos) {
        value.resize(dot);
    }
    std::size_t pos = 0;
    while (pos + 1 < value.size() && value[pos] == '0') {
        ++pos;
    }
    value.erase(0, pos);
    return value.empty() ? "0" : value;
}

int compare_decimal(const std::string &left, const std::string &right) {
    const std::string a = normalize_decimal(left);
    const std::string b = normalize_decimal(right);
    if (a.size() != b.size()) {
        return a.size() < b.size() ? -1 : 1;
    }
    if (a == b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

std::string decimal_min(const std::string &left, const std::string &right) {
    return compare_decimal(left, right) <= 0 ? normalize_decimal(left) : normalize_decimal(right);
}

std::string decimal_max(const std::string &left, const std::string &right) {
    return compare_decimal(left, right) >= 0 ? normalize_decimal(left) : normalize_decimal(right);
}

std::string decimal_add_small(const std::string &value, std::int64_t delta) {
    if (delta < 0) {
        throw std::invalid_argument("decimal_add_small only accepts non-negative deltas");
    }
    std::string out = normalize_decimal(value);
    while (delta > 0) {
        int carry = 1;
        for (std::size_t i = out.size(); i > 0 && carry; --i) {
            int digit = (out[i - 1] - '0') + carry;
            out[i - 1] = static_cast<char>('0' + (digit % 10));
            carry = digit / 10;
        }
        if (carry) out.insert(out.begin(), '1');
        --delta;
    }
    return normalize_decimal(out);
}

std::string decimal_sub_small(const std::string &value, std::int64_t delta) {
    if (delta < 0) {
        throw std::invalid_argument("decimal_sub_small only accepts non-negative deltas");
    }
    std::string out = normalize_decimal(value);
    while (delta > 0) {
        if (compare_decimal(out, "0") <= 0) {
            throw std::runtime_error("decimal underflow");
        }
        int borrow = 1;
        for (std::size_t i = out.size(); i > 0 && borrow; --i) {
            int digit = (out[i - 1] - '0') - borrow;
            if (digit < 0) {
                out[i - 1] = '9';
                borrow = 1;
            } else {
                out[i - 1] = static_cast<char>('0' + digit);
                borrow = 0;
            }
        }
        --delta;
    }
    return normalize_decimal(out);
}

bool decimal_gap_at_least(const std::string &hi, const std::string &lo, std::int64_t gap) {
    return compare_decimal(hi, decimal_add_small(lo, gap)) >= 0;
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

struct NativeKeyPrefixFilter {
    std::vector<std::string> columns;
    std::vector<Value> values;
};

std::string pg_key_where_sql(const std::vector<std::string> &pk_columns, int start = 1) {
    std::string key_where;
    for (std::size_t i = 0; i < pk_columns.size(); ++i) {
        if (i) {
            key_where += " AND ";
        }
        key_where += quote_ident(pk_columns[i]) + " = $" + std::to_string(start + static_cast<int>(i));
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

struct Int64IntervalBounds {
    static std::int64_t min(const std::int64_t &left, const std::int64_t &right) {
        return std::min(left, right);
    }
    static std::int64_t max(const std::int64_t &left, const std::int64_t &right) {
        return std::max(left, right);
    }
    static bool less(const std::int64_t &left, const std::int64_t &right) {
        return left < right;
    }
};

struct DecimalIntervalBounds {
    static std::string min(const std::string &left, const std::string &right) {
        return decimal_min(left, right);
    }
    static std::string max(const std::string &left, const std::string &right) {
        return decimal_max(left, right);
    }
    static bool less(const std::string &left, const std::string &right) {
        return compare_decimal(left, right) < 0;
    }
};

// Shared physical interval copy-on-write algorithm. SQL engines only provide
// adapter operations for locating existing physical rows, narrowing an old row
// in place, replacing an old row whose live_lo is already the new start, and
// inserting new middle/right fragments. The visibility math stays here so
// SQLite/Postgres/DuckDB cannot drift apart.
//
// For old [old_lo, old_hi) and write [write_lo, write_hi), the split is:
//   1. UPDATE old live_hi to overlap_lo when a left fragment exists.
//   2. INSERT the replacement middle fragment, or UPDATE old in place when the
//      replacement starts at old live_lo.
//   3. INSERT the old right fragment when one exists.
//
// The physical key includes live_lo. Adapters may update live_hi and payload
// columns in place, but must never move live_lo.
template <
    typename PhysicalRow,
    typename Bound,
    typename Bounds,
    typename InsertRow,
    typename ShrinkLeftRow,
    typename ReplaceRow
>
void splice_interval_rows(
    BulkUpsertStats &stats,
    const std::vector<Value> &replacement_row,
    const std::vector<PhysicalRow> &physical_rows,
    const Bound &live_lo,
    const Bound &live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    InsertRow insert_row,
    ShrinkLeftRow shrink_left_row,
    ReplaceRow replace_row
) {
    stats.selected += static_cast<std::int64_t>(physical_rows.size());
    if (physical_rows.empty()) {
        // Pure insert: no visible physical row for this key overlaps the target
        // branch interval, so the replacement owns the whole interval.
        insert_row(replacement_row, live_lo, live_hi, writer_segment_id, replacement_deleted ? 1 : 0);
        return;
    }

    for (const auto &physical : physical_rows) {
        const Bound overlap_lo = Bounds::max(physical.live_lo, live_lo);
        const Bound overlap_hi = Bounds::min(physical.live_hi, live_hi);
        const bool has_left = Bounds::less(physical.live_lo, overlap_lo);
        const bool has_right = Bounds::less(overlap_hi, physical.live_hi);
        const std::vector<Value> &replacement_values =
            replacement_deleted ? physical.values : replacement_row;

        // Replace only the overlapping slice.  Left/right fragments preserve
        // the old row for readers outside the current branch interval, while
        // the middle fragment becomes either the replacement row or a tombstone.
        ++stats.deleted_rows;

        if (has_left) {
            shrink_left_row(physical, overlap_lo);
            // Preserve the historical stats contract: this left fragment is an
            // output physical interval even if the adapter implemented it as an
            // UPDATE instead of a literal INSERT.
            ++stats.inserted;
            insert_row(
                replacement_values,
                overlap_lo,
                overlap_hi,
                writer_segment_id,
                replacement_deleted ? 1 : 0
            );
        } else {
            replace_row(
                physical,
                replacement_values,
                overlap_hi,
                writer_segment_id,
                replacement_deleted ? 1 : 0
            );
            ++stats.inserted;
        }

        if (has_right) {
            insert_row(
                physical.values,
                overlap_hi,
                physical.live_hi,
                physical.writer_segment_id,
                physical.deleted
            );
        }
    }
}

template <typename PhysicalRow, typename Bound, typename Bounds>
bool physical_rows_contain_visible_live_conflict(
    const std::vector<PhysicalRow> &physical_rows,
    const Bound &branch_point
) {
    for (const auto &physical : physical_rows) {
        if (physical.deleted) continue;
        const bool starts_at_or_before_branch = !Bounds::less(branch_point, physical.live_lo);
        const bool ends_after_branch = Bounds::less(branch_point, physical.live_hi);
        if (starts_at_or_before_branch && ends_after_branch) return true;
    }
    return false;
}

bool insert_mode_should_skip_conflict(IntervalWriteMode write_mode) {
    return write_mode == IntervalWriteMode::InsertIgnoreConflicts;
}

bool insert_mode_checks_conflicts(IntervalWriteMode write_mode) {
    return write_mode == IntervalWriteMode::Insert ||
        write_mode == IntervalWriteMode::InsertIgnoreConflicts;
}

void raise_insert_duplicate_key() {
    throw std::runtime_error("duplicate key value violates unique constraint");
}

// -----------------------------------------------------------------------------
// Native value conversion and URL helpers
// Source: interval_value_utils.cpp
// -----------------------------------------------------------------------------
py::dict stats_to_py_dict(const BulkUpsertStats &native_stats, std::size_t input_rows, const std::string &strategy) {
    py::dict result;
    result["input_rows"] = input_rows;
    result["selected_rows"] = native_stats.selected;
    result["deleted_rows"] = native_stats.deleted_rows;
    result["inserted_rows"] = native_stats.inserted;
    result["strategy"] = strategy;
    return result;
}

std::string sqlite_path_from_url(const std::string &database_url) {
    const std::string prefix = "sqlite:///";
    if (database_url.rfind(prefix, 0) == 0) return "/" + database_url.substr(prefix.size());
    if (database_url.rfind("sqlite://", 0) == 0) return database_url.substr(std::string("sqlite://").size());
    return database_url;
}

std::optional<std::string> native_sqlite_synchronous_from_env() {
    const char *raw = std::getenv("CHRONOS_NATIVE_SQLITE_SYNCHRONOUS");
    if (!raw || !*raw) return std::nullopt;
    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    if (value == "OFF" || value == "NORMAL" || value == "FULL" || value == "EXTRA") {
        return value;
    }
    throw std::invalid_argument("invalid CHRONOS_NATIVE_SQLITE_SYNCHRONOUS: " + value);
}

std::optional<std::int64_t> native_sqlite_cache_size_kib_from_env() {
    const char *raw = std::getenv("CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB");
    if (!raw || !*raw) return std::nullopt;
    std::string value(raw);
    std::int64_t cache_size = 0;
    try {
        cache_size = std::stoll(value);
    } catch (const std::exception &) {
        throw std::invalid_argument("invalid CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB: " + value);
    }
    if (cache_size <= 0) {
        throw std::invalid_argument("CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB must be positive");
    }
    return cache_size;
}

std::optional<std::int64_t> native_sqlite_wal_autocheckpoint_pages_from_env() {
    const char *raw = std::getenv("CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES");
    if (!raw || !*raw) return std::nullopt;
    std::string value(raw);
    std::int64_t pages = 0;
    try {
        pages = std::stoll(value);
    } catch (const std::exception &) {
        throw std::invalid_argument(
            "invalid CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES: " + value);
    }
    if (pages < 0) {
        throw std::invalid_argument(
            "CHRONOS_NATIVE_SQLITE_WAL_AUTOCHECKPOINT_PAGES must be non-negative");
    }
    return pages;
}

constexpr std::int64_t kDefaultNativeSqliteWalAutocheckpointPages = 16384;

std::optional<std::int64_t> native_sqlite_journal_size_limit_bytes_from_env() {
    const char *raw = std::getenv("CHRONOS_NATIVE_SQLITE_JOURNAL_SIZE_LIMIT_BYTES");
    if (!raw || !*raw) return std::nullopt;
    std::string value(raw);
    std::int64_t bytes = 0;
    try {
        bytes = std::stoll(value);
    } catch (const std::exception &) {
        throw std::invalid_argument(
            "invalid CHRONOS_NATIVE_SQLITE_JOURNAL_SIZE_LIMIT_BYTES: " + value);
    }
    if (bytes < -1) {
        throw std::invalid_argument(
            "CHRONOS_NATIVE_SQLITE_JOURNAL_SIZE_LIMIT_BYTES must be -1 or non-negative");
    }
    return bytes;
}

std::size_t native_sqlite_physical_write_batch_size_from_env() {
    const char *raw = std::getenv("CHRONOS_NATIVE_SQLITE_PHYSICAL_WRITE_BATCH_SIZE");
    if (!raw || !*raw) return 256;
    std::string value(raw);
    std::uint64_t batch_size = 0;
    try {
        batch_size = std::stoull(value);
    } catch (const std::exception &) {
        throw std::invalid_argument(
            "invalid CHRONOS_NATIVE_SQLITE_PHYSICAL_WRITE_BATCH_SIZE: " + value);
    }
    if (batch_size == 0) {
        throw std::invalid_argument(
            "CHRONOS_NATIVE_SQLITE_PHYSICAL_WRITE_BATCH_SIZE must be positive");
    }
    return static_cast<std::size_t>(std::min<std::uint64_t>(batch_size, 1024));
}

std::string duckdb_path_from_url(const std::string &database_url) {
    const std::string absolute_prefix = "duckdb:///";
    const std::string url_prefix = "duckdb://";
    const std::string scheme_prefix = "duckdb:";
    if (database_url.rfind(absolute_prefix, 0) == 0) {
        std::string path = database_url.substr(absolute_prefix.size());
        if (path == ":memory:") return path;
        while (!path.empty() && path.front() == '/') path.erase(path.begin());
        return "/" + path;
    }
    if (database_url.rfind(url_prefix, 0) == 0) {
        return database_url.substr(url_prefix.size());
    }
    if (database_url.rfind(scheme_prefix, 0) == 0) {
        return database_url.substr(scheme_prefix.size());
    }
    return database_url;
}

class NativeDuckDBError : public std::runtime_error {
  public:
    explicit NativeDuckDBError(const std::string &message) : std::runtime_error(message) {}
};

class DuckDBResult {
  public:
    DuckDBResult() = default;
    ~DuckDBResult() { duckdb_destroy_result(&result_); }
    DuckDBResult(const DuckDBResult &) = delete;
    DuckDBResult &operator=(const DuckDBResult &) = delete;
    duckdb_result *get() { return &result_; }

  private:
    duckdb_result result_{};
};

cpp_int duckdb_hugeint_to_cpp_int(const duckdb_hugeint &value) {
    cpp_int out = value.upper;
    out <<= 64;
    out += value.lower;
    return out;
}

std::string duckdb_decimal_to_text(const duckdb_decimal &value) {
    cpp_int integer = duckdb_hugeint_to_cpp_int(value.value);
    const bool negative = integer < 0;
    if (negative) integer = -integer;

    std::string digits = integer.convert_to<std::string>();
    if (value.scale == 0) {
        return negative ? "-" + digits : digits;
    }

    const std::size_t scale = static_cast<std::size_t>(value.scale);
    if (digits.size() <= scale) {
        digits.insert(0, scale + 1 - digits.size(), '0');
    }
    const std::size_t point = digits.size() - scale;
    digits.insert(point, ".");
    return negative ? "-" + digits : digits;
}

Value duckdb_column_value(duckdb_result *result, idx_t column, idx_t row) {
    if (duckdb_value_is_null(result, column, row)) return std::monostate{};
    switch (duckdb_column_type(result, column)) {
    case DUCKDB_TYPE_BOOLEAN:
        return static_cast<std::int64_t>(duckdb_value_boolean(result, column, row) ? 1 : 0);
    case DUCKDB_TYPE_TINYINT:
    case DUCKDB_TYPE_SMALLINT:
    case DUCKDB_TYPE_INTEGER:
    case DUCKDB_TYPE_BIGINT:
        return static_cast<std::int64_t>(duckdb_value_int64(result, column, row));
    case DUCKDB_TYPE_UTINYINT:
    case DUCKDB_TYPE_USMALLINT:
    case DUCKDB_TYPE_UINTEGER: {
        const auto value = duckdb_value_uint64(result, column, row);
        return static_cast<std::int64_t>(value);
    }
    case DUCKDB_TYPE_UBIGINT: {
        const auto value = duckdb_value_uint64(result, column, row);
        if (value <= static_cast<uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            return static_cast<std::int64_t>(value);
        }
        return std::to_string(value);
    }
    case DUCKDB_TYPE_HUGEINT: {
        const duckdb_hugeint value = duckdb_value_hugeint(result, column, row);
        if (value.upper == 0 &&
            value.lower <= static_cast<uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            return static_cast<std::int64_t>(value.lower);
        }
        if (value.upper == -1 &&
            value.lower >= (uint64_t{1} << 63)) {
            return static_cast<std::int64_t>(value.lower);
        }
        char *raw = duckdb_value_varchar(result, column, row);
        if (!raw) return std::monostate{};
        std::string text(raw);
        duckdb_free(raw);
        return text;
    }
    case DUCKDB_TYPE_UHUGEINT: {
        const duckdb_uhugeint value = duckdb_value_uhugeint(result, column, row);
        if (value.upper == 0 &&
            value.lower <= static_cast<uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            return static_cast<std::int64_t>(value.lower);
        }
        char *raw = duckdb_value_varchar(result, column, row);
        if (!raw) return std::monostate{};
        std::string text(raw);
        duckdb_free(raw);
        return text;
    }
    case DUCKDB_TYPE_FLOAT:
        return static_cast<double>(duckdb_value_float(result, column, row));
    case DUCKDB_TYPE_DOUBLE:
        return duckdb_value_double(result, column, row);
    case DUCKDB_TYPE_DATE: {
        const duckdb_date_struct value = duckdb_from_date(
            duckdb_value_date(result, column, row)
        );
        return DateValue{
            static_cast<int>(value.year),
            static_cast<int>(value.month),
            static_cast<int>(value.day),
        };
    }
    case DUCKDB_TYPE_DECIMAL:
        return DecimalValue{duckdb_decimal_to_text(
            duckdb_value_decimal(result, column, row)
        )};
    case DUCKDB_TYPE_BLOB: {
        duckdb_blob blob = duckdb_value_blob(result, column, row);
        Blob out;
        if (blob.data && blob.size > 0) {
            const auto *data = static_cast<const unsigned char *>(blob.data);
            out.assign(data, data + blob.size);
        }
        duckdb_free(blob.data);
        return out;
    }
    default: {
        char *raw = duckdb_value_varchar(result, column, row);
        if (!raw) return std::monostate{};
        std::string text(raw);
        duckdb_free(raw);
        return text;
    }
    }
}

std::int64_t native_as_int(const Value &value) {
    if (auto ptr = std::get_if<std::int64_t>(&value)) return *ptr;
    if (auto ptr = std::get_if<double>(&value)) return static_cast<std::int64_t>(*ptr);
    if (auto ptr = std::get_if<std::string>(&value)) {
        if (*ptr == "t" || *ptr == "true") return 1;
        if (*ptr == "f" || *ptr == "false") return 0;
        return ptr->empty() ? 0 : std::stoll(*ptr);
    }
    if (auto ptr = std::get_if<DecimalValue>(&value)) {
        return ptr->text.empty() ? 0 : std::stoll(ptr->text);
    }
    return 0;
}

std::string native_as_string(const Value &value) {
    if (auto ptr = std::get_if<std::string>(&value)) return *ptr;
    if (std::holds_alternative<DecimalValue>(value) ||
        std::holds_alternative<DateValue>(value)) {
        return value_text(value);
    }
    if (auto ptr = std::get_if<std::int64_t>(&value)) return std::to_string(*ptr);
    if (auto ptr = std::get_if<double>(&value)) {
        std::ostringstream out;
        out.precision(17);
        out << *ptr;
        return out.str();
    }
    return {};
}

cpp_int cpp_int_from_decimal(const std::string &value) {
    std::string normalized = normalize_decimal(value);
    cpp_int out = 0;
    for (char ch : normalized) {
        if (ch < '0' || ch > '9') continue;
        out *= 10;
        out += static_cast<int>(ch - '0');
    }
    return out;
}

std::string cpp_int_to_decimal(const cpp_int &value) {
    return value.convert_to<std::string>();
}

cpp_int cpp_int_isqrt(const cpp_int &value) {
    if (value <= 0) return 0;
    cpp_int x = value;
    cpp_int y = (x + 1) / 2;
    while (y < x) {
        x = y;
        y = (x + value / x) / 2;
    }
    return x;
}

Value pg_branch_value(PGresult *result, int row, int column) {
    if (PQgetisnull(result, row, column)) return std::monostate{};
    const Oid oid = PQftype(result, column);
    std::string text(PQgetvalue(result, row, column), PQgetlength(result, row, column));
    switch (oid) {
    case 16:
        return text == "t" ? Value(std::int64_t{1}) : Value(std::int64_t{0});
    case 20:
    case 21:
    case 23:
        return static_cast<std::int64_t>(std::stoll(text));
    case 700:
    case 701:
        return std::stod(text);
    case 17: {
        Blob blob;
        if (text.rfind("\\x", 0) == 0) {
            blob.reserve((text.size() - 2) / 2);
            for (std::size_t i = 2; i + 1 < text.size(); i += 2) {
                unsigned int byte = 0;
                std::stringstream ss;
                ss << std::hex << text.substr(i, 2);
                ss >> byte;
                blob.push_back(static_cast<unsigned char>(byte));
            }
        } else {
            blob.assign(text.begin(), text.end());
        }
        return blob;
    }
    case 114:
    case 3802:
        return text;
    case 1700:
    default:
        return text;
    }
}

BulkUpsertResult sqlite_adapter_bulk_upsert(
    sqlite3 *db,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const NativeRows &rows,
    std::int64_t live_lo,
    std::int64_t live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction,
    SQLiteStatementCache *statement_cache,
    IntervalWriteMode write_mode = IntervalWriteMode::Upsert,
    std::int64_t branch_point = 0
);

BulkUpsertResult postgres_adapter_bulk_upsert(
    PGconn *conn,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const NativeRows &rows,
    const std::string &live_lo,
    const std::string &live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction,
    PgPreparedStatementCache *statement_cache,
    IntervalWriteMode write_mode = IntervalWriteMode::Upsert,
    const std::string &branch_point = ""
);

// -----------------------------------------------------------------------------
// SQLite/PostgreSQL interval splice adapters
// Source: interval_splice_adapters.cpp
// -----------------------------------------------------------------------------
BulkUpsertResult sqlite_adapter_bulk_upsert(
    sqlite3 *db,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const NativeRows &rows,
    std::int64_t live_lo,
    std::int64_t live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction,
    SQLiteStatementCache *statement_cache,
    IntervalWriteMode write_mode,
    std::int64_t branch_point
) {
    if (columns.empty()) {
        throw std::invalid_argument("columns must not be empty");
    }
    if (pk_columns.empty()) {
        throw std::invalid_argument("pk_columns must not be empty");
    }

    BulkUpsertResult result;
    BulkUpsertStats &stats = result.stats;
    const std::string quoted_table = quote_ident(physical_name);
    const std::vector<std::size_t> pk_indices = pk_column_indices(columns, pk_columns);
    // This adapter owns SQLite mechanics only: batching, prepared statements,
    // rowid-based updates, and parameter binding. The shared splice algorithm
    // decides which interval fragments exist.
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
    const std::size_t physical_write_batch_size =
        native_sqlite_physical_write_batch_size_from_env();
    const std::size_t insert_value_count = columns.size() + 4;
    const std::size_t sqlite_variable_limit =
        static_cast<std::size_t>(std::max(1, sqlite3_limit(db, SQLITE_LIMIT_VARIABLE_NUMBER, -1)));
    const std::size_t max_update_batch =
        std::max<std::size_t>(1, std::min(physical_write_batch_size, sqlite_variable_limit));
    const std::size_t max_insert_batch =
        std::max<std::size_t>(
            1,
            std::min(
                physical_write_batch_size,
                sqlite_variable_limit / std::max<std::size_t>(1, insert_value_count)));

    struct PhysicalRow {
        std::int64_t rowid;
        std::vector<Value> values;
        std::int64_t live_lo;
        std::int64_t live_hi;
        std::int64_t writer_segment_id;
        std::int64_t deleted;
    };

    struct PendingInsert {
        // Points either at the replacement row or at a fetched physical row.
        // Both owners outlive the pending queue: replacement rows are held by
        // splice_rows/deduped_storage, and physical rows are held in the current
        // PendingSplice chunk until flush_inserts() runs.
        const std::vector<Value> *values;
        std::int64_t live_lo;
        std::int64_t live_hi;
        std::int64_t writer_segment_id;
        std::int64_t deleted;
    };

    struct PendingLiveHiUpdate {
        std::int64_t rowid;
        std::int64_t live_hi;
    };

    auto key_for_row = [&](const std::vector<Value> &row) {
        NativeRowKey key;
        key.values.reserve(pk_indices.size());
        for (std::size_t index : pk_indices) key.values.push_back(row[index]);
        return key;
    };

    auto key_batch_where = [&](std::size_t count) {
        std::string sql;
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            if (row_index) sql += " OR ";
            sql += "(";
            for (std::size_t pk_index = 0; pk_index < pk_columns.size(); ++pk_index) {
                if (pk_index) sql += " AND ";
                sql += quote_ident(pk_columns[pk_index]) + " = ?";
            }
            sql += ")";
        }
        return sql;
    };

    auto insert_batch_sql = [&](std::size_t count) {
        std::string sql =
            "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") VALUES ";
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            if (row_index) sql += ", ";
            sql += "(" + placeholders(insert_value_count) + ")";
        }
        return sql;
    };

    auto live_hi_update_batch_sql = [&](std::size_t count) {
        std::string sql = "UPDATE " + quoted_table + " SET \"live_hi\" = CASE rowid ";
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            sql += "WHEN ? THEN ? ";
        }
        sql += "END WHERE rowid IN (" + placeholders(count) + ")";
        return sql;
    };

    auto replacement_update_sql = [&] {
        std::string sql = "UPDATE " + quoted_table + " SET ";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (i) {
                sql += ", ";
            }
            sql += quote_ident(columns[i]) + " = ?";
        }
        sql += ", \"live_hi\" = ?, \"writer_segment_id\" = ?, \"deleted\" = ? WHERE rowid = ?";
        return sql;
    };

    try {
        if (manage_transaction) {
            // Standalone calls own the SQLite transaction.  Calls from
            // BranchSession.transaction() pass manage_transaction=false so this
            // native splice is atomic with metadata/inode updates in the
            // caller's already-open transaction.
            execute_sql(db, "BEGIN IMMEDIATE");
        }
        std::unique_ptr<SqliteStatement> local_select_stmt;
        SqliteStatement *select_stmt = nullptr;
        if (statement_cache) {
            select_stmt = &statement_cache->statement(db, select_sql);
        } else {
            local_select_stmt = std::make_unique<SqliteStatement>(db, select_sql);
            select_stmt = local_select_stmt.get();
        }

        std::vector<PendingLiveHiUpdate> pending_live_hi_updates;
        std::vector<PendingInsert> pending_inserts;
        pending_live_hi_updates.reserve(max_update_batch);
        pending_inserts.reserve(max_insert_batch);

        auto statement_for_sql = [&](const std::string &sql, std::unique_ptr<SqliteStatement> &local_stmt) -> SqliteStatement & {
            if (statement_cache) {
                return statement_cache->statement(db, sql);
            }
            local_stmt = std::make_unique<SqliteStatement>(db, sql);
            return *local_stmt;
        };

        auto flush_live_hi_updates = [&] {
            for (std::size_t start = 0; start < pending_live_hi_updates.size(); start += max_update_batch) {
                const std::size_t count =
                    std::min<std::size_t>(max_update_batch, pending_live_hi_updates.size() - start);
                const std::string sql = live_hi_update_batch_sql(count);
                std::unique_ptr<SqliteStatement> local_stmt;
                SqliteStatement &stmt = statement_for_sql(sql, local_stmt);
                stmt.reset();
                int bind_index = 1;
                for (std::size_t i = 0; i < count; ++i) {
                    const PendingLiveHiUpdate &pending = pending_live_hi_updates[start + i];
                    stmt.bind_int64(bind_index++, pending.rowid);
                    stmt.bind_int64(bind_index++, pending.live_hi);
                }
                for (std::size_t i = 0; i < count; ++i) {
                    stmt.bind_int64(bind_index++, pending_live_hi_updates[start + i].rowid);
                }
                stmt.step_done();
            }
            pending_live_hi_updates.clear();
        };

        const std::string update_replacement_sql = replacement_update_sql();
        std::unique_ptr<SqliteStatement> local_update_replacement_stmt;
        SqliteStatement *update_replacement_stmt = nullptr;
        if (statement_cache) {
            update_replacement_stmt = &statement_cache->statement(db, update_replacement_sql);
        } else {
            local_update_replacement_stmt = std::make_unique<SqliteStatement>(db, update_replacement_sql);
            update_replacement_stmt = local_update_replacement_stmt.get();
        }

        auto update_replacement_row = [&](const PhysicalRow &physical,
                                          const std::vector<Value> &values,
                                          std::int64_t row_live_hi,
                                          std::int64_t row_writer_segment_id,
                                          std::int64_t row_deleted) {
            update_replacement_stmt->reset();
            int bind_index = 1;
            for (const auto &value : values) {
                update_replacement_stmt->bind(bind_index++, value);
            }
            update_replacement_stmt->bind_int64(bind_index++, row_live_hi);
            update_replacement_stmt->bind_int64(bind_index++, row_writer_segment_id);
            update_replacement_stmt->bind_int64(bind_index++, row_deleted);
            update_replacement_stmt->bind_int64(bind_index++, physical.rowid);
            update_replacement_stmt->step_done();
        };

        auto flush_inserts = [&] {
            if (pending_inserts.empty()) return;
            // Narrow old rows first, then insert replacement/right fragments.
            // Physical table keys include live_lo, so middle/right fragments do
            // not collide with the old row after its live_hi is shortened.
            flush_live_hi_updates();
            for (std::size_t start = 0; start < pending_inserts.size(); start += max_insert_batch) {
                const std::size_t count =
                    std::min<std::size_t>(max_insert_batch, pending_inserts.size() - start);
                const std::string sql = insert_batch_sql(count);
                std::unique_ptr<SqliteStatement> local_stmt;
                SqliteStatement &stmt = statement_for_sql(sql, local_stmt);
                stmt.reset();
                int bind_index = 1;
                for (std::size_t i = 0; i < count; ++i) {
                    const PendingInsert &pending = pending_inserts[start + i];
                    for (const auto &value : *pending.values) {
                        stmt.bind(bind_index++, value);
                    }
                    stmt.bind_int64(bind_index++, pending.live_lo);
                    stmt.bind_int64(bind_index++, pending.live_hi);
                    stmt.bind_int64(bind_index++, pending.writer_segment_id);
                    stmt.bind_int64(bind_index++, pending.deleted);
                }
                stmt.step_done();
            }
            pending_inserts.clear();
        };

        std::unordered_map<NativeRowKey, std::vector<PhysicalRow>, NativeRowKeyHash> batch_physical_rows;
        const bool use_batch_select = splice_rows->size() > 1;
        if (use_batch_select) {
            // Fetch overlaps for many keys with one indexed range query per chunk.
            // The interval split below is unchanged; this only removes repeated
            // select/step/parse work for batched logical writes.
            const std::size_t max_params = 900;
            const std::size_t chunk_rows = std::max<std::size_t>(1, max_params / std::max<std::size_t>(1, pk_indices.size()));
            for (std::size_t start = 0; start < splice_rows->size(); start += chunk_rows) {
                const std::size_t count = std::min<std::size_t>(chunk_rows, splice_rows->size() - start);
                const std::string batch_sql =
                    "SELECT rowid, " + data_cols + ", live_lo, live_hi, writer_segment_id, deleted "
                    "FROM " + quoted_table + " "
                    "WHERE (" + key_batch_where(count) + ") AND live_lo < ? AND ? < live_hi "
                    "ORDER BY " + comma_join_quoted(pk_columns) + ", live_lo";
                std::unique_ptr<SqliteStatement> local_batch_stmt;
                SqliteStatement *batch_stmt = nullptr;
                if (statement_cache) {
                    batch_stmt = &statement_cache->statement(db, batch_sql);
                } else {
                    local_batch_stmt = std::make_unique<SqliteStatement>(db, batch_sql);
                    batch_stmt = local_batch_stmt.get();
                }
                batch_stmt->reset();
                int bind_index = 1;
                for (std::size_t offset = 0; offset < count; ++offset) {
                    const auto &row = (*splice_rows)[start + offset];
                    for (std::size_t index : pk_indices) batch_stmt->bind(bind_index++, row[index]);
                }
                batch_stmt->bind_int64(bind_index++, live_hi);
                batch_stmt->bind_int64(bind_index++, live_lo);
                while (batch_stmt->step_row()) {
                    PhysicalRow physical;
                    physical.rowid = sqlite3_column_int64(batch_stmt->get(), 0);
                    physical.values.reserve(columns.size());
                    for (std::size_t i = 0; i < columns.size(); ++i) {
                        physical.values.push_back(column_value(batch_stmt->get(), static_cast<int>(1 + i)));
                    }
                    int metadata_offset = static_cast<int>(1 + columns.size());
                    physical.live_lo = sqlite3_column_int64(batch_stmt->get(), metadata_offset);
                    physical.live_hi = sqlite3_column_int64(batch_stmt->get(), metadata_offset + 1);
                    physical.writer_segment_id = sqlite3_column_int64(batch_stmt->get(), metadata_offset + 2);
                    physical.deleted = sqlite3_column_int64(batch_stmt->get(), metadata_offset + 3);
                    batch_physical_rows[key_for_row(physical.values)].push_back(std::move(physical));
                }
            }
        }

        auto load_physical_rows = [&](const std::vector<Value> &row) {
            std::vector<PhysicalRow> physical_rows;
            if (use_batch_select) {
                auto found = batch_physical_rows.find(key_for_row(row));
                if (found != batch_physical_rows.end()) {
                    physical_rows = std::move(found->second);
                }
                return physical_rows;
            }
            select_stmt->reset();
            int bind_index = 1;
            for (std::size_t index : pk_indices) {
                select_stmt->bind(bind_index++, row[index]);
            }
            select_stmt->bind_int64(bind_index++, live_hi);
            select_stmt->bind_int64(bind_index++, live_lo);
            while (select_stmt->step_row()) {
                PhysicalRow physical;
                physical.rowid = sqlite3_column_int64(select_stmt->get(), 0);
                physical.values.reserve(columns.size());
                for (std::size_t i = 0; i < columns.size(); ++i) {
                    physical.values.push_back(column_value(select_stmt->get(), static_cast<int>(1 + i)));
                }
                int metadata_offset = static_cast<int>(1 + columns.size());
                physical.live_lo = sqlite3_column_int64(select_stmt->get(), metadata_offset);
                physical.live_hi = sqlite3_column_int64(select_stmt->get(), metadata_offset + 1);
                physical.writer_segment_id = sqlite3_column_int64(select_stmt->get(), metadata_offset + 2);
                physical.deleted = sqlite3_column_int64(select_stmt->get(), metadata_offset + 3);
                physical_rows.push_back(std::move(physical));
            }
            return physical_rows;
        };

        struct PendingSplice {
            const std::vector<Value> *replacement = nullptr;
            std::vector<PhysicalRow> physical_rows;
        };

        for (std::size_t chunk_start = 0; chunk_start < splice_rows->size(); chunk_start += physical_write_batch_size) {
            const std::size_t chunk_count =
                std::min<std::size_t>(physical_write_batch_size, splice_rows->size() - chunk_start);
            std::vector<PendingSplice> chunk;
            chunk.reserve(chunk_count);
            for (std::size_t offset = 0; offset < chunk_count; ++offset) {
                const auto &row = (*splice_rows)[chunk_start + offset];
                std::vector<PhysicalRow> physical_rows = load_physical_rows(row);
                if (insert_mode_checks_conflicts(write_mode) &&
                    physical_rows_contain_visible_live_conflict<PhysicalRow, std::int64_t, Int64IntervalBounds>(
                        physical_rows,
                        branch_point
                    )) {
                    if (insert_mode_should_skip_conflict(write_mode)) continue;
                    raise_insert_duplicate_key();
                }
                ++result.logical_rows_written;
                chunk.push_back(PendingSplice{&row, std::move(physical_rows)});
            }

            auto insert_physical = [&](const std::vector<Value> &values,
                                       std::int64_t row_live_lo,
                                       std::int64_t row_live_hi,
                                       std::int64_t row_writer_segment_id,
                                       std::int64_t row_deleted) {
                pending_inserts.push_back(
                    PendingInsert{&values, row_live_lo, row_live_hi, row_writer_segment_id, row_deleted});
                ++stats.inserted;
                if (pending_inserts.size() >= max_insert_batch) {
                    flush_inserts();
                }
            };
            auto shrink_left_row = [&](const PhysicalRow &physical, std::int64_t row_live_hi) {
                pending_live_hi_updates.push_back(PendingLiveHiUpdate{physical.rowid, row_live_hi});
                if (pending_live_hi_updates.size() >= max_update_batch) {
                    flush_live_hi_updates();
                }
            };
            auto replace_row = [&](const PhysicalRow &physical,
                                   const std::vector<Value> &values,
                                   std::int64_t row_live_hi,
                                   std::int64_t row_writer_segment_id,
                                   std::int64_t row_deleted) {
                update_replacement_row(
                    physical,
                    values,
                    row_live_hi,
                    row_writer_segment_id,
                    row_deleted
                );
            };
            for (const PendingSplice &work : chunk) {
                splice_interval_rows<PhysicalRow, std::int64_t, Int64IntervalBounds>(
                    stats,
                    *work.replacement,
                    work.physical_rows,
                    live_lo,
                    live_hi,
                    writer_segment_id,
                    replacement_deleted,
                    insert_physical,
                    shrink_left_row,
                    replace_row
                );
            }
            flush_inserts();
        }
        flush_inserts();
        flush_live_hi_updates();
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
    return result;
}

struct PgPhysicalRow {
    std::string ctid;
    std::vector<Value> values;
    std::string live_lo;
    std::string live_hi;
    std::int64_t writer_segment_id;
    std::int64_t deleted;
};

BulkUpsertResult postgres_adapter_bulk_upsert(
    PGconn *conn,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const NativeRows &rows,
    const std::string &live_lo,
    const std::string &live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction,
    PgPreparedStatementCache *statement_cache,
    IntervalWriteMode write_mode,
    const std::string &branch_point
) {
    if (columns.empty()) {
        throw std::invalid_argument("columns must not be empty");
    }
    if (pk_columns.empty()) {
        throw std::invalid_argument("pk_columns must not be empty");
    }

    BulkUpsertResult result;
    BulkUpsertStats &stats = result.stats;
    const std::string quoted_table = quote_ident(physical_name);
    const std::vector<std::size_t> pk_indices = pk_column_indices(columns, pk_columns);
    // PostgreSQL uses ctid for the selected physical row instances. The shared
    // splice algorithm decides interval fragments; this adapter implements the
    // engine operations for UPDATE-in-place and INSERT.
    NativeRows deduped_storage;
    const NativeRows *splice_rows = &rows;
    if (rows.size() > 1) {
        deduped_storage = dedupe_rows_by_pk(rows, pk_indices);
        splice_rows = &deduped_storage;
    }

    const std::string data_cols = comma_join_quoted(columns);
    const std::string all_insert_cols = data_cols + ", \"live_lo\", \"live_hi\", \"writer_segment_id\", \"deleted\"";
    const int live_lo_param = static_cast<int>(pk_columns.size()) + 1;
    const int live_hi_param = static_cast<int>(pk_columns.size()) + 2;
    const std::string select_sql =
        "SELECT ctid::text AS __chronos_ctid, " + data_cols + ", live_lo, live_hi, writer_segment_id, deleted "
        "FROM " + quoted_table + " "
        "WHERE " + pg_key_where_sql(pk_columns) + " AND live_lo < $" + std::to_string(live_lo_param) +
        " AND $" + std::to_string(live_hi_param) + " < live_hi "
        "ORDER BY live_lo FOR UPDATE";
    const std::size_t max_pg_params = 60000;
    const std::size_t physical_write_batch_size = 500;
    const std::size_t insert_value_count = columns.size() + 4;
    const std::size_t replacement_value_count = columns.size() + 4;
    const std::size_t max_insert_batch =
        std::max<std::size_t>(
            1,
            std::min(
                physical_write_batch_size,
                max_pg_params / std::max<std::size_t>(1, insert_value_count)));
    const std::size_t max_live_hi_update_batch =
        std::max<std::size_t>(1, std::min<std::size_t>(physical_write_batch_size, max_pg_params / 2));
    const std::size_t max_replacement_update_batch =
        std::max<std::size_t>(
            1,
            std::min(
                physical_write_batch_size,
                max_pg_params / std::max<std::size_t>(1, replacement_value_count)));

    auto key_for_row = [&](const std::vector<Value> &row) {
        NativeRowKey key;
        key.values.reserve(pk_indices.size());
        for (std::size_t index : pk_indices) key.values.push_back(row[index]);
        return key;
    };

    struct PendingInsert {
        std::vector<Value> values;
        std::string live_lo;
        std::string live_hi;
        std::int64_t writer_segment_id = 0;
        std::int64_t deleted = 0;
    };

    struct PendingLiveHiUpdate {
        std::string ctid;
        std::string live_hi;
    };

    struct PendingReplacementUpdate {
        std::string ctid;
        std::vector<Value> values;
        std::string live_hi;
        std::int64_t writer_segment_id = 0;
        std::int64_t deleted = 0;
    };

    auto insert_batch_sql = [&](std::size_t count) {
        std::string sql =
            "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") VALUES ";
        int param = 1;
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            if (row_index) sql += ", ";
            sql += "(" + pg_placeholders(insert_value_count, param) + ")";
            param += static_cast<int>(insert_value_count);
        }
        return sql;
    };

    auto live_hi_update_batch_sql = [&](std::size_t count) {
        std::string sql =
            "UPDATE " + quoted_table + " AS t SET \"live_hi\" = v.\"live_hi\" "
            "FROM (VALUES ";
        int param = 1;
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            if (row_index) sql += ", ";
            const int ctid_param = param++;
            const int live_hi_param = param++;
            sql += "($" + std::to_string(ctid_param) + "::tid, $" +
                std::to_string(live_hi_param) + "::numeric)";
        }
        sql += ") AS v(\"__chronos_ctid\", \"live_hi\") "
            "WHERE t.ctid = v.\"__chronos_ctid\"";
        return sql;
    };

    auto replacement_update_batch_sql = [&](std::size_t count) {
        std::string sql = "UPDATE " + quoted_table + " AS t SET ";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (i) sql += ", ";
            sql += quote_ident(columns[i]) + " = CASE t.ctid ";
            for (std::size_t row_index = 0; row_index < count; ++row_index) {
                const int base_param = 1 + static_cast<int>(row_index * replacement_value_count);
                const int ctid_param = base_param;
                const int value_param = base_param + 1 + static_cast<int>(i);
                sql += "WHEN $" + std::to_string(ctid_param) + "::tid THEN $" +
                    std::to_string(value_param) + " ";
            }
            sql += "ELSE t." + quote_ident(columns[i]) + " END";
        }
        sql += ", \"live_hi\" = CASE t.ctid ";
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            const int base_param = 1 + static_cast<int>(row_index * replacement_value_count);
            sql += "WHEN $" + std::to_string(base_param) + "::tid THEN $" +
                std::to_string(base_param + 1 + static_cast<int>(columns.size())) +
                "::numeric ";
        }
        sql += "ELSE t.\"live_hi\" END";

        sql += ", \"writer_segment_id\" = CASE t.ctid ";
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            const int base_param = 1 + static_cast<int>(row_index * replacement_value_count);
            sql += "WHEN $" + std::to_string(base_param) + "::tid THEN $" +
                std::to_string(base_param + 2 + static_cast<int>(columns.size())) +
                "::bigint ";
        }
        sql += "ELSE t.\"writer_segment_id\" END";

        sql += ", \"deleted\" = CASE t.ctid ";
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            const int base_param = 1 + static_cast<int>(row_index * replacement_value_count);
            sql += "WHEN $" + std::to_string(base_param) + "::tid THEN $" +
                std::to_string(base_param + 3 + static_cast<int>(columns.size())) +
                "::boolean ";
        }
        sql += "ELSE t.\"deleted\" END WHERE t.ctid IN (";
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            if (row_index) sql += ", ";
            const int base_param = 1 + static_cast<int>(row_index * replacement_value_count);
            sql += "$" + std::to_string(base_param) + "::tid";
        }
        sql += ")";
        return sql;
    };

    auto pg_key_batch_where = [&](std::size_t count, int start_param) {
        std::string sql;
        int param = start_param;
        for (std::size_t row_index = 0; row_index < count; ++row_index) {
            if (row_index) sql += " OR ";
            sql += "(";
            for (std::size_t pk_index = 0; pk_index < pk_columns.size(); ++pk_index) {
                if (pk_index) sql += " AND ";
                sql += quote_ident(pk_columns[pk_index]) + " = $" + std::to_string(param++);
            }
            sql += ")";
        }
        return sql;
    };

    auto exec_simple = [&](const std::string &sql) {
        record_sql_profile_statement(sql, SqlProfileCall::SimpleExec);
        const auto start = std::chrono::steady_clock::now();
        PGresult *raw = PQexec(conn, sql.c_str());
        record_sql_trace_statement(
            sql,
            SqlProfileCall::SimpleExec,
            elapsed_ms_since(start),
            pg_result_rows(raw),
            0
        );
        PgResult result(conn, raw);
        result.require(PGRES_COMMAND_OK);
    };

    struct PendingPgWriteCommand {
        std::string sql;
        std::vector<Value> params;
    };

    std::vector<PendingInsert> pending_inserts;
    std::vector<PendingLiveHiUpdate> pending_live_hi_updates;
    std::vector<PendingReplacementUpdate> pending_replacement_updates;
    pending_inserts.reserve(max_insert_batch);
    pending_live_hi_updates.reserve(max_live_hi_update_batch);
    pending_replacement_updates.reserve(max_replacement_update_batch);

    auto execute_bulk_write = [&](const std::string &sql, const std::vector<Value> &params) {
        SqlProfileScope scope(SqlProfileBucket::BulkWrite);
        PgResult result = statement_cache
            ? statement_cache->exec(conn, sql, params)
            : pg_exec_params(conn, sql, params);
        result.require(PGRES_COMMAND_OK);
    };

    auto execute_bulk_write_commands = [&](const std::vector<PendingPgWriteCommand> &commands) {
        if (commands.empty()) return;
        if (commands.size() == 1) {
            execute_bulk_write(commands[0].sql, commands[0].params);
            return;
        }

        struct PendingPipelineCommand {
            std::string sql;
            PgParams params;
        };
        std::vector<PendingPipelineCommand> pending;
        pending.reserve(commands.size());
        for (const auto &command : commands) {
            pending.push_back({command.sql, make_pg_params(command.params)});
        }

        {
            SqlProfileScope scope(SqlProfileBucket::BulkWrite);
            record_sql_profile_pipeline(
                static_cast<std::int64_t>(pending.size()),
                0,
                static_cast<std::int64_t>(pending.size())
            );
        }
        const auto pipeline_start = std::chrono::steady_clock::now();
        if (PQenterPipelineMode(conn) != 1) {
            throw PgError(PQerrorMessage(conn));
        }
        bool pipeline_active = true;
        try {
            for (const auto &command : pending) {
                const int param_count = static_cast<int>(command.params.values.size());
                if (PQsendQueryParams(
                        conn,
                        command.sql.c_str(),
                        param_count,
                        nullptr,
                        command.params.values.empty() ? nullptr : command.params.values.data(),
                        command.params.lengths.empty() ? nullptr : command.params.lengths.data(),
                        command.params.formats.empty() ? nullptr : command.params.formats.data(),
                        0
                    ) != 1) {
                    throw PgError(PQerrorMessage(conn));
                }
            }
            if (PQpipelineSync(conn) != 1 || PQflush(conn) == -1) {
                throw PgError(PQerrorMessage(conn));
            }

            std::size_t command_results = 0;
            bool saw_sync = false;
            while (!saw_sync) {
                PGresult *raw = PQgetResult(conn);
                if (raw == nullptr) continue;
                PgResult result(conn, raw);
                const ExecStatusType status = PQresultStatus(result.get());
                if (status == PGRES_PIPELINE_SYNC) {
                    saw_sync = true;
                    break;
                }
                result.require(PGRES_COMMAND_OK);
                ++command_results;
            }
            if (PQexitPipelineMode(conn) != 1) {
                throw PgError(PQerrorMessage(conn));
            }
            pipeline_active = false;
            if (command_results != commands.size()) {
                throw PgError("PostgreSQL bulk write pipeline returned an unexpected result count");
            }
            const double pipeline_elapsed_ms = elapsed_ms_since(pipeline_start);
            record_sql_trace_statement(
                "PIPELINE_SYNC statements=" + std::to_string(commands.size()),
                "pipeline_batch",
                SqlProfileBucket::BulkWrite,
                pipeline_elapsed_ms,
                static_cast<std::int64_t>(command_results),
                static_cast<std::int64_t>(commands.size())
            );
            const double allocated_ms = commands.empty()
                ? 0.0
                : pipeline_elapsed_ms / static_cast<double>(commands.size());
            for (const auto &command : commands) {
                record_sql_trace_statement(
                    command.sql,
                    "pipeline_statement",
                    SqlProfileBucket::BulkWrite,
                    allocated_ms,
                    -1,
                    static_cast<std::int64_t>(command.params.size())
                );
            }
        } catch (...) {
            while (PGresult *raw = PQgetResult(conn)) {
                PQclear(raw);
            }
            if (pipeline_active) {
                PQexitPipelineMode(conn);
            }
            throw;
        }
    };

    auto append_live_hi_update_commands = [&](std::vector<PendingPgWriteCommand> &commands) {
        if (pending_live_hi_updates.empty()) return;
        for (std::size_t start = 0; start < pending_live_hi_updates.size(); start += max_live_hi_update_batch) {
            const std::size_t count =
                std::min<std::size_t>(max_live_hi_update_batch, pending_live_hi_updates.size() - start);
            std::vector<Value> params;
            params.reserve(count * 2);
            for (std::size_t i = 0; i < count; ++i) {
                const PendingLiveHiUpdate &pending = pending_live_hi_updates[start + i];
                params.push_back(pending.ctid);
                params.push_back(pending.live_hi);
            }
            commands.push_back(PendingPgWriteCommand{live_hi_update_batch_sql(count), std::move(params)});
        }
        pending_live_hi_updates.clear();
    };

    auto append_replacement_update_commands = [&](std::vector<PendingPgWriteCommand> &commands) {
        if (pending_replacement_updates.empty()) return;
        for (std::size_t start = 0; start < pending_replacement_updates.size(); start += max_replacement_update_batch) {
            const std::size_t count =
                std::min<std::size_t>(max_replacement_update_batch, pending_replacement_updates.size() - start);
            std::vector<Value> params;
            params.reserve(count * replacement_value_count);
            for (std::size_t i = 0; i < count; ++i) {
                const PendingReplacementUpdate &pending = pending_replacement_updates[start + i];
                params.push_back(pending.ctid);
                params.insert(params.end(), pending.values.begin(), pending.values.end());
                params.push_back(pending.live_hi);
                params.push_back(pending.writer_segment_id);
                params.push_back(std::string(pending.deleted ? "true" : "false"));
            }
            commands.push_back(PendingPgWriteCommand{replacement_update_batch_sql(count), std::move(params)});
        }
        pending_replacement_updates.clear();
    };

    auto append_insert_commands = [&](std::vector<PendingPgWriteCommand> &commands) {
        if (pending_inserts.empty()) return;
        for (std::size_t start = 0; start < pending_inserts.size(); start += max_insert_batch) {
            const std::size_t count =
                std::min<std::size_t>(max_insert_batch, pending_inserts.size() - start);
            std::vector<Value> params;
            params.reserve(count * insert_value_count);
            for (std::size_t i = 0; i < count; ++i) {
                const PendingInsert &pending = pending_inserts[start + i];
                params.insert(params.end(), pending.values.begin(), pending.values.end());
                params.push_back(pending.live_lo);
                params.push_back(pending.live_hi);
                params.push_back(pending.writer_segment_id);
                params.push_back(std::string(pending.deleted ? "true" : "false"));
            }
            commands.push_back(PendingPgWriteCommand{insert_batch_sql(count), std::move(params)});
            stats.inserted += static_cast<std::int64_t>(count);
        }
        pending_inserts.clear();
    };

    auto flush_pending_writes = [&] {
        std::vector<PendingPgWriteCommand> commands;
        commands.reserve(3);
        append_live_hi_update_commands(commands);
        append_replacement_update_commands(commands);
        append_insert_commands(commands);
        execute_bulk_write_commands(commands);
    };

    auto flush_inserts = [&] {
        if (pending_inserts.empty()) return;
        flush_pending_writes();
    };

    auto insert_row = [&](const std::vector<Value> &values,
                          const std::string &row_live_lo,
                          const std::string &row_live_hi,
                          std::int64_t row_writer_segment_id,
                          std::int64_t row_deleted) {
        pending_inserts.push_back(
            PendingInsert{values, row_live_lo, row_live_hi, row_writer_segment_id, row_deleted});
        if (pending_inserts.size() >= max_insert_batch) {
            flush_inserts();
        }
    };

    auto shrink_left_row = [&](const PgPhysicalRow &physical, const std::string &row_live_hi) {
        pending_live_hi_updates.push_back(PendingLiveHiUpdate{physical.ctid, row_live_hi});
        if (pending_live_hi_updates.size() >= max_live_hi_update_batch) {
            flush_pending_writes();
        }
    };

    auto replace_row = [&](const PgPhysicalRow &physical,
                           const std::vector<Value> &values,
                           const std::string &row_live_hi,
                           std::int64_t row_writer_segment_id,
                           std::int64_t row_deleted) {
        pending_replacement_updates.push_back(PendingReplacementUpdate{
            physical.ctid,
            values,
            row_live_hi,
            row_writer_segment_id,
            row_deleted
        });
        if (pending_replacement_updates.size() >= max_replacement_update_batch) {
            flush_pending_writes();
        }
    };

    try {
        if (manage_transaction) {
            SqlProfileScope scope(SqlProfileBucket::BulkTransaction);
            exec_simple("BEGIN");
        }

        std::unordered_map<NativeRowKey, std::vector<PgPhysicalRow>, NativeRowKeyHash> batch_physical_rows;
        const bool use_batch_select = splice_rows->size() > 1;
        if (use_batch_select) {
            const std::size_t chunk_rows = 500;
            for (std::size_t start = 0; start < splice_rows->size(); start += chunk_rows) {
                const std::size_t count = std::min<std::size_t>(chunk_rows, splice_rows->size() - start);
                const int live_lo_batch_param = static_cast<int>(count * pk_columns.size()) + 1;
                const int live_hi_batch_param = live_lo_batch_param + 1;
                const std::string batch_sql =
                    "SELECT ctid::text AS __chronos_ctid, " + data_cols + ", live_lo, live_hi, writer_segment_id, deleted "
                    "FROM " + quoted_table + " "
                    "WHERE (" + pg_key_batch_where(count, 1) + ") AND live_lo < $" +
                    std::to_string(live_lo_batch_param) + " AND $" +
                    std::to_string(live_hi_batch_param) + " < live_hi "
                    "ORDER BY " + comma_join_quoted(pk_columns) + ", live_lo FOR UPDATE";
                std::vector<Value> batch_params;
                batch_params.reserve(count * pk_indices.size() + 2);
                for (std::size_t offset = 0; offset < count; ++offset) {
                    const auto &row = (*splice_rows)[start + offset];
                    for (std::size_t index : pk_indices) batch_params.push_back(row[index]);
                }
                batch_params.push_back(live_hi);
                batch_params.push_back(live_lo);
                SqlProfileScope scope(SqlProfileBucket::BulkSelect);
                PgResult selected = statement_cache
                    ? statement_cache->exec(conn, batch_sql, batch_params)
                    : pg_exec_params(conn, batch_sql, batch_params);
                selected.require(PGRES_TUPLES_OK);
                const int row_count = PQntuples(selected.get());
                for (int r = 0; r < row_count; ++r) {
                    PgPhysicalRow physical;
                    physical.ctid = PQgetvalue(selected.get(), r, 0);
                    physical.values.reserve(columns.size());
                    for (std::size_t i = 0; i < columns.size(); ++i) {
                        physical.values.push_back(pg_column_value(selected.get(), r, static_cast<int>(1 + i)));
                    }
                    int metadata_offset = static_cast<int>(1 + columns.size());
                    physical.live_lo = PQgetvalue(selected.get(), r, metadata_offset);
                    physical.live_hi = PQgetvalue(selected.get(), r, metadata_offset + 1);
                    physical.writer_segment_id = std::get<std::int64_t>(
                        pg_column_value(selected.get(), r, metadata_offset + 2)
                    );
                    physical.deleted = std::get<std::int64_t>(pg_column_value(selected.get(), r, metadata_offset + 3));
                    batch_physical_rows[key_for_row(physical.values)].push_back(std::move(physical));
                }
            }
        }

        for (const auto &row : *splice_rows) {
            std::vector<PgPhysicalRow> physical_rows;
            if (use_batch_select) {
                auto found = batch_physical_rows.find(key_for_row(row));
                if (found != batch_physical_rows.end()) {
                    physical_rows = std::move(found->second);
                }
            } else {
                std::vector<Value> select_params;
                select_params.reserve(pk_indices.size() + 2);
                for (std::size_t index : pk_indices) {
                    select_params.push_back(row[index]);
                }
                select_params.push_back(live_hi);
                select_params.push_back(live_lo);

                SqlProfileScope scope(SqlProfileBucket::BulkSelect);
                PgResult selected = statement_cache
                    ? statement_cache->exec(conn, select_sql, select_params)
                    : pg_exec_params(conn, select_sql, select_params);
                selected.require(PGRES_TUPLES_OK);
                const int row_count = PQntuples(selected.get());
                physical_rows.reserve(static_cast<std::size_t>(row_count));
                for (int r = 0; r < row_count; ++r) {
                    PgPhysicalRow physical;
                    physical.ctid = PQgetvalue(selected.get(), r, 0);
                    physical.values.reserve(columns.size());
                    for (std::size_t i = 0; i < columns.size(); ++i) {
                        physical.values.push_back(pg_column_value(selected.get(), r, static_cast<int>(1 + i)));
                    }
                    int metadata_offset = static_cast<int>(1 + columns.size());
                    physical.live_lo = PQgetvalue(selected.get(), r, metadata_offset);
                    physical.live_hi = PQgetvalue(selected.get(), r, metadata_offset + 1);
                    physical.writer_segment_id = std::get<std::int64_t>(
                        pg_column_value(selected.get(), r, metadata_offset + 2)
                    );
                    physical.deleted = std::get<std::int64_t>(pg_column_value(selected.get(), r, metadata_offset + 3));
                    physical_rows.push_back(std::move(physical));
                }
            }
            if (insert_mode_checks_conflicts(write_mode) &&
                physical_rows_contain_visible_live_conflict<PgPhysicalRow, std::string, DecimalIntervalBounds>(
                    physical_rows,
                    branch_point
                )) {
                if (insert_mode_should_skip_conflict(write_mode)) continue;
                raise_insert_duplicate_key();
            }
            ++result.logical_rows_written;
            splice_interval_rows<PgPhysicalRow, std::string, DecimalIntervalBounds>(
                stats,
                row,
                physical_rows,
                live_lo,
                live_hi,
                writer_segment_id,
                replacement_deleted,
                insert_row,
                shrink_left_row,
                replace_row
            );
        }
        flush_pending_writes();

        if (manage_transaction) {
            SqlProfileScope scope(SqlProfileBucket::BulkTransaction);
            exec_simple("COMMIT");
        }
    } catch (...) {
        if (manage_transaction) {
            try {
                SqlProfileScope scope(SqlProfileBucket::BulkTransaction);
                exec_simple("ROLLBACK");
            } catch (...) {
            }
        }
        throw;
    }
    return result;
}

// -----------------------------------------------------------------------------
// SQL driver implementations for SQLite/PostgreSQL/DuckDB
// Source: interval_sql_drivers.cpp
// -----------------------------------------------------------------------------
class NativeSqlDriver {
  public:
    virtual ~NativeSqlDriver() = default;
    virtual const std::string &dialect() const = 0;
    virtual bool in_transaction() const = 0;
    virtual QueryResult query_result(const std::string &sql, const std::vector<Value> &params = {}) = 0;
    std::vector<std::vector<Value>> query(const std::string &sql, const std::vector<Value> &params = {}) {
        return query_result(sql, params).rows;
    }
    virtual std::vector<QueryResult> query_many(
        const std::vector<std::pair<std::string, std::vector<Value>>> &queries
    ) {
        std::vector<QueryResult> results;
        results.reserve(queries.size());
        for (const auto &[sql, params] : queries) {
            results.push_back(query_result(sql, params));
        }
        return results;
    }
    virtual void execute(const std::string &sql, const std::vector<Value> &params = {}) = 0;
    virtual std::int64_t execute_changes(const std::string &sql, const std::vector<Value> &params = {}) = 0;
    virtual void refresh_catalog() {}
    virtual BulkUpsertResult interval_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        const std::string &live_lo,
        const std::string &live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction,
        IntervalWriteMode write_mode,
        const std::string &branch_point
    ) = 0;
};

// SQLite, DuckDB, and Postgres all expose enough relational primitives for the
// interval backend: indexed visible scans, keyed deletes, and bulk inserts.
// Drivers own SQL syntax/connection details; NativeBranchStore above them owns
// branch metadata, statement planning, merge staging, and the shared splice
// algorithm.
class NativeSQLiteDriver final : public NativeSqlDriver {
  public:
    explicit NativeSQLiteDriver(const std::string &database_url) : dialect_("sqlite") {
        std::string path = sqlite_path_from_url(database_url);
        if (sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
            std::string msg = db_ ? sqlite3_errmsg(db_) : "could not open SQLite database";
            throw SqliteError(msg);
        }
        owns_connection_ = true;
        sqlite3_busy_timeout(db_, 30000);
        execute("PRAGMA foreign_keys=OFF");
        execute("PRAGMA journal_mode=WAL");
        const std::int64_t wal_autocheckpoint_pages =
            native_sqlite_wal_autocheckpoint_pages_from_env().value_or(
                kDefaultNativeSqliteWalAutocheckpointPages);
        // SQLite measures wal_autocheckpoint in database pages.  The native
        // default is intentionally larger than SQLite's 1000-page default so
        // large ChronosFS/interval writes do not synchronously checkpoint in
        // the middle of one logical copy-on-write operation.  Value 0 remains
        // an explicit override that disables automatic checkpoints.
        execute("PRAGMA wal_autocheckpoint=" + std::to_string(wal_autocheckpoint_pages));
        if (auto bytes = native_sqlite_journal_size_limit_bytes_from_env()) {
            execute("PRAGMA journal_size_limit=" + std::to_string(*bytes));
        }
        if (auto synchronous = native_sqlite_synchronous_from_env()) {
            execute("PRAGMA synchronous=" + *synchronous);
            execute("PRAGMA fullfsync=OFF");
            execute("PRAGMA checkpoint_fullfsync=OFF");
        }
        if (auto cache_size = native_sqlite_cache_size_kib_from_env()) {
            // Negative cache_size values are KiB units in SQLite.  The benchmark
            // uses this to cap SQLite's page cache while direct I/O bypasses the
            // kernel page cache for the mounted filesystem path.
            execute("PRAGMA cache_size=-" + std::to_string(*cache_size));
        }
    }
    explicit NativeSQLiteDriver(sqlite3 *db) : db_(db), dialect_("sqlite"), owns_connection_(false) {
        if (!db_) {
            throw SqliteError("SQLite connection pointer is null");
        }
        sqlite3_busy_timeout(db_, 30000);
        const std::int64_t wal_autocheckpoint_pages =
            native_sqlite_wal_autocheckpoint_pages_from_env().value_or(
                kDefaultNativeSqliteWalAutocheckpointPages);
        execute("PRAGMA wal_autocheckpoint=" + std::to_string(wal_autocheckpoint_pages));
    }
    ~NativeSQLiteDriver() override {
        for (auto &[_, stmt] : statements_) sqlite3_finalize(stmt);
        if (owns_connection_ && db_) sqlite3_close(db_);
    }
    const std::string &dialect() const override { return dialect_; }
    bool in_transaction() const override { return sqlite3_get_autocommit(db_) == 0; }

    QueryResult query_result(const std::string &sql, const std::vector<Value> &params = {}) override {
        sqlite3_stmt *stmt = statement(sql);
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        bind_all(stmt, params);
        QueryResult result;
        int cols = sqlite3_column_count(stmt);
        result.columns.reserve(static_cast<std::size_t>(cols));
        for (int c = 0; c < cols; ++c) {
            const char *name = sqlite3_column_name(stmt, c);
            result.columns.emplace_back(name ? name : "");
        }
        while (true) {
            int rc = sqlite3_step(stmt);
            if (rc == SQLITE_DONE) break;
            if (rc != SQLITE_ROW) {
                std::string msg = sqlite3_errmsg(db_);
                sqlite3_reset(stmt);
                throw SqliteError(msg);
            }
            std::vector<Value> row;
            row.reserve(static_cast<std::size_t>(cols));
            for (int c = 0; c < cols; ++c) row.push_back(column_value(stmt, c));
            result.rows.push_back(std::move(row));
        }
        sqlite3_reset(stmt);
        return result;
    }

    void execute(const std::string &sql, const std::vector<Value> &params = {}) override {
        sqlite3_stmt *stmt = statement(sql);
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        bind_all(stmt, params);
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
            std::string msg = sqlite3_errmsg(db_);
            sqlite3_reset(stmt);
            throw SqliteError(msg);
        }
        sqlite3_reset(stmt);
    }

    std::int64_t execute_changes(const std::string &sql, const std::vector<Value> &params = {}) override {
        execute(sql, params);
        return sqlite3_changes(db_);
    }

    BulkUpsertResult interval_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        const std::string &live_lo,
        const std::string &live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction,
        IntervalWriteMode write_mode,
        const std::string &branch_point
    ) override {
        return sqlite_adapter_bulk_upsert(
            db_, physical_name, columns, pk_columns, rows,
            std::stoll(live_lo), std::stoll(live_hi), writer_segment_id,
            replacement_deleted, manage_transaction, &interval_upsert_statements_,
            write_mode, std::stoll(branch_point)
        );
    }

  private:
    sqlite3_stmt *statement(const std::string &sql) {
        auto found = statements_.find(sql);
        if (found != statements_.end()) return found->second;
        if (statements_.size() > 512) {
            for (auto &[_, stmt] : statements_) sqlite3_finalize(stmt);
            statements_.clear();
        }
        sqlite3_stmt *stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) throw SqliteError(db_);
        statements_.emplace(sql, stmt);
        return stmt;
    }
    void bind_all(sqlite3_stmt *stmt, const std::vector<Value> &params) {
        int index = 1;
        for (const auto &value : params) {
            int rc = SQLITE_OK;
            if (std::holds_alternative<std::monostate>(value)) rc = sqlite3_bind_null(stmt, index);
            else if (auto ptr = std::get_if<std::int64_t>(&value)) rc = sqlite3_bind_int64(stmt, index, *ptr);
            else if (auto ptr = std::get_if<double>(&value)) rc = sqlite3_bind_double(stmt, index, *ptr);
            else if (auto ptr = std::get_if<std::string>(&value)) rc = sqlite3_bind_text(stmt, index, ptr->c_str(), static_cast<int>(ptr->size()), SQLITE_TRANSIENT);
            else if (std::holds_alternative<DecimalValue>(value) ||
                     std::holds_alternative<DateValue>(value)) {
                const std::string text = value_text(value);
                rc = sqlite3_bind_text(stmt, index, text.c_str(), static_cast<int>(text.size()), SQLITE_TRANSIENT);
            }
            else {
                const auto &blob = std::get<Blob>(value);
                rc = sqlite3_bind_blob(stmt, index, blob.data(), static_cast<int>(blob.size()), SQLITE_TRANSIENT);
            }
            if (rc != SQLITE_OK) throw SqliteError(db_);
            ++index;
        }
    }
    sqlite3 *db_ = nullptr;
    std::string dialect_;
    bool owns_connection_ = false;
    std::unordered_map<std::string, sqlite3_stmt *> statements_;
    SQLiteStatementCache interval_upsert_statements_;
};

class NativeDuckDBDriver final : public NativeSqlDriver {
  public:
    explicit NativeDuckDBDriver(const std::string &database_url) : dialect_("duckdb") {
        path_ = duckdb_path_from_url(database_url);
        if (path_ != ":memory:" && !path_.empty()) {
            std::filesystem::path db_path(path_);
            if (db_path.has_parent_path()) {
                std::filesystem::create_directories(db_path.parent_path());
            }
        }
        reconnect();
    }

    ~NativeDuckDBDriver() override {
        clear_statements();
        if (conn_) duckdb_disconnect(&conn_);
        if (db_) duckdb_close(&db_);
    }

    const std::string &dialect() const override { return dialect_; }
    bool in_transaction() const override { return in_transaction_; }

    QueryResult query_result(const std::string &sql, const std::vector<Value> &params = {}) override {
        duckdb_prepared_statement stmt = statement(sql);
        bind_all(stmt, params);
        DuckDBResult result;
        if (duckdb_execute_prepared(stmt, result.get()) == DuckDBError) {
            const char *error = duckdb_result_error(result.get());
            throw NativeDuckDBError(error ? error : "DuckDB query failed");
        }
        QueryResult out;
        const idx_t cols = duckdb_column_count(result.get());
        const idx_t rows = duckdb_row_count(result.get());
        out.columns.reserve(static_cast<std::size_t>(cols));
        for (idx_t c = 0; c < cols; ++c) {
            const char *name = duckdb_column_name(result.get(), c);
            out.columns.emplace_back(name ? name : "");
        }
        out.rows.reserve(static_cast<std::size_t>(rows));
        for (idx_t r = 0; r < rows; ++r) {
            std::vector<Value> row;
            row.reserve(static_cast<std::size_t>(cols));
            for (idx_t c = 0; c < cols; ++c) {
                row.push_back(duckdb_column_value(result.get(), c, r));
            }
            out.rows.push_back(std::move(row));
        }
        return out;
    }

    void execute(const std::string &sql, const std::vector<Value> &params = {}) override {
        execute_result(sql, params);
    }

    std::int64_t execute_changes(const std::string &sql, const std::vector<Value> &params = {}) override {
        return execute_result(sql, params);
    }

    void refresh_catalog() override {
        if (path_ == ":memory:" || in_transaction_) return;
        reconnect();
    }

    BulkUpsertResult interval_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        const std::string &live_lo,
        const std::string &live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction,
        IntervalWriteMode write_mode,
        const std::string &branch_point
    ) override {
        return duckdb_interval_bulk_upsert(
            physical_name,
            columns,
            pk_columns,
            rows,
            std::stoll(live_lo),
            std::stoll(live_hi),
            writer_segment_id,
            replacement_deleted,
            manage_transaction,
            write_mode,
            std::stoll(branch_point)
        );
    }

  private:
    duckdb_prepared_statement statement(const std::string &sql) {
        auto found = statements_.find(sql);
        if (found != statements_.end()) return found->second;
        if (statements_.size() >= 512) clear_statements();
        duckdb_prepared_statement stmt = nullptr;
        if (duckdb_prepare(conn_, sql.c_str(), &stmt) == DuckDBError) {
            std::string message = stmt && duckdb_prepare_error(stmt)
                ? duckdb_prepare_error(stmt)
                : "DuckDB prepare failed";
            if (stmt) duckdb_destroy_prepare(&stmt);
            throw NativeDuckDBError(message);
        }
        statements_.emplace(sql, stmt);
        return stmt;
    }

    void clear_statements() {
        for (auto &[_, stmt] : statements_) {
            duckdb_destroy_prepare(&stmt);
        }
        statements_.clear();
    }

    void reconnect() {
        clear_statements();
        if (conn_) duckdb_disconnect(&conn_);
        if (db_) duckdb_close(&db_);
        conn_ = nullptr;
        db_ = nullptr;
        if (duckdb_open(path_.c_str(), &db_) == DuckDBError) {
            throw NativeDuckDBError("could not open DuckDB database: " + path_);
        }
        if (duckdb_connect(db_, &conn_) == DuckDBError) {
            duckdb_close(&db_);
            db_ = nullptr;
            throw NativeDuckDBError("could not connect to DuckDB database: " + path_);
        }
    }

    void bind_all(duckdb_prepared_statement stmt, const std::vector<Value> &params) {
        if (duckdb_clear_bindings(stmt) == DuckDBError) {
            throw NativeDuckDBError("DuckDB failed to clear prepared statement bindings");
        }
        idx_t index = 1;
        for (const auto &value : params) {
            duckdb_state rc = DuckDBSuccess;
            if (std::holds_alternative<std::monostate>(value)) {
                rc = duckdb_bind_null(stmt, index);
            } else if (auto ptr = std::get_if<std::int64_t>(&value)) {
                rc = duckdb_bind_int64(stmt, index, *ptr);
            } else if (auto ptr = std::get_if<double>(&value)) {
                rc = duckdb_bind_double(stmt, index, *ptr);
            } else if (auto ptr = std::get_if<std::string>(&value)) {
                rc = duckdb_bind_varchar_length(stmt, index, ptr->c_str(), ptr->size());
            } else if (std::holds_alternative<DecimalValue>(value) ||
                       std::holds_alternative<DateValue>(value)) {
                const std::string text = value_text(value);
                rc = duckdb_bind_varchar_length(stmt, index, text.c_str(), text.size());
            } else {
                const auto &blob = std::get<Blob>(value);
                rc = duckdb_bind_blob(stmt, index, blob.data(), blob.size());
            }
            if (rc == DuckDBError) {
                throw NativeDuckDBError("DuckDB failed to bind prepared statement parameter");
            }
            ++index;
        }
    }

    std::int64_t execute_result(const std::string &sql, const std::vector<Value> &params) {
        duckdb_prepared_statement stmt = statement(sql);
        bind_all(stmt, params);
        DuckDBResult result;
        if (duckdb_execute_prepared(stmt, result.get()) == DuckDBError) {
            const char *error = duckdb_result_error(result.get());
            throw NativeDuckDBError(error ? error : "DuckDB statement failed");
        }
        const std::string keyword = leading_keyword(sql);
        update_transaction_state(sql);
        if (is_schema_mutation(sql)) {
            clear_statements();
        }
        if (keyword == "COMMIT" && path_ != ":memory:") {
            // DuckDB permits multiple connections in one process, but separate
            // database handles do not necessarily observe another handle's
            // committed writes immediately.  Chronos branch commits require
            // visibility once merge_apply returns, so file-backed DuckDB
            // commits force a checkpoint before another workspace can read.
            checkpoint_after_commit();
        }
        return static_cast<std::int64_t>(duckdb_rows_changed(result.get()));
    }

    void checkpoint_after_commit() {
        clear_statements();
        DuckDBResult checkpoint_result;
        if (duckdb_query(conn_, "CHECKPOINT", checkpoint_result.get()) == DuckDBError) {
            const char *error = duckdb_result_error(checkpoint_result.get());
            throw NativeDuckDBError(error ? error : "DuckDB checkpoint failed after commit");
        }
    }

    static std::string leading_keyword(const std::string &sql) {
        std::size_t start = 0;
        while (start < sql.size() && std::isspace(static_cast<unsigned char>(sql[start]))) ++start;
        std::size_t end = start;
        while (end < sql.size() && std::isalpha(static_cast<unsigned char>(sql[end]))) ++end;
        std::string keyword = sql.substr(start, end - start);
        std::transform(keyword.begin(), keyword.end(), keyword.begin(), [](unsigned char ch) {
            return static_cast<char>(std::toupper(ch));
        });
        return keyword;
    }

    void update_transaction_state(const std::string &sql) {
        const std::string keyword = leading_keyword(sql);
        if (keyword == "BEGIN" || keyword == "START") {
            in_transaction_ = true;
        } else if (keyword == "COMMIT" || keyword == "ROLLBACK") {
            in_transaction_ = false;
        }
    }

    static bool is_schema_mutation(const std::string &sql) {
        const std::string keyword = leading_keyword(sql);
        return keyword == "CREATE" || keyword == "ALTER" || keyword == "DROP";
    }

    struct DuckDBPhysicalRow {
        std::int64_t rowid = 0;
        std::vector<Value> values;
        std::int64_t live_lo = 0;
        std::int64_t live_hi = 0;
        std::int64_t writer_segment_id = 0;
        std::int64_t deleted = 0;
    };

    BulkUpsertResult duckdb_interval_bulk_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        std::int64_t live_lo,
        std::int64_t live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction,
        IntervalWriteMode write_mode,
        std::int64_t branch_point
    ) {
        if (columns.empty()) throw std::invalid_argument("columns must not be empty");
        if (pk_columns.empty()) throw std::invalid_argument("pk_columns must not be empty");

        BulkUpsertResult result;
        BulkUpsertStats &stats = result.stats;
        const std::vector<std::size_t> pk_indices = pk_column_indices(columns, pk_columns);
        NativeRows deduped_storage;
        const NativeRows *splice_rows = &rows;
        if (rows.size() > 1) {
            deduped_storage = dedupe_rows_by_pk(rows, pk_indices);
            splice_rows = &deduped_storage;
        }

        const std::string quoted_table = quote_ident(physical_name);
        const std::string data_cols = comma_join_quoted(columns);
        const std::string all_insert_cols =
            data_cols + ", \"live_lo\", \"live_hi\", \"writer_segment_id\", \"deleted\"";
        const std::string insert_sql =
            "INSERT INTO " + quoted_table + " (" + all_insert_cols + ") VALUES (" +
            placeholders(columns.size() + 4) + ")";
        // DuckDB exposes rowid as the physical handle used by the shared splice
        // algorithm's UPDATE-in-place callbacks.
        const std::string shrink_left_sql =
            "UPDATE " + quoted_table + " SET \"live_hi\" = ? WHERE rowid = ?";
        auto replacement_update_sql = [&] {
            std::string sql = "UPDATE " + quoted_table + " SET ";
            for (std::size_t i = 0; i < columns.size(); ++i) {
                if (i) {
                    sql += ", ";
                }
                sql += quote_ident(columns[i]) + " = ?";
            }
            sql += ", \"live_hi\" = ?, \"writer_segment_id\" = ?, \"deleted\" = ? WHERE rowid = ?";
            return sql;
        };

        auto key_for_row = [&](const std::vector<Value> &row) {
            NativeRowKey key;
            key.values.reserve(pk_indices.size());
            for (std::size_t index : pk_indices) key.values.push_back(row[index]);
            return key;
        };

        auto key_batch_where = [&](std::size_t count) {
            std::string sql;
            for (std::size_t row_index = 0; row_index < count; ++row_index) {
                if (row_index) sql += " OR ";
                sql += "(";
                for (std::size_t pk_index = 0; pk_index < pk_columns.size(); ++pk_index) {
                    if (pk_index) sql += " AND ";
                    sql += quote_ident(pk_columns[pk_index]) + " = ?";
                }
                sql += ")";
            }
            return sql;
        };

        auto insert_row = [&](const std::vector<Value> &values,
                              std::int64_t row_live_lo,
                              std::int64_t row_live_hi,
                              std::int64_t row_writer_segment_id,
                              std::int64_t row_deleted) {
            std::vector<Value> params = values;
            params.reserve(columns.size() + 4);
            params.push_back(row_live_lo);
            params.push_back(row_live_hi);
            params.push_back(row_writer_segment_id);
            params.push_back(row_deleted);
            execute(insert_sql, params);
            ++stats.inserted;
        };

        auto shrink_left_row = [&](const DuckDBPhysicalRow &physical, std::int64_t row_live_hi) {
            execute(shrink_left_sql, {row_live_hi, physical.rowid});
        };

        const std::string update_replacement_sql = replacement_update_sql();
        auto replace_row = [&](const DuckDBPhysicalRow &physical,
                               const std::vector<Value> &values,
                               std::int64_t row_live_hi,
                               std::int64_t row_writer_segment_id,
                               std::int64_t row_deleted) {
            std::vector<Value> params = values;
            params.reserve(columns.size() + 4);
            params.push_back(row_live_hi);
            params.push_back(row_writer_segment_id);
            params.push_back(row_deleted);
            params.push_back(physical.rowid);
            execute(update_replacement_sql, params);
        };

        try {
            if (manage_transaction) execute("BEGIN");

            std::unordered_map<NativeRowKey, std::vector<DuckDBPhysicalRow>, NativeRowKeyHash> batch_physical_rows;
            const bool use_batch_select = splice_rows->size() > 1;
            if (use_batch_select) {
                const std::size_t chunk_rows = std::max<std::size_t>(
                    1,
                    500 / std::max<std::size_t>(1, pk_indices.size())
                );
                for (std::size_t start = 0; start < splice_rows->size(); start += chunk_rows) {
                    const std::size_t count = std::min<std::size_t>(chunk_rows, splice_rows->size() - start);
                    const std::string batch_sql =
                        "SELECT rowid, " + data_cols + ", live_lo, live_hi, writer_segment_id, deleted "
                        "FROM " + quoted_table + " "
                        "WHERE (" + key_batch_where(count) + ") AND live_lo < ? AND ? < live_hi "
                        "ORDER BY " + comma_join_quoted(pk_columns) + ", live_lo";
                    std::vector<Value> batch_params;
                    batch_params.reserve(count * pk_indices.size() + 2);
                    for (std::size_t offset = 0; offset < count; ++offset) {
                        const auto &row = (*splice_rows)[start + offset];
                        for (std::size_t index : pk_indices) batch_params.push_back(row[index]);
                    }
                    batch_params.push_back(live_hi);
                    batch_params.push_back(live_lo);
                    auto selected = query(batch_sql, batch_params);
                    for (auto &selected_row : selected) {
                        DuckDBPhysicalRow physical;
                        physical.rowid = native_as_int(selected_row[0]);
                        physical.values.reserve(columns.size());
                        for (std::size_t i = 0; i < columns.size(); ++i) {
                            physical.values.push_back(std::move(selected_row[1 + i]));
                        }
                        std::size_t metadata_offset = 1 + columns.size();
                        physical.live_lo = native_as_int(selected_row[metadata_offset]);
                        physical.live_hi = native_as_int(selected_row[metadata_offset + 1]);
                        physical.writer_segment_id = native_as_int(selected_row[metadata_offset + 2]);
                        physical.deleted = native_as_int(selected_row[metadata_offset + 3]);
                        batch_physical_rows[key_for_row(physical.values)].push_back(std::move(physical));
                    }
                }
            }

            for (const auto &row : *splice_rows) {
                std::vector<DuckDBPhysicalRow> physical_rows;
                if (use_batch_select) {
                    auto found = batch_physical_rows.find(key_for_row(row));
                    if (found != batch_physical_rows.end()) physical_rows = std::move(found->second);
                } else {
                    std::string key_where;
                    for (std::size_t i = 0; i < pk_columns.size(); ++i) {
                        if (i) key_where += " AND ";
                        key_where += quote_ident(pk_columns[i]) + " = ?";
                    }
                    const std::string select_sql =
                        "SELECT rowid, " + data_cols + ", live_lo, live_hi, writer_segment_id, deleted "
                        "FROM " + quoted_table + " "
                        "WHERE " + key_where + " AND live_lo < ? AND ? < live_hi "
                        "ORDER BY live_lo";
                    std::vector<Value> select_params;
                    select_params.reserve(pk_indices.size() + 2);
                    for (std::size_t index : pk_indices) select_params.push_back(row[index]);
                    select_params.push_back(live_hi);
                    select_params.push_back(live_lo);
                    auto selected = query(select_sql, select_params);
                    physical_rows.reserve(selected.size());
                    for (auto &selected_row : selected) {
                        DuckDBPhysicalRow physical;
                        physical.rowid = native_as_int(selected_row[0]);
                        physical.values.reserve(columns.size());
                        for (std::size_t i = 0; i < columns.size(); ++i) {
                            physical.values.push_back(std::move(selected_row[1 + i]));
                        }
                        std::size_t metadata_offset = 1 + columns.size();
                        physical.live_lo = native_as_int(selected_row[metadata_offset]);
                        physical.live_hi = native_as_int(selected_row[metadata_offset + 1]);
                        physical.writer_segment_id = native_as_int(selected_row[metadata_offset + 2]);
                        physical.deleted = native_as_int(selected_row[metadata_offset + 3]);
                        physical_rows.push_back(std::move(physical));
                    }
                }

                if (insert_mode_checks_conflicts(write_mode) &&
                    physical_rows_contain_visible_live_conflict<DuckDBPhysicalRow, std::int64_t, Int64IntervalBounds>(
                        physical_rows,
                        branch_point
                    )) {
                    if (insert_mode_should_skip_conflict(write_mode)) continue;
                    raise_insert_duplicate_key();
                }
                ++result.logical_rows_written;
                splice_interval_rows<DuckDBPhysicalRow, std::int64_t, Int64IntervalBounds>(
                    stats,
                    row,
                    physical_rows,
                    live_lo,
                    live_hi,
                    writer_segment_id,
                    replacement_deleted,
                    insert_row,
                    shrink_left_row,
                    replace_row
                );
            }

            if (manage_transaction) execute("COMMIT");
        } catch (...) {
            if (manage_transaction) {
                try {
                    execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
        return result;
    }

    std::string dialect_;
    std::string path_;
    duckdb_database db_ = nullptr;
    duckdb_connection conn_ = nullptr;
    bool in_transaction_ = false;
    std::unordered_map<std::string, duckdb_prepared_statement> statements_;
};

class NativePostgresDriver final : public NativeSqlDriver {
  public:
    explicit NativePostgresDriver(const std::string &database_url) : dialect_("postgres"), conn_(PQconnectdb(database_url.c_str())) {
        if (!conn_ || PQstatus(conn_) != CONNECTION_OK) {
            std::string message = conn_ ? PQerrorMessage(conn_) : "could not allocate PostgreSQL connection";
            if (conn_) PQfinish(conn_);
            conn_ = nullptr;
            throw PgError(message);
        }
        PQsetNoticeProcessor(conn_, chronos_pg_ignore_notice, nullptr);
        owns_connection_ = true;
    }
    explicit NativePostgresDriver(PGconn *conn) : dialect_("postgres"), conn_(conn), owns_connection_(false) {
        if (!conn_ || PQstatus(conn_) != CONNECTION_OK) {
            throw PgError(conn_ ? PQerrorMessage(conn_) : "PostgreSQL connection pointer is null");
        }
        PQsetNoticeProcessor(conn_, chronos_pg_ignore_notice, nullptr);
    }
    ~NativePostgresDriver() override { if (owns_connection_ && conn_) PQfinish(conn_); }
    const std::string &dialect() const override { return dialect_; }
    bool in_transaction() const override { return PQtransactionStatus(conn_) != PQTRANS_IDLE; }

    QueryResult query_result(const std::string &sql, const std::vector<Value> &params = {}) override {
        const std::string &pg_sql = pg_sql_cached(sql);
        // Branch-visible SELECT predicates are dominated by segment-specific
        // interval bounds. Using unnamed execution keeps PostgreSQL on a
        // value-specific custom plan instead of eventually switching a prepared
        // statement to a generic plan that scans too many interval rows.
        PgResult result = pg_exec_params(conn_, pg_sql, params);
        result.require(PGRES_TUPLES_OK);
        return query_result_from_pg(result.get());
    }

    std::vector<QueryResult> query_many(
        const std::vector<std::pair<std::string, std::vector<Value>>> &queries
    ) override {
        if (queries.empty()) return {};
        if (queries.size() == 1) {
            return {query_result(queries[0].first, queries[0].second)};
        }

        struct PendingQuery {
            std::string sql;
            PgParams params;
        };
        std::vector<PendingQuery> pending;
        pending.reserve(queries.size());
        for (const auto &[sql, params] : queries) {
            pending.push_back({pg_sql_cached(sql), make_pg_params(params)});
        }

        record_sql_profile_pipeline(static_cast<std::int64_t>(pending.size()));
        const auto pipeline_start = std::chrono::steady_clock::now();
        if (PQenterPipelineMode(conn_) != 1) {
            throw PgError(PQerrorMessage(conn_));
        }
        bool pipeline_active = true;
        try {
            for (std::size_t i = 0; i < pending.size(); ++i) {
                const PendingQuery &query = pending[i];
                const int param_count = static_cast<int>(queries[i].second.size());
                if (PQsendQueryParams(
                        conn_,
                        query.sql.c_str(),
                        param_count,
                        nullptr,
                        query.params.values.empty() ? nullptr : query.params.values.data(),
                        query.params.lengths.empty() ? nullptr : query.params.lengths.data(),
                        query.params.formats.empty() ? nullptr : query.params.formats.data(),
                        0
                    ) != 1) {
                    throw PgError(PQerrorMessage(conn_));
                }
            }
            if (PQpipelineSync(conn_) != 1 || PQflush(conn_) == -1) {
                throw PgError(PQerrorMessage(conn_));
            }

            std::vector<QueryResult> results;
            results.reserve(queries.size());
            bool saw_sync = false;
            while (!saw_sync) {
                PGresult *raw = PQgetResult(conn_);
                if (raw == nullptr) {
                    continue;
                }
                PgResult result(conn_, raw);
                const ExecStatusType status = PQresultStatus(result.get());
                if (status == PGRES_PIPELINE_SYNC) {
                    saw_sync = true;
                    break;
                }
                if (status == PGRES_TUPLES_OK) {
                    results.push_back(query_result_from_pg(result.get()));
                    continue;
                }
                result.require(PGRES_TUPLES_OK);
            }
            if (PQexitPipelineMode(conn_) != 1) {
                throw PgError(PQerrorMessage(conn_));
            }
            pipeline_active = false;
            if (results.size() != queries.size()) {
                throw PgError("PostgreSQL pipeline returned an unexpected result count");
            }
            const double pipeline_elapsed_ms = elapsed_ms_since(pipeline_start);
            record_sql_trace_statement(
                "PIPELINE_SYNC statements=" + std::to_string(queries.size()),
                "pipeline_batch",
                tls_sql_profile_bucket,
                pipeline_elapsed_ms,
                static_cast<std::int64_t>(results.size()),
                static_cast<std::int64_t>(queries.size())
            );
            const double allocated_ms = queries.empty()
                ? 0.0
                : pipeline_elapsed_ms / static_cast<double>(queries.size());
            for (const auto &[sql, params] : queries) {
                record_sql_trace_statement(
                    pg_sql_cached(sql),
                    "pipeline_statement",
                    tls_sql_profile_bucket,
                    allocated_ms,
                    -1,
                    static_cast<std::int64_t>(params.size())
                );
            }
            return results;
        } catch (...) {
            while (PGresult *raw = PQgetResult(conn_)) {
                PQclear(raw);
            }
            if (pipeline_active) {
                PQexitPipelineMode(conn_);
            }
            throw;
        }
    }

    static QueryResult query_result_from_pg(PGresult *result) {
        QueryResult out;
        int nrows = PQntuples(result);
        int ncols = PQnfields(result);
        out.columns.reserve(static_cast<std::size_t>(ncols));
        for (int c = 0; c < ncols; ++c) {
            out.columns.emplace_back(PQfname(result, c));
        }
        out.rows.reserve(static_cast<std::size_t>(nrows));
        for (int r = 0; r < nrows; ++r) {
            std::vector<Value> row;
            row.reserve(static_cast<std::size_t>(ncols));
            for (int c = 0; c < ncols; ++c) row.push_back(pg_branch_value(result, r, c));
            out.rows.push_back(std::move(row));
        }
        return out;
    }

    void execute(const std::string &sql, const std::vector<Value> &params = {}) override {
        const std::string &pg_sql = pg_sql_cached(sql);
        PgResult result = pg_can_prepare_statement(pg_sql)
            ? statements_.exec(conn_, pg_sql, params)
            : pg_exec_params(conn_, pg_sql, params);
        result.require_query_or_command();
    }

    std::int64_t execute_changes(const std::string &sql, const std::vector<Value> &params = {}) override {
        const std::string &pg_sql = pg_sql_cached(sql);
        PgResult result = pg_can_prepare_statement(pg_sql)
            ? statements_.exec(conn_, pg_sql, params)
            : pg_exec_params(conn_, pg_sql, params);
        result.require_query_or_command();
        const char *tuples = PQcmdTuples(result.get());
        if (!tuples || !*tuples) return 0;
        return std::strtoll(tuples, nullptr, 10);
    }

    BulkUpsertResult interval_upsert(
        const std::string &physical_name,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        const std::string &live_lo,
        const std::string &live_hi,
        std::int64_t writer_segment_id,
        bool replacement_deleted,
        bool manage_transaction,
        IntervalWriteMode write_mode,
        const std::string &branch_point
    ) override {
        return postgres_adapter_bulk_upsert(
            conn_, physical_name, columns, pk_columns, rows,
            live_lo, live_hi, writer_segment_id, replacement_deleted, manage_transaction,
            &interval_upsert_statements_, write_mode, branch_point
        );
    }

  private:
    const std::string &pg_sql_cached(const std::string &qmark_sql) {
        auto found = pg_sql_cache_.find(qmark_sql);
        if (found != pg_sql_cache_.end()) return found->second;
        if (pg_sql_cache_.size() > 512) pg_sql_cache_.clear();
        auto inserted = pg_sql_cache_.emplace(qmark_sql, pg_placeholder_sql(qmark_sql));
        return inserted.first->second;
    }
    std::string dialect_;
    PGconn *conn_ = nullptr;
    bool owns_connection_ = false;
    std::unordered_map<std::string, std::string> pg_sql_cache_;
    PgPreparedStatementCache statements_;
    PgPreparedStatementCache interval_upsert_statements_;
};

std::unique_ptr<NativeSqlDriver> open_native_sql_driver(const std::string &database_url) {
    if (database_url.rfind("postgresql://", 0) == 0 || database_url.rfind("postgres://", 0) == 0) {
        return std::make_unique<NativePostgresDriver>(database_url);
    }
    if (database_url.rfind("duckdb://", 0) == 0 || database_url.rfind("duckdb:", 0) == 0) {
        return std::make_unique<NativeDuckDBDriver>(database_url);
    }
    return std::make_unique<NativeSQLiteDriver>(database_url);
}

// -----------------------------------------------------------------------------
// Native SQL parser/planner
// Source: interval_sql_planner.cpp
// -----------------------------------------------------------------------------
struct NativeBranchSegment {
    std::int64_t segment_id = 0;
    std::string live_lo;
    std::string live_hi;
    std::string branch_point;
    std::string branch_kind;
};

struct NativeTableMeta {
    std::string logical_name;
    std::string physical_name;
    std::vector<std::string> columns;
    std::vector<std::string> pk_columns;
    std::vector<std::string> column_defs;
    std::string schema_version_id;
    std::string ddl_op;
    bool has_schema_binding = false;
};

struct NativeDirectMergeSegment {
    std::int64_t segment_id = 0;
    std::int64_t parent_segment_id = 0;
    std::string kind;
    std::string live_lo;
    std::string live_hi;
    std::string branch_point;
};

struct NativeMergeRowState {
    bool present = false;
    std::vector<Value> row;
};

struct NativeMergePlan {
    NativeDirectMergeSegment source;
    NativeDirectMergeSegment target;
    NativeDirectMergeSegment base;
    std::vector<std::int64_t> source_writer_segments;
    std::vector<std::int64_t> target_writer_segments;
};

std::string trim_copy(const std::string &input) {
    const auto first = input.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    const auto last = input.find_last_not_of(" \t\r\n");
    return input.substr(first, last - first + 1);
}

std::string upper_copy(std::string input) {
    std::transform(input.begin(), input.end(), input.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return input;
}

std::vector<std::string> parse_json_string_array(const std::string &json) {
    std::vector<std::string> out;
    bool in_string = false;
    bool escape = false;
    std::string current;
    for (char ch : json) {
        if (!in_string) {
            if (ch == '"') {
                in_string = true;
                current.clear();
            }
            continue;
        }
        if (escape) {
            switch (ch) {
            case '"': current.push_back('"'); break;
            case '\\': current.push_back('\\'); break;
            case '/': current.push_back('/'); break;
            case 'b': current.push_back('\b'); break;
            case 'f': current.push_back('\f'); break;
            case 'n': current.push_back('\n'); break;
            case 'r': current.push_back('\r'); break;
            case 't': current.push_back('\t'); break;
            default: current.push_back(ch); break;
            }
            escape = false;
            continue;
        }
        if (ch == '\\') {
            escape = true;
            continue;
        }
        if (ch == '"') {
            in_string = false;
            out.push_back(current);
            continue;
        }
        current.push_back(ch);
    }
    return out;
}

std::string unquote_ident_copy(std::string ident) {
    ident = trim_copy(ident);
    if (ident.size() >= 2 && ident.front() == '"' && ident.back() == '"') {
        std::string out;
        for (std::size_t i = 1; i + 1 < ident.size(); ++i) {
            if (ident[i] == '"' && i + 2 < ident.size() && ident[i + 1] == '"') {
                out.push_back('"');
                ++i;
            } else {
                out.push_back(ident[i]);
            }
        }
        return out;
    }
    return ident;
}

std::string visible_where_sql(const std::string &branch_point) {
    return "live_lo <= " + branch_point + " AND " + branch_point + " < live_hi AND deleted = FALSE";
}

std::string visible_select_where_sql(const std::string &branch_point, const std::string &dialect) {
    std::string predicate = visible_where_sql(branch_point);
    if (dialect == "duckdb") {
        // DuckDB treats the raw interval predicate as a scan-local filter, which
        // prevents dynamic filters on user columns from reaching large fact
        // table scans. Keeping the visibility check in a vectorized scalar
        // expression preserves those pushed join filters while retaining the
        // same three-valued SQL semantics as a WHERE predicate.
        return "if(" + predicate + ", TRUE, FALSE)";
    }
    return predicate;
}

std::string node_string_value(const PgQuery__Node *node) {
    if (!node || node->node_case != PG_QUERY__NODE__NODE_STRING || !node->string) {
        throw std::runtime_error("expected string node");
    }
    return node->string->sval ? node->string->sval : "";
}

std::string column_ref_name(const PgQuery__ColumnRef *ref) {
    if (!ref || ref->n_fields == 0) throw std::runtime_error("expected column reference");
    return node_string_value(ref->fields[ref->n_fields - 1]);
}

std::string relation_name(const PgQuery__RangeVar *relation) {
    if (!relation || !relation->relname || std::string(relation->relname).empty()) {
        throw std::runtime_error("statement does not target a table");
    }
    if (relation->schemaname && std::string(relation->schemaname).size() > 0) {
        throw std::runtime_error("schema-qualified table names are not supported by native branch DML");
    }
    return relation->relname;
}

std::string operator_name(const PgQuery__AExpr *expr) {
    if (!expr || expr->n_name == 0) throw std::runtime_error("expected operator expression");
    return node_string_value(expr->name[expr->n_name - 1]);
}

double value_as_double(const Value &value) {
    if (auto ptr = std::get_if<std::int64_t>(&value)) return static_cast<double>(*ptr);
    if (auto ptr = std::get_if<double>(&value)) return *ptr;
    if (auto ptr = std::get_if<std::string>(&value)) return std::stod(*ptr);
    if (auto ptr = std::get_if<DecimalValue>(&value)) return std::stod(ptr->text);
    throw std::runtime_error("expected numeric SQL value");
}

bool value_truthy(const Value &value) {
    if (std::holds_alternative<std::monostate>(value)) return false;
    if (auto ptr = std::get_if<std::int64_t>(&value)) return *ptr != 0;
    if (auto ptr = std::get_if<double>(&value)) return *ptr != 0.0;
    if (auto ptr = std::get_if<std::string>(&value)) return !ptr->empty();
    if (auto ptr = std::get_if<Blob>(&value)) return !ptr->empty();
    if (auto ptr = std::get_if<DecimalValue>(&value)) return ptr->text != "0";
    if (std::holds_alternative<DateValue>(value)) return true;
    return false;
}

std::string current_timestamp_string() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t tt = std::chrono::system_clock::to_time_t(now);
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()
    ).count() % 1000000;
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &tt);
#else
    gmtime_r(&tt, &tm);
#endif
    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S")
        << "." << std::setw(6) << std::setfill('0') << micros
        << "+00:00";
    return out.str();
}

std::string hex_u64(std::uint64_t value, std::size_t width = 0) {
    std::ostringstream out;
    out << std::hex << std::nouppercase;
    if (width) out << std::setw(static_cast<int>(width)) << std::setfill('0');
    out << value;
    return out.str();
}

std::uint64_t stable_fnv1a64(const std::string &value) {
    std::uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : value) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string identifier_token(const std::string &value) {
    std::string token;
    token.reserve(value.size());
    for (char ch : value) {
        if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
            (ch >= '0' && ch <= '9') || ch == '_') {
            token.push_back(ch);
        } else if (!token.empty() && token.back() != '_') {
            token.push_back('_');
        }
    }
    while (!token.empty() && token.front() == '_') token.erase(token.begin());
    while (!token.empty() && token.back() == '_') token.pop_back();
    if (token.empty()) token = "value";
    if (token.size() > 48) token.resize(48);
    return token + "_" + hex_u64(stable_fnv1a64(value), 10).substr(0, 10);
}

std::string physical_table_suffix(const std::string &table) {
    if (table.empty()) return identifier_token(table);
    const auto first = static_cast<unsigned char>(table.front());
    if (!(std::isalpha(first) || first == '_')) return identifier_token(table);
    for (unsigned char ch : table) {
        if (!(std::isalnum(ch) || ch == '_')) return identifier_token(table);
    }
    return table;
}

std::string json_escape(const std::string &value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char ch : value) {
        switch (ch) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\b': out += "\\b"; break;
        case '\f': out += "\\f"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default: out.push_back(ch); break;
        }
    }
    return out;
}

std::string json_string_array(const std::vector<std::string> &values) {
    std::string out = "[";
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i) out += ", ";
        out += "\"" + json_escape(values[i]) + "\"";
    }
    out += "]";
    return out;
}

std::string sql_string_literal(const std::string &value) {
    std::string out = "'";
    for (char ch : value) {
        if (ch == '\'') out += "''";
        else out.push_back(ch);
    }
    out += "'";
    return out;
}

std::string node_string_name(const PgQuery__Node *node) {
    if (!node || node->node_case != PG_QUERY__NODE__NODE_STRING || !node->string) {
        throw std::runtime_error("expected identifier string");
    }
    return node->string->sval ? node->string->sval : "";
}

std::vector<std::string> node_string_list(std::size_t count, PgQuery__Node **nodes) {
    std::vector<std::string> out;
    out.reserve(count);
    for (std::size_t i = 0; i < count; ++i) out.push_back(node_string_name(nodes[i]));
    return out;
}

std::string join_strings(const std::vector<std::string> &values, const std::string &separator) {
    std::string out;
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i) out += separator;
        out += values[i];
    }
    return out;
}

std::string type_name_sql(const PgQuery__TypeName *type_name) {
    if (!type_name || type_name->n_names == 0) return "TEXT";
    std::vector<std::string> names = node_string_list(type_name->n_names, type_name->names);
    std::string base = names.back();
    std::string upper;
    if (base == "int4") upper = "INTEGER";
    else if (base == "int8") upper = "BIGINT";
    else if (base == "text") upper = "TEXT";
    else if (base == "bool") upper = "BOOLEAN";
    else if (base == "varchar") upper = "VARCHAR";
    else if (base == "numeric") upper = "NUMERIC";
    else upper = upper_copy(base);

    if (type_name->n_typmods > 0) {
        std::vector<std::string> mods;
        mods.reserve(type_name->n_typmods);
        for (std::size_t i = 0; i < type_name->n_typmods; ++i) {
            PgQuery__Node *node = type_name->typmods[i];
            if (!node || node->node_case != PG_QUERY__NODE__NODE_A_CONST || !node->a_const) {
                throw std::runtime_error("unsupported type modifier");
            }
            auto *constant = node->a_const;
            if (constant->val_case == PG_QUERY__A__CONST__VAL_IVAL) {
                mods.push_back(std::to_string(constant->ival->ival));
            } else if (constant->val_case == PG_QUERY__A__CONST__VAL_SVAL) {
                mods.push_back(constant->sval->sval ? constant->sval->sval : "");
            } else {
                throw std::runtime_error("unsupported type modifier");
            }
        }
        upper += "(" + join_strings(mods, ", ") + ")";
    }
    return upper;
}

std::string expression_sql(const PgQuery__Node *node) {
    if (!node) return "NULL";
    switch (node->node_case) {
    case PG_QUERY__NODE__NODE_A_CONST: {
        auto *c = node->a_const;
        if (c->isnull) return "NULL";
        switch (c->val_case) {
        case PG_QUERY__A__CONST__VAL_IVAL:
            return std::to_string(c->ival->ival);
        case PG_QUERY__A__CONST__VAL_FVAL:
            return c->fval->fval ? c->fval->fval : "0";
        case PG_QUERY__A__CONST__VAL_BOOLVAL:
            return c->boolval->boolval ? "TRUE" : "FALSE";
        case PG_QUERY__A__CONST__VAL_SVAL:
            return sql_string_literal(c->sval->sval ? c->sval->sval : "");
        default:
            throw std::runtime_error("unsupported SQL constant");
        }
    }
    case PG_QUERY__NODE__NODE_COLUMN_REF:
        return quote_ident(column_ref_name(node->column_ref));
    case PG_QUERY__NODE__NODE_A_EXPR: {
        auto *expr = node->a_expr;
        const std::string op = operator_name(expr);
        return "(" + expression_sql(expr->lexpr) + " " + op + " " + expression_sql(expr->rexpr) + ")";
    }
    case PG_QUERY__NODE__NODE_TYPE_CAST:
        return expression_sql(node->type_cast->arg) + "::" + type_name_sql(node->type_cast->type_name);
    default:
        throw std::runtime_error("unsupported SQL expression in schema DDL");
    }
}

std::atomic<std::uint64_t> native_schema_id_counter{1};

std::string unique_schema_suffix() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto counter = native_schema_id_counter.fetch_add(1, std::memory_order_relaxed);
    return hex_u64(static_cast<std::uint64_t>(now) ^ (counter * 0x9e3779b97f4a7c15ULL), 12).substr(0, 12);
}

int compare_values(const Value &left, const Value &right) {
    const bool left_numeric = std::holds_alternative<std::int64_t>(left) ||
        std::holds_alternative<double>(left) ||
        std::holds_alternative<DecimalValue>(left);
    const bool right_numeric = std::holds_alternative<std::int64_t>(right) ||
        std::holds_alternative<double>(right) ||
        std::holds_alternative<DecimalValue>(right);
    if (left_numeric && right_numeric) {
        const double l = value_as_double(left);
        const double r = value_as_double(right);
        return l < r ? -1 : (l > r ? 1 : 0);
    }
    const std::string l = native_as_string(left);
    const std::string r = native_as_string(right);
    return l < r ? -1 : (l > r ? 1 : 0);
}

std::unordered_map<std::string, Value> row_map_for(
    const std::vector<std::string> &columns,
    const std::vector<Value> &row
) {
    std::unordered_map<std::string, Value> out;
    for (std::size_t i = 0; i < columns.size() && i < row.size(); ++i) {
        out.emplace(columns[i], row[i]);
    }
    return out;
}

using SubqueryEvaluator = std::function<std::vector<Value>(const PgQuery__SelectStmt *)>;

Value eval_ast_value(
    const PgQuery__Node *node,
    const std::unordered_map<std::string, Value> &row,
    const std::vector<Value> &params,
    const SubqueryEvaluator *subquery_evaluator = nullptr
);

bool is_row_constructor_node(const PgQuery__Node *node) {
    return node &&
        (node->node_case == PG_QUERY__NODE__NODE_ROW_EXPR ||
         node->node_case == PG_QUERY__NODE__NODE_LIST);
}

std::vector<Value> eval_ast_row_values(
    const PgQuery__Node *node,
    const std::unordered_map<std::string, Value> &row,
    const std::vector<Value> &params,
    const SubqueryEvaluator *subquery_evaluator = nullptr
) {
    if (!node) return {};
    std::vector<Value> values;
    if (node->node_case == PG_QUERY__NODE__NODE_ROW_EXPR) {
        auto *expr = node->row_expr;
        if (!expr) return values;
        values.reserve(expr->n_args);
        for (std::size_t i = 0; i < expr->n_args; ++i) {
            values.push_back(eval_ast_value(expr->args[i], row, params, subquery_evaluator));
        }
        return values;
    }
    if (node->node_case == PG_QUERY__NODE__NODE_LIST) {
        auto *list = node->list;
        if (!list) return values;
        values.reserve(list->n_items);
        for (std::size_t i = 0; i < list->n_items; ++i) {
            values.push_back(eval_ast_value(list->items[i], row, params, subquery_evaluator));
        }
        return values;
    }
    values.push_back(eval_ast_value(node, row, params, subquery_evaluator));
    return values;
}

int compare_row_values(const std::vector<Value> &left, const std::vector<Value> &right) {
    if (left.size() != right.size()) {
        throw std::runtime_error("row comparison arity mismatch in native branch executor");
    }
    for (std::size_t i = 0; i < left.size(); ++i) {
        const int cmp = compare_values(left[i], right[i]);
        if (cmp != 0) return cmp;
    }
    return 0;
}

bool eval_ast_predicate(
    const PgQuery__Node *node,
    const std::unordered_map<std::string, Value> &row,
    const std::vector<Value> &params,
    const SubqueryEvaluator *subquery_evaluator = nullptr
) {
    if (!node) return true;
    switch (node->node_case) {
    case PG_QUERY__NODE__NODE_BOOL_EXPR: {
        auto *expr = node->bool_expr;
        if (expr->boolop == PG_QUERY__BOOL_EXPR_TYPE__AND_EXPR) {
            for (std::size_t i = 0; i < expr->n_args; ++i) {
                if (!eval_ast_predicate(expr->args[i], row, params, subquery_evaluator)) return false;
            }
            return true;
        }
        if (expr->boolop == PG_QUERY__BOOL_EXPR_TYPE__OR_EXPR) {
            for (std::size_t i = 0; i < expr->n_args; ++i) {
                if (eval_ast_predicate(expr->args[i], row, params, subquery_evaluator)) return true;
            }
            return false;
        }
        if (expr->boolop == PG_QUERY__BOOL_EXPR_TYPE__NOT_EXPR) {
            if (expr->n_args != 1) throw std::runtime_error("invalid NOT expression");
            return !eval_ast_predicate(expr->args[0], row, params, subquery_evaluator);
        }
        break;
    }
    case PG_QUERY__NODE__NODE_A_EXPR: {
        auto *expr = node->a_expr;
        const std::string op = operator_name(expr);
        if (upper_copy(op) == "LIKE") {
            const std::string text = native_as_string(eval_ast_value(expr->lexpr, row, params, subquery_evaluator));
            const std::string pattern = native_as_string(eval_ast_value(expr->rexpr, row, params, subquery_evaluator));
            if (pattern.size() >= 1 && pattern.back() == '%' && pattern.find('%') == pattern.size() - 1) {
                return text.rfind(pattern.substr(0, pattern.size() - 1), 0) == 0;
            }
            return text == pattern;
        }
        int cmp = 0;
        if (is_row_constructor_node(expr->lexpr) || is_row_constructor_node(expr->rexpr)) {
            // PostgreSQL tuple predicates are lexicographic. db-fork uses this
            // form for composite primary-key ranges, so keep it in the generic
            // evaluator instead of adding range-specific executor code.
            cmp = compare_row_values(
                eval_ast_row_values(expr->lexpr, row, params, subquery_evaluator),
                eval_ast_row_values(expr->rexpr, row, params, subquery_evaluator)
            );
        } else {
            const Value left = eval_ast_value(expr->lexpr, row, params, subquery_evaluator);
            const Value right = eval_ast_value(expr->rexpr, row, params, subquery_evaluator);
            cmp = compare_values(left, right);
        }
        if (op == "=") return cmp == 0;
        if (op == "<>" || op == "!=") return cmp != 0;
        if (op == "<") return cmp < 0;
        if (op == "<=") return cmp <= 0;
        if (op == ">") return cmp > 0;
        if (op == ">=") return cmp >= 0;
        throw std::runtime_error("unsupported predicate operator: " + op);
    }
    case PG_QUERY__NODE__NODE_SUB_LINK: {
        auto *link = node->sub_link;
        if (!subquery_evaluator || link->sub_link_type != PG_QUERY__SUB_LINK_TYPE__ANY_SUBLINK ||
            !link->testexpr || !link->subselect ||
            link->subselect->node_case != PG_QUERY__NODE__NODE_SELECT_STMT) {
            throw std::runtime_error("unsupported subquery predicate in native branch executor");
        }
        const Value test = eval_ast_value(link->testexpr, row, params, subquery_evaluator);
        for (const auto &candidate : (*subquery_evaluator)(link->subselect->select_stmt)) {
            if (compare_values(test, candidate) == 0) return true;
        }
        return false;
    }
    default:
        return value_truthy(eval_ast_value(node, row, params, subquery_evaluator));
    }
    throw std::runtime_error("unsupported boolean expression");
}

Value eval_ast_value(
    const PgQuery__Node *node,
    const std::unordered_map<std::string, Value> &row,
    const std::vector<Value> &params,
    const SubqueryEvaluator *subquery_evaluator
) {
    if (!node) return std::monostate{};
    switch (node->node_case) {
    case PG_QUERY__NODE__NODE_PARAM_REF: {
        int index = node->param_ref->number - 1;
        if (index < 0 || static_cast<std::size_t>(index) >= params.size()) {
            throw std::runtime_error("missing SQL parameter");
        }
        return params[static_cast<std::size_t>(index)];
    }
    case PG_QUERY__NODE__NODE_A_CONST: {
        auto *c = node->a_const;
        if (c->isnull) return std::monostate{};
        switch (c->val_case) {
        case PG_QUERY__A__CONST__VAL_IVAL:
            return static_cast<std::int64_t>(c->ival->ival);
        case PG_QUERY__A__CONST__VAL_FVAL:
            return std::stod(c->fval->fval ? c->fval->fval : "0");
        case PG_QUERY__A__CONST__VAL_BOOLVAL:
            return static_cast<std::int64_t>(c->boolval->boolval ? 1 : 0);
        case PG_QUERY__A__CONST__VAL_SVAL:
            return std::string(c->sval->sval ? c->sval->sval : "");
        default:
            throw std::runtime_error("unsupported SQL constant");
        }
    }
    case PG_QUERY__NODE__NODE_COLUMN_REF: {
        const auto name = column_ref_name(node->column_ref);
        auto found = row.find(name);
        if (found == row.end()) throw std::runtime_error("unknown column in expression: " + name);
        return found->second;
    }
    case PG_QUERY__NODE__NODE_A_EXPR: {
        auto *expr = node->a_expr;
        const std::string op = operator_name(expr);
        if (op == "+" || op == "-" || op == "*" || op == "/") {
            const Value left = eval_ast_value(expr->lexpr, row, params, subquery_evaluator);
            const Value right = eval_ast_value(expr->rexpr, row, params, subquery_evaluator);
            const double l = value_as_double(left);
            const double r = value_as_double(right);
            const bool integer_result = std::holds_alternative<std::int64_t>(left) &&
                std::holds_alternative<std::int64_t>(right) && op != "/";
            double result = 0.0;
            if (op == "+") result = l + r;
            else if (op == "-") result = l - r;
            else if (op == "*") result = l * r;
            else result = l / r;
            if (integer_result) return static_cast<std::int64_t>(result);
            return result;
        }
        return static_cast<std::int64_t>(eval_ast_predicate(node, row, params, subquery_evaluator) ? 1 : 0);
    }
    case PG_QUERY__NODE__NODE_CASE_EXPR: {
        auto *expr = node->case_expr;
        if (!expr) return std::monostate{};
        for (std::size_t i = 0; i < expr->n_args; ++i) {
            auto *arg = expr->args[i];
            if (!arg || arg->node_case != PG_QUERY__NODE__NODE_CASE_WHEN || !arg->case_when) {
                throw std::runtime_error("unsupported CASE expression in native branch executor");
            }
            if (eval_ast_predicate(arg->case_when->expr, row, params, subquery_evaluator)) {
                return eval_ast_value(arg->case_when->result, row, params, subquery_evaluator);
            }
        }
        return eval_ast_value(expr->defresult, row, params, subquery_evaluator);
    }
    case PG_QUERY__NODE__NODE_COALESCE_EXPR: {
        auto *expr = node->coalesce_expr;
        if (!expr) return std::monostate{};
        for (std::size_t i = 0; i < expr->n_args; ++i) {
            Value value = eval_ast_value(expr->args[i], row, params, subquery_evaluator);
            if (!std::holds_alternative<std::monostate>(value)) return value;
        }
        return std::monostate{};
    }
    case PG_QUERY__NODE__NODE_SQLVALUE_FUNCTION: {
        auto *expr = node->sqlvalue_function;
        if (!expr) return std::monostate{};
        switch (expr->op) {
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIMESTAMP:
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIMESTAMP_N:
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIMESTAMP:
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIMESTAMP_N:
            return current_timestamp_string();
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_DATE:
            return current_timestamp_string().substr(0, 10);
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIME:
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIME_N:
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIME:
        case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIME_N:
            return current_timestamp_string().substr(11);
        default:
            throw std::runtime_error("unsupported SQL value function in native branch executor");
        }
    }
    default:
        throw std::runtime_error("unsupported SQL expression in native branch executor");
    }
}

bool extract_key_equality_atom(
    const PgQuery__Node *node,
    const std::vector<std::string> &pk_columns,
    const std::vector<Value> &params,
    std::unordered_map<std::string, Value> &out
) {
    if (!node || node->node_case != PG_QUERY__NODE__NODE_A_EXPR || !node->a_expr) return false;
    auto *expr = node->a_expr;
    if (operator_name(expr) != "=") return false;

    const PgQuery__Node *column_node = nullptr;
    const PgQuery__Node *value_node = nullptr;
    if (expr->lexpr && expr->lexpr->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
        column_node = expr->lexpr;
        value_node = expr->rexpr;
    } else if (expr->rexpr && expr->rexpr->node_case == PG_QUERY__NODE__NODE_COLUMN_REF) {
        column_node = expr->rexpr;
        value_node = expr->lexpr;
    } else {
        return false;
    }
    const std::string column = column_ref_name(column_node->column_ref);
    if (std::find(pk_columns.begin(), pk_columns.end(), column) == pk_columns.end()) return false;

    try {
        Value value = eval_ast_value(value_node, {}, params, nullptr);
        auto found = out.find(column);
        if (found != out.end()) {
            return found->second == value;
        }
        out.emplace(column, std::move(value));
        return true;
    } catch (...) {
        return false;
    }
}

bool extract_key_equality_conjunction(
    const PgQuery__Node *node,
    const std::vector<std::string> &pk_columns,
    const std::vector<Value> &params,
    std::unordered_map<std::string, Value> &out
) {
    if (!node) return false;
    if (node->node_case == PG_QUERY__NODE__NODE_BOOL_EXPR && node->bool_expr &&
        node->bool_expr->boolop == PG_QUERY__BOOL_EXPR_TYPE__AND_EXPR) {
        for (std::size_t i = 0; i < node->bool_expr->n_args; ++i) {
            if (!extract_key_equality_conjunction(node->bool_expr->args[i], pk_columns, params, out)) {
                return false;
            }
        }
        return true;
    }
    return extract_key_equality_atom(node, pk_columns, params, out);
}

std::optional<std::vector<Value>> point_key_values_from_predicate(
    const PgQuery__Node *where,
    const NativeTableMeta &meta,
    const std::vector<Value> &params
) {
    if (!where || meta.pk_columns.empty()) return std::nullopt;
    std::unordered_map<std::string, Value> by_column;
    by_column.reserve(meta.pk_columns.size());
    if (!extract_key_equality_conjunction(where, meta.pk_columns, params, by_column)) {
        return std::nullopt;
    }
    std::vector<Value> values;
    values.reserve(meta.pk_columns.size());
    for (const auto &pk : meta.pk_columns) {
        auto found = by_column.find(pk);
        if (found == by_column.end()) return std::nullopt;
        values.push_back(found->second);
    }
    return values;
}

void collect_key_equality_conjunction(
    const PgQuery__Node *node,
    const std::vector<std::string> &pk_columns,
    const std::vector<Value> &params,
    std::unordered_map<std::string, Value> &out
) {
    if (!node) return;
    if (node->node_case == PG_QUERY__NODE__NODE_BOOL_EXPR && node->bool_expr &&
        node->bool_expr->boolop == PG_QUERY__BOOL_EXPR_TYPE__AND_EXPR) {
        for (std::size_t i = 0; i < node->bool_expr->n_args; ++i) {
            collect_key_equality_conjunction(node->bool_expr->args[i], pk_columns, params, out);
        }
        return;
    }
    try {
        (void)extract_key_equality_atom(node, pk_columns, params, out);
    } catch (...) {
    }
}

std::optional<NativeKeyPrefixFilter> leading_key_prefix_from_predicate(
    const PgQuery__Node *where,
    const NativeTableMeta &meta,
    const std::vector<Value> &params
) {
    if (!where || meta.pk_columns.empty()) return std::nullopt;
    std::unordered_map<std::string, Value> by_column;
    by_column.reserve(meta.pk_columns.size());
    collect_key_equality_conjunction(where, meta.pk_columns, params, by_column);

    NativeKeyPrefixFilter prefix;
    for (const auto &pk : meta.pk_columns) {
        auto found = by_column.find(pk);
        if (found == by_column.end()) break;
        prefix.columns.push_back(pk);
        prefix.values.push_back(found->second);
    }
    if (prefix.columns.empty()) return std::nullopt;
    return prefix;
}

std::optional<NativeKeyPrefixFilter> exact_leading_key_prefix_from_predicate(
    const PgQuery__Node *where,
    const NativeTableMeta &meta,
    const std::vector<Value> &params
) {
    if (!where || meta.pk_columns.empty()) return std::nullopt;
    std::unordered_map<std::string, Value> by_column;
    by_column.reserve(meta.pk_columns.size());
    if (!extract_key_equality_conjunction(where, meta.pk_columns, params, by_column)) {
        return std::nullopt;
    }

    NativeKeyPrefixFilter prefix;
    for (const auto &pk : meta.pk_columns) {
        auto found = by_column.find(pk);
        if (found == by_column.end()) break;
        prefix.columns.push_back(pk);
        prefix.values.push_back(found->second);
    }
    if (prefix.columns.empty() || prefix.columns.size() != by_column.size()) {
        return std::nullopt;
    }
    return prefix;
}

std::unordered_map<std::string, Value> column_default_values(const NativeTableMeta &meta) {
    std::unordered_map<std::string, Value> defaults;
    for (std::size_t i = 0; i < meta.columns.size() && i < meta.column_defs.size(); ++i) {
        const std::string &definition = meta.column_defs[i];
        std::string upper_definition = upper_copy(definition);
        const std::size_t pos = upper_definition.find(" DEFAULT ");
        if (pos == std::string::npos) continue;
        const std::string default_sql = trim_copy(definition.substr(pos + 9));
        if (default_sql.empty()) continue;
        try {
            PgProtobufParseResult parsed("SELECT " + default_sql);
            PgQuery__Node *stmt = parsed.single_statement();
            if (!stmt || stmt->node_case != PG_QUERY__NODE__NODE_SELECT_STMT) continue;
            auto *select = stmt->select_stmt;
            if (!select || select->n_target_list != 1) continue;
            auto *target = select->target_list[0];
            if (!target || target->node_case != PG_QUERY__NODE__NODE_RES_TARGET || !target->res_target->val) continue;
            defaults.emplace(
                meta.columns[i],
                eval_ast_value(target->res_target->val, {}, {}, nullptr)
            );
        } catch (...) {
        }
    }
    return defaults;
}

std::string physical_interval_table(const std::string &logical_table) {
    return "_chronos_b_interval_" + logical_table;
}

std::string select_columns_sql(const std::vector<std::string> &columns) {
    return comma_join_quoted(columns);
}

} // namespace chronos::native::detail

namespace chronos::native {

void set_sql_profile_enabled(bool enabled) {
    detail::set_sql_profile_enabled_impl(enabled);
}

void reset_sql_profile() {
    detail::reset_sql_profile_impl();
}

NativeSqlProfile snapshot_sql_profile() {
    return detail::snapshot_sql_profile_impl();
}

void set_sql_trace_enabled(bool enabled) {
    detail::set_sql_trace_enabled_impl(enabled);
}

void reset_sql_trace() {
    detail::reset_sql_trace_impl();
}

std::vector<NativeSqlTraceEntry> snapshot_sql_trace() {
    return detail::snapshot_sql_trace_impl();
}

} // namespace chronos::native
