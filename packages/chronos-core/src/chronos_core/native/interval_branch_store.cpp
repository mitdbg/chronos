namespace chronos::native {

using namespace detail;

class NativeSqlConnectionImpl {
  public:
    explicit NativeSqlConnectionImpl(const std::string &database_url)
        : driver_(open_native_sql_driver(database_url)) {}

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
    void refresh_catalog() {
        driver_->refresh_catalog();
    }
    std::vector<std::vector<Value>> query_sql(const std::string &sql, const std::vector<Value> &params) {
        return driver_->query(sql, params);
    }
    QueryResult query_sql_result(const std::string &sql, const std::vector<Value> &params) {
        return driver_->query_result(sql, params);
    }
    void execute_sql(const std::string &sql, const std::vector<Value> &params) {
        driver_->execute(sql, params);
    }
    NativeSqlDriver &driver() { return *driver_; }

  private:
    std::unique_ptr<NativeSqlDriver> driver_;
};

class NativeBranchStoreImpl {
  public:
    explicit NativeBranchStoreImpl(const std::string &database_url)
        : metadata_driver_(open_native_sql_driver(database_url)), driver_(metadata_driver_.get()) {
        if (driver_->dialect() == "duckdb") {
            throw std::invalid_argument(
                "DuckDB is a Chronos interval data plane; branch metadata must live in SQLite/PostgreSQL"
            );
        }
    }
    NativeBranchStoreImpl(const std::string &data_url, const std::string &metadata_url)
        : metadata_driver_(open_native_sql_driver(metadata_url)),
          data_driver_(open_native_sql_driver(data_url)),
          driver_(metadata_driver_.get()) {
        if (driver_->dialect() == "duckdb") {
            throw std::invalid_argument("split interval metadata store must be SQLite/PostgreSQL");
        }
    }
    NativeBranchStoreImpl(const std::string &data_url, sqlite3 *metadata_db)
        : metadata_driver_(std::make_unique<NativeSQLiteDriver>(metadata_db)),
          data_driver_(open_native_sql_driver(data_url)),
          driver_(metadata_driver_.get()) {
    }
    NativeBranchStoreImpl(const std::string &data_url, PGconn *metadata_conn)
        : metadata_driver_(std::make_unique<NativePostgresDriver>(metadata_conn)),
          data_driver_(open_native_sql_driver(data_url)),
          driver_(metadata_driver_.get()) {
    }
    NativeBranchStoreImpl(NativeSqlConnectionImpl &data_conn, const std::string &metadata_url)
        : metadata_driver_(open_native_sql_driver(metadata_url)),
          borrowed_data_driver_(&data_conn.driver()),
          driver_(metadata_driver_.get()) {
        if (driver_->dialect() == "duckdb") {
            throw std::invalid_argument("split interval metadata store must be SQLite/PostgreSQL");
        }
    }
    NativeBranchStoreImpl(NativeSqlConnectionImpl &data_conn, sqlite3 *metadata_db)
        : metadata_driver_(std::make_unique<NativeSQLiteDriver>(metadata_db)),
          borrowed_data_driver_(&data_conn.driver()),
          driver_(metadata_driver_.get()) {}
    NativeBranchStoreImpl(NativeSqlConnectionImpl &data_conn, PGconn *metadata_conn)
        : metadata_driver_(std::make_unique<NativePostgresDriver>(metadata_conn)),
          borrowed_data_driver_(&data_conn.driver()),
          driver_(metadata_driver_.get()) {}
    explicit NativeBranchStoreImpl(sqlite3 *db)
        : metadata_driver_(std::make_unique<NativeSQLiteDriver>(db)), driver_(metadata_driver_.get()) {}
    explicit NativeBranchStoreImpl(PGconn *conn)
        : metadata_driver_(std::make_unique<NativePostgresDriver>(conn)), driver_(metadata_driver_.get()) {}

    NativeSqlDriver *data_driver_ptr() const {
        if (data_driver_) return data_driver_.get();
        return borrowed_data_driver_;
    }
    NativeSqlDriver &data_driver() {
        NativeSqlDriver *driver = data_driver_ptr();
        return driver ? *driver : *driver_;
    }
    const std::string &dialect() const {
        NativeSqlDriver *driver = data_driver_ptr();
        return driver ? driver->dialect() : driver_->dialect();
    }
    const std::string &metadata_dialect() const { return driver_->dialect(); }
    NativeSqlDriver &driver() { return data_driver(); }
    NativeSqlDriver &metadata_driver() { return *driver_; }
    bool split_store() const { return data_driver_ptr() != nullptr; }
    void set_create_secondary_indexes(bool enabled) { create_secondary_indexes_ = enabled; }
    bool create_secondary_indexes() const { return create_secondary_indexes_; }
    void set_create_writer_segment_index(bool enabled) { create_writer_segment_index_ = enabled; }
    bool create_writer_segment_index() const { return create_writer_segment_index_; }
    bool in_transaction() const {
        NativeSqlDriver *data = data_driver_ptr();
        return driver_->in_transaction() || (data != nullptr && data->in_transaction());
    }
    void commit() {
        if (driver_->in_transaction()) driver_->execute("COMMIT");
        NativeSqlDriver *data = data_driver_ptr();
        if (data != nullptr && data->in_transaction()) data->execute("COMMIT");
    }
    void rollback() {
        if (driver_->in_transaction()) {
            try {
                driver_->execute("ROLLBACK");
            } catch (...) {
            }
        }
        NativeSqlDriver *data = data_driver_ptr();
        if (data != nullptr && data->in_transaction()) {
            try {
                data->execute("ROLLBACK");
            } catch (...) {
            }
        }
    }

    void defer_or_execute_schema_index_sqls(const std::vector<std::string> &sqls) {
        for (const auto &sql : sqls) {
            driver().execute(sql);
        }
    }

    void flush_deferred_schema_indexes() {}

    void clear_deferred_schema_indexes() {}

    CachedNativeStatement cached_statement_plan(
        const std::string &sql,
        const std::function<CachedNativeStatement()> &build
    ) {
        std::lock_guard<std::mutex> guard(statement_plan_cache_mutex_);
        auto found = statement_plan_cache_.find(sql);
        if (found != statement_plan_cache_.end()) return found->second;
        if (statement_plan_cache_.size() > 2048) statement_plan_cache_.clear();
        auto inserted = statement_plan_cache_.emplace(sql, build());
        return inserted.first->second;
    }

    void clear_statement_plan_cache() {
        std::lock_guard<std::mutex> guard(statement_plan_cache_mutex_);
        statement_plan_cache_.clear();
    }

    std::vector<std::vector<Value>> query_sql(const std::string &sql, const std::vector<Value> &params) {
        return sql_targets_metadata(sql) ? driver_->query(sql, params) : driver().query(sql, params);
    }
    QueryResult query_sql_result(const std::string &sql, const std::vector<Value> &params) {
        return sql_targets_metadata(sql)
            ? driver_->query_result(sql, params)
            : driver().query_result(sql, params);
    }
    void execute_sql(const std::string &sql, const std::vector<Value> &params) {
        if (sql_targets_metadata(sql)) {
            driver_->execute(sql, params);
        } else {
            driver().execute(sql, params);
        }
    }
    bool table_exists(const std::string &table_name) {
        NativeSqlDriver &target = (
            table_name.rfind("_chronos_branch_", 0) == 0 ? metadata_driver() : driver()
        );
        const std::string &target_dialect = target.dialect();
        if (target_dialect == "postgres") {
            auto rows = target.query("SELECT to_regclass(?) IS NOT NULL", {table_name});
            return !rows.empty() && native_as_int(rows[0][0]) != 0;
        }
        if (target_dialect == "duckdb") {
            auto rows = target.query(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
                {table_name}
            );
            return !rows.empty();
        }
        auto rows = target.query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            {table_name}
        );
        return !rows.empty();
    }

#include "interval_branch_store_impl.cpp"

  private:
    static bool sql_targets_metadata(const std::string &sql) {
        std::string lowered;
        lowered.reserve(sql.size());
        std::transform(sql.begin(), sql.end(), std::back_inserter(lowered), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return lowered.find("_chronos_branch_tables") != std::string::npos ||
            lowered.find("_chronos_branch_indexes") != std::string::npos ||
            lowered.find("_chronos_branch_interval_") != std::string::npos ||
            lowered.find("_chronos_branch_transaction_commits") != std::string::npos ||
            lowered.find("_chronos_branch_table_schema_versions") != std::string::npos ||
            lowered.find("_chronos_branch_table_bindings") != std::string::npos ||
            lowered.find("pg_advisory_") != std::string::npos;
    }

    std::unique_ptr<NativeSqlDriver> metadata_driver_;
    std::unique_ptr<NativeSqlDriver> data_driver_;
    NativeSqlDriver *borrowed_data_driver_ = nullptr;
    NativeSqlDriver *driver_ = nullptr;
    bool create_secondary_indexes_ = true;
    bool create_writer_segment_index_ = true;
    std::mutex statement_plan_cache_mutex_;
    std::unordered_map<std::string, CachedNativeStatement> statement_plan_cache_;
};
