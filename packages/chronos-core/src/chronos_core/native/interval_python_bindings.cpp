// Python-facing conversion helpers and pybind11 bindings for the native backend.
//
// Kept in one include-fragment so Python compatibility conversion and binding
// declarations stay adjacent, while C++ interval execution remains independent.

// -----------------------------------------------------------------------------
// Python compatibility conversion helpers
// Source: interval_python_compat.cpp
// -----------------------------------------------------------------------------
namespace chronos::native::detail {

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
		            manage_transaction,
		            &interval_upsert_statements_,
                    IntervalWriteMode::Upsert,
                    live_lo
		        ).stats;
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
	    SQLiteStatementCache interval_upsert_statements_;
	};

class PostgresIntervalConnectionAdapter final : public IntervalConnectionAdapter {
  public:
    explicit PostgresIntervalConnectionAdapter(const py::object &connection)
        : conn_(postgres_conn_from_python_connection(connection)) {}

    py::list query(const std::string &sql, const py::object &params) override {
        BoundSql bound = bind_sql_params_cached(sql, params);
        PgResult result = pg_exec_params(conn_, pg_sql_cached(bound.sql), bound.positional_params);
        result.require(PGRES_TUPLES_OK);
        return rows_from_pg_result(result.get());
    }

    int execute(const std::string &sql, const py::object &params) override {
        BoundSql bound = bind_sql_params_cached(sql, params);
        PgResult result = pg_exec_params(conn_, pg_sql_cached(bound.sql), bound.positional_params);
        result.require(PGRES_COMMAND_OK);
        const char *tuples = PQcmdTuples(result.get());
        if (tuples == nullptr || *tuples == '\0') {
            return 0;
        }
        return std::stoi(tuples);
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
        return postgres_adapter_bulk_upsert(
            conn_,
            physical_name,
            columns,
            pk_columns,
            rows,
            std::to_string(live_lo),
            std::to_string(live_hi),
            writer_segment_id,
            replacement_deleted,
            manage_transaction,
            &interval_upsert_statements_,
            IntervalWriteMode::Upsert,
            std::to_string(live_lo)
        ).stats;
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

    const std::string &pg_sql_cached(const std::string &qmark_sql) {
        auto found = pg_sql_cache_.find(qmark_sql);
        if (found != pg_sql_cache_.end()) {
            return found->second;
        }
        if (pg_sql_cache_.size() > 512) {
            pg_sql_cache_.clear();
        }
        auto inserted = pg_sql_cache_.emplace(qmark_sql, pg_placeholder_sql(qmark_sql));
        return inserted.first->second;
    }

    PGconn *conn_;
    std::unordered_map<std::string, SqlBindPlan> bind_plan_cache_;
    std::unordered_map<std::string, std::string> pg_sql_cache_;
    PgPreparedStatementCache interval_upsert_statements_;
};

bool supports_connection_dialect(const std::string &dialect) {
    return dialect == "sqlite" || dialect == "postgres";
}

std::unique_ptr<IntervalConnectionAdapter> make_interval_connection_adapter(
    const std::string &dialect,
    const py::object &connection
) {
    if (dialect == "sqlite") {
        return std::make_unique<SQLiteIntervalConnectionAdapter>(connection);
    }
    if (dialect == "postgres") {
        return std::make_unique<PostgresIntervalConnectionAdapter>(connection);
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

} // namespace chronos::native::detail

// -----------------------------------------------------------------------------
// pybind11 bindings
// Source: interval_bindings.cpp
// -----------------------------------------------------------------------------
namespace chronos::native {

using namespace detail;

namespace {

py::dict branch_info_to_py(const NativeBranchInfo &info) {
    py::dict out;
    out["branch_id"] = info.branch_id;
    out["current_ref"] = info.current_ref;
    out["created_at"] = info.created_at;
    out["metadata_json"] = info.metadata_json;
    return out;
}

py::dict checkpoint_info_to_py(const NativeCheckpointInfo &info) {
    py::dict out;
    out["checkpoint_id"] = info.checkpoint_id;
    out["branch_id"] = info.branch_id;
    out["ref"] = info.ref;
    out["created_at"] = info.created_at;
    out["metadata_json"] = info.metadata_json;
    return out;
}

py::dict segment_info_to_py(const NativeSegmentInfo &segment) {
    py::dict out;
    out["segment_id"] = segment.segment_id;
    out["live_lo"] = segment.live_lo;
    out["live_hi"] = segment.live_hi;
    out["branch_point"] = segment.branch_point;
    return out;
}

py::dict table_info_to_py(const NativeTableInfo &meta) {
    py::dict out;
    out["table_name"] = meta.logical_name;
    out["physical_table"] = meta.physical_name;
    out["pk_columns"] = meta.primary_key;
    out["columns"] = meta.columns;
    out["column_defs"] = meta.column_defs;
    return out;
}

py::dict prepared_ref_info_to_py(const NativePreparedRefInfo &info) {
    py::dict out;
    out["segment"] = segment_info_to_py(info.segment);
    py::list tables;
    for (const auto &meta : info.tables) {
        tables.append(table_info_to_py(meta));
    }
    out["tables"] = tables;
    out["known_schema_tables"] = info.known_schema_tables;
    return out;
}

py::dict row_values_to_py_dict(
    const std::vector<std::string> &columns,
    const std::vector<IntervalValue> &values
) {
    py::dict out;
    for (std::size_t i = 0; i < columns.size() && i < values.size(); ++i) {
        out[columns[i].c_str()] = value_to_py(values[i]);
    }
    return out;
}

py::dict row_diff_to_py(const NativeRowDiff &diff) {
    py::dict out;
    out["table"] = diff.table;
    out["key"] = row_values_to_py_dict(diff.key_columns, diff.key_values);
    out["change"] = diff.change;
    out["before"] = diff.has_before
        ? row_values_to_py_dict(diff.columns, diff.before)
        : py::object(py::none());
    out["after"] = diff.has_after
        ? row_values_to_py_dict(diff.columns, diff.after)
        : py::object(py::none());
    return out;
}

py::list row_diff_list_to_py(const std::vector<NativeRowDiff> &diffs) {
    py::list out;
    for (const auto &diff : diffs) {
        out.append(row_diff_to_py(diff));
    }
    return out;
}

py::dict merge_preview_to_py(const NativeMergePreview &preview) {
    py::dict out;
    out["changes"] = row_diff_list_to_py(preview.changes);
    out["conflicts"] = row_diff_list_to_py(preview.conflicts);
    return out;
}

std::vector<std::string> dict_string_keys(const py::dict &dict) {
    std::vector<std::string> keys;
    keys.reserve(static_cast<std::size_t>(py::len(dict)));
    for (const auto item : dict) {
        keys.push_back(py::str(item.first).cast<std::string>());
    }
    return keys;
}

std::vector<IntervalValue> dict_values_for_keys(
    const py::dict &dict,
    const std::vector<std::string> &keys
) {
    std::vector<IntervalValue> values;
    values.reserve(keys.size());
    for (const auto &key : keys) {
        values.push_back(py_to_value(dict[py::str(key)]));
    }
    return values;
}

NativeMergeChange merge_change_from_py(const py::dict &item) {
    NativeMergeChange change;
    change.table = py::str(item["table"]).cast<std::string>();
    change.change = py::str(item["change"]).cast<std::string>();

    py::dict key = py::reinterpret_borrow<py::dict>(item["key"]);
    change.key_columns = dict_string_keys(key);
    change.key_values = dict_values_for_keys(key, change.key_columns);

    py::object after = py::reinterpret_borrow<py::object>(item["after"]);
    if (!after.is_none()) {
        py::dict after_dict = py::reinterpret_borrow<py::dict>(after);
        change.after_columns = dict_string_keys(after_dict);
        change.after_values = dict_values_for_keys(after_dict, change.after_columns);
        change.has_after = true;
    }
    return change;
}

std::vector<NativeMergeChange> merge_changes_from_py(const py::list &items) {
    std::vector<NativeMergeChange> changes;
    changes.reserve(static_cast<std::size_t>(py::len(items)));
    for (const auto item : items) {
        changes.push_back(merge_change_from_py(py::reinterpret_borrow<py::dict>(item)));
    }
    return changes;
}

} // namespace

NativeBranchSession::NativeBranchSession(std::unique_ptr<NativeBranchSessionImpl> impl)
    : impl_(std::move(impl)) {}
NativeBranchSession::~NativeBranchSession() = default;
NativeBranchSession::NativeBranchSession(NativeBranchSession &&) noexcept = default;
NativeBranchSession &NativeBranchSession::operator=(NativeBranchSession &&) noexcept = default;

std::vector<std::vector<IntervalValue>> NativeBranchSession::query_visible(
    const std::string &logical_table,
    const std::vector<std::string> &columns,
    const std::string &where_sql,
    const std::vector<IntervalValue> &params,
    const std::string &suffix_sql
) {
    return impl_->query_visible(logical_table, columns, where_sql, params, suffix_sql);
}

IntervalQueryResult NativeBranchSession::query(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    QueryResult result = impl_->query(sql, params);
    return {std::move(result.columns), std::move(result.rows)};
}

IntervalQueryResult NativeBranchSession::explain(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    QueryResult result = impl_->explain(sql, params);
    return {std::move(result.columns), std::move(result.rows)};
}

std::string NativeBranchSession::rewrite_query(const std::string &sql) {
    return impl_->rewrite_query(sql);
}

std::int64_t NativeBranchSession::execute(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    return impl_->execute(sql, params);
}

std::int64_t NativeBranchSession::execute_schema(const std::string &sql) {
    return impl_->execute_schema(sql);
}

void NativeBranchSession::upsert_rows(
    const std::string &logical_table,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const IntervalRows &rows
) {
    impl_->upsert_rows(logical_table, columns, pk_columns, rows, false);
}

void NativeBranchSession::delete_rows(
    const std::string &logical_table,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const IntervalRows &rows
) {
    impl_->upsert_rows(logical_table, columns, pk_columns, rows, true);
}

void NativeBranchSession::begin() { impl_->begin(); }
void NativeBranchSession::commit() { impl_->commit(); }
void NativeBranchSession::rollback() { impl_->rollback(); }
bool NativeBranchSession::in_transaction() const { return impl_->in_transaction(); }

NativeBranchStore::NativeBranchStore(const std::string &database_url)
    : impl_(std::make_unique<NativeBranchStoreImpl>(database_url)) {}
NativeBranchStore::NativeBranchStore(const std::string &data_url, const std::string &metadata_url)
    : impl_(std::make_unique<NativeBranchStoreImpl>(data_url, metadata_url)) {}
NativeBranchStore::NativeBranchStore(const std::string &data_url, sqlite3 *metadata_db)
    : impl_(std::make_unique<NativeBranchStoreImpl>(data_url, metadata_db)) {}
NativeBranchStore::NativeBranchStore(const std::string &data_url, PGconn *metadata_conn)
    : impl_(std::make_unique<NativeBranchStoreImpl>(data_url, metadata_conn)) {}
NativeBranchStore::NativeBranchStore(NativeSqlConnection &data_conn, const std::string &metadata_url)
    : impl_(std::make_unique<NativeBranchStoreImpl>(*data_conn.impl_, metadata_url)) {}
NativeBranchStore::NativeBranchStore(NativeSqlConnection &data_conn, sqlite3 *metadata_db)
    : impl_(std::make_unique<NativeBranchStoreImpl>(*data_conn.impl_, metadata_db)) {}
NativeBranchStore::NativeBranchStore(NativeSqlConnection &data_conn, PGconn *metadata_conn)
    : impl_(std::make_unique<NativeBranchStoreImpl>(*data_conn.impl_, metadata_conn)) {}
NativeBranchStore::NativeBranchStore(sqlite3 *db)
    : impl_(std::make_unique<NativeBranchStoreImpl>(db)) {}
NativeBranchStore::NativeBranchStore(PGconn *conn)
    : impl_(std::make_unique<NativeBranchStoreImpl>(conn)) {}
NativeBranchStore::~NativeBranchStore() = default;
NativeBranchStore::NativeBranchStore(NativeBranchStore &&) noexcept = default;
NativeBranchStore &NativeBranchStore::operator=(NativeBranchStore &&) noexcept = default;

const std::string &NativeBranchStore::dialect() const { return impl_->dialect(); }

NativeBranchSession NativeBranchStore::checkout(const std::string &branch_id) {
    return NativeBranchSession(std::make_unique<NativeBranchSessionImpl>(*impl_, branch_id));
}

NativeBranchSession NativeBranchStore::checkout_segment(
    const std::string &branch_id,
    std::int64_t segment_id,
    const std::string &live_lo,
    const std::string &live_hi,
    const std::string &branch_point
) {
    NativeBranchSegment segment{segment_id, live_lo, live_hi, branch_point};
    return NativeBranchSession(
        std::make_unique<NativeBranchSessionImpl>(*impl_, branch_id, std::move(segment))
    );
}

void NativeBranchStore::ensure(bool enable_schema_branching) {
    impl_->ensure_metadata(enable_schema_branching);
}

void NativeBranchStore::set_create_secondary_indexes(bool enabled) {
    impl_->set_create_secondary_indexes(enabled);
}

bool NativeBranchStore::create_secondary_indexes() const {
    return impl_->create_secondary_indexes();
}

void NativeBranchStore::register_table(
    const std::string &table,
    const std::vector<std::string> &primary_key,
    bool enable_schema_branching
) {
    impl_->register_table(table, primary_key, enable_schema_branching);
}

std::string NativeBranchStore::create_index(
    const std::string &table,
    const std::vector<std::string> &columns,
    const std::string &name
) {
    return impl_->create_index(table, columns, name);
}

std::vector<std::string> NativeBranchStore::branches() { return impl_->branches(); }

void NativeBranchStore::create_branch(
    const std::string &branch_id,
    const std::string &from_branch,
    bool terminal,
    const std::string &metadata_json,
    int continuation_percent,
    int child_width,
    const std::string &allocation_strategy
) {
    impl_->create_branch(
        branch_id,
        from_branch,
        terminal,
        metadata_json,
        continuation_percent,
        child_width,
        allocation_strategy
    );
}

void NativeBranchStore::create_branch_from_checkpoint(
    const std::string &branch_id,
    const std::string &checkpoint
) {
    impl_->create_branch_from_checkpoint(branch_id, checkpoint);
}

void NativeBranchStore::delete_branch(const std::string &branch_id) {
    impl_->delete_branch(branch_id);
}

NativeBranchInfo NativeBranchStore::update_branch_metadata(
    const std::string &branch_id,
    const std::string &metadata_json
) {
    return impl_->update_branch_metadata(branch_id, metadata_json);
}

NativeBranchInfo NativeBranchStore::get_branch(const std::string &branch_id) {
    return impl_->get_branch_info(branch_id);
}

std::vector<NativeBranchInfo> NativeBranchStore::list_branches() {
    return impl_->list_branch_infos();
}

NativeCheckpointInfo NativeBranchStore::create_checkpoint(
    const std::string &checkpoint,
    const std::string &branch,
    const std::string &metadata_json,
    int continuation_percent
) {
    return impl_->create_checkpoint(checkpoint, branch, metadata_json, continuation_percent);
}

NativeCheckpointInfo NativeBranchStore::get_checkpoint(const std::string &checkpoint) {
    return impl_->get_checkpoint_info(checkpoint);
}

std::vector<NativeCheckpointInfo> NativeBranchStore::list_checkpoints(const std::string &branch) {
    return impl_->list_checkpoint_infos(branch);
}

NativePreparedRefInfo NativeBranchStore::prepare_ref_info(
    std::int64_t segment_id,
    bool enable_schema_branching
) {
    return impl_->prepare_ref_info(segment_id, enable_schema_branching);
}

std::vector<std::string> NativeBranchStore::known_schema_tables() {
    return impl_->known_schema_table_names();
}

std::vector<NativeRowDiff> NativeBranchStore::diff_rows(
    const std::string &left,
    const std::string &right,
    const std::string &table
) {
    return impl_->diff_rows(left, right, table);
}

NativeMergePreview NativeBranchStore::merge_preview(
    const std::string &source,
    const std::string &target
) {
    return impl_->merge_preview(source, target);
}

std::int64_t NativeBranchStore::apply_merge_changes(
    const std::string &source,
    const std::string &target,
    const std::vector<NativeMergeChange> &changes
) {
    return impl_->apply_merge_changes(source, target, changes);
}

std::int64_t NativeBranchStore::merge_apply(const std::string &source, const std::string &target) {
    return impl_->merge_apply(source, target);
}

void NativeBranchStore::lock_branches_for_merge(const std::string &source, const std::string &target) {
    impl_->lock_branches_for_merge(source, target);
}

std::int64_t NativeBranchStore::collect_interval_garbage() {
    return impl_->collect_interval_garbage();
}

void NativeBranchStore::commit() { impl_->commit(); }
void NativeBranchStore::rollback() { impl_->rollback(); }
bool NativeBranchStore::in_transaction() const { return impl_->in_transaction(); }
void NativeBranchStore::flush_deferred_schema_indexes() { impl_->flush_deferred_schema_indexes(); }
void NativeBranchStore::clear_deferred_schema_indexes() { impl_->clear_deferred_schema_indexes(); }

std::vector<std::vector<IntervalValue>> NativeBranchStore::query_sql(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    return impl_->query_sql(sql, params);
}

IntervalQueryResult NativeBranchStore::query_sql_result(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    QueryResult result = impl_->query_sql_result(sql, params);
    return {std::move(result.columns), std::move(result.rows)};
}

void NativeBranchStore::execute_sql(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    impl_->execute_sql(sql, params);
}

NativeSqlConnection::NativeSqlConnection(const std::string &database_url)
    : impl_(std::make_unique<NativeSqlConnectionImpl>(database_url)) {}
NativeSqlConnection::~NativeSqlConnection() = default;
NativeSqlConnection::NativeSqlConnection(NativeSqlConnection &&) noexcept = default;
NativeSqlConnection &NativeSqlConnection::operator=(NativeSqlConnection &&) noexcept = default;

const std::string &NativeSqlConnection::dialect() const { return impl_->dialect(); }
void NativeSqlConnection::commit() { impl_->commit(); }
void NativeSqlConnection::rollback() { impl_->rollback(); }
bool NativeSqlConnection::in_transaction() const { return impl_->in_transaction(); }
void NativeSqlConnection::refresh_catalog() { impl_->refresh_catalog(); }

std::vector<std::vector<IntervalValue>> NativeSqlConnection::query_sql(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    return impl_->query_sql(sql, params);
}

IntervalQueryResult NativeSqlConnection::query_sql_result(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    QueryResult result = impl_->query_sql_result(sql, params);
    return {std::move(result.columns), std::move(result.rows)};
}

void NativeSqlConnection::execute_sql(
    const std::string &sql,
    const std::vector<IntervalValue> &params
) {
    impl_->execute_sql(sql, params);
}

IntervalBulkUpsertStats sqlite_interval_bulk_upsert(
    sqlite3 *db,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const IntervalRows &rows,
    std::int64_t live_lo,
    std::int64_t live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction
) {
    BulkUpsertResult result = sqlite_adapter_bulk_upsert(
        db,
        physical_name,
        columns,
        pk_columns,
        rows,
        live_lo,
        live_hi,
        writer_segment_id,
        replacement_deleted,
        manage_transaction,
        nullptr,
        IntervalWriteMode::Upsert,
        live_lo
    );
    BulkUpsertStats stats = result.stats;
    return {stats.selected, stats.deleted_rows, stats.inserted};
}

IntervalBulkUpsertStats postgres_interval_bulk_upsert(
    PGconn *conn,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const IntervalRows &rows,
    std::int64_t live_lo,
    std::int64_t live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction
) {
    return postgres_interval_bulk_upsert(
        conn,
        physical_name,
        columns,
        pk_columns,
        rows,
        std::to_string(live_lo),
        std::to_string(live_hi),
        writer_segment_id,
        replacement_deleted,
        manage_transaction
    );
}

IntervalBulkUpsertStats postgres_interval_bulk_upsert(
    PGconn *conn,
    const std::string &physical_name,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns,
    const IntervalRows &rows,
    const std::string &live_lo,
    const std::string &live_hi,
    std::int64_t writer_segment_id,
    bool replacement_deleted,
    bool manage_transaction
) {
    BulkUpsertResult result = postgres_adapter_bulk_upsert(
        conn,
        physical_name,
        columns,
        pk_columns,
        rows,
        live_lo,
        live_hi,
        writer_segment_id,
        replacement_deleted,
        manage_transaction,
        nullptr,
        IntervalWriteMode::Upsert,
        live_lo
    );
    BulkUpsertStats stats = result.stats;
    return {stats.selected, stats.deleted_rows, stats.inserted};
}

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

    py::class_<NativeBranchSession>(m, "NativeBranchSession")
        .def(
            "query",
            [](NativeBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                auto result = session.query(bound.sql, bound.positional_params);
                py::list out;
                for (const auto &row : result.rows) {
                    py::dict item;
                    for (std::size_t i = 0; i < result.columns.size() && i < row.size(); ++i) {
                        item[result.columns[i].c_str()] = value_to_py(row[i]);
                    }
                    out.append(item);
                }
                return out;
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def(
            "explain",
            [](NativeBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                auto result = session.explain(bound.sql, bound.positional_params);
                py::list out;
                for (const auto &row : result.rows) {
                    py::dict item;
                    for (std::size_t i = 0; i < result.columns.size() && i < row.size(); ++i) {
                        item[result.columns[i].c_str()] = value_to_py(row[i]);
                    }
                    out.append(item);
                }
                return out;
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def(
            "rewrite_query",
            [](NativeBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                return session.rewrite_query(bound.sql);
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def(
            "execute",
            [](NativeBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                return session.execute(bound.sql, bound.positional_params);
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def("execute_schema", &NativeBranchSession::execute_schema, py::arg("sql"))
        .def(
            "query_visible",
            [](NativeBranchSession &session,
               const std::string &logical_table,
               const std::vector<std::string> &columns,
               const std::string &where_sql,
               const py::object &params,
               const std::string &suffix_sql) {
                std::vector<Value> bound = params_to_values(params);
                auto rows = session.query_visible(logical_table, columns, where_sql, bound, suffix_sql);
                py::list out;
                for (const auto &row : rows) {
                    py::dict item;
                    for (std::size_t i = 0; i < columns.size(); ++i) {
                        item[columns[i].c_str()] = value_to_py(row[i]);
                    }
                    out.append(item);
                }
                return out;
            },
            py::arg("logical_table"),
            py::arg("columns"),
            py::arg("where_sql"),
            py::arg("params") = py::tuple(),
            py::arg("suffix_sql") = ""
        )
        .def(
            "upsert_rows",
            [](NativeBranchSession &session,
               const std::string &logical_table,
               const std::vector<std::string> &columns,
               const std::vector<std::string> &pk_columns,
               const py::list &rows) {
                session.upsert_rows(logical_table, columns, pk_columns, rows_to_native_values(rows, columns));
            },
            py::arg("logical_table"),
            py::arg("columns"),
            py::arg("pk_columns"),
            py::arg("rows")
        )
        .def(
            "delete_rows",
            [](NativeBranchSession &session,
               const std::string &logical_table,
               const std::vector<std::string> &columns,
               const std::vector<std::string> &pk_columns,
               const py::list &rows) {
                session.delete_rows(logical_table, columns, pk_columns, rows_to_native_values(rows, columns));
            },
            py::arg("logical_table"),
            py::arg("columns"),
            py::arg("pk_columns"),
            py::arg("rows")
        )
        .def("begin", &NativeBranchSession::begin)
        .def("commit", &NativeBranchSession::commit)
        .def("rollback", &NativeBranchSession::rollback)
        .def("in_transaction", &NativeBranchSession::in_transaction);

    auto native_sql_connection_class = py::class_<NativeSqlConnection>(m, "NativeSqlConnection");

    py::class_<NativeBranchStore>(m, "NativeBranchStore")
        .def(py::init<const std::string &>())
        .def(py::init<const std::string &, const std::string &>())
        .def_static(
            "from_connection",
            [](const std::string &dialect, const py::object &connection) {
                if (dialect == "sqlite") {
                    return NativeBranchStore(sqlite_db_from_python_connection(connection));
                }
                if (dialect == "postgres") {
                    return NativeBranchStore(postgres_conn_from_python_connection(connection));
                }
                throw std::invalid_argument("native branch store dialect is not supported: " + dialect);
            },
            py::arg("dialect"),
            py::arg("connection"),
            py::keep_alive<0, 2>()
        )
        .def(
            py::init([](NativeSqlConnection &data_conn, const std::string &metadata_url) {
                return NativeBranchStore(data_conn, metadata_url);
            }),
            py::keep_alive<1, 2>()
        )
        .def(
            py::init([](const std::string &data_url, const std::string &metadata_dialect, const py::object &connection) {
                if (metadata_dialect == "sqlite") {
                    return NativeBranchStore(data_url, sqlite_db_from_python_connection(connection));
                }
                if (metadata_dialect == "postgres") {
                    return NativeBranchStore(data_url, postgres_conn_from_python_connection(connection));
                }
                throw std::invalid_argument("native split metadata dialect is not supported: " + metadata_dialect);
            }),
            py::keep_alive<1, 4>()
        )
        .def(
            py::init([](NativeSqlConnection &data_conn, const std::string &metadata_dialect, const py::object &connection) {
                if (metadata_dialect == "sqlite") {
                    return NativeBranchStore(data_conn, sqlite_db_from_python_connection(connection));
                }
                if (metadata_dialect == "postgres") {
                    return NativeBranchStore(data_conn, postgres_conn_from_python_connection(connection));
                }
                throw std::invalid_argument("native split metadata dialect is not supported: " + metadata_dialect);
            }),
            py::keep_alive<1, 2>(),
            py::keep_alive<1, 4>()
        )
        .def(
            py::init([](const std::string &dialect, const py::object &connection) {
                if (dialect == "sqlite") {
                    return NativeBranchStore(sqlite_db_from_python_connection(connection));
                }
                if (dialect == "postgres") {
                    return NativeBranchStore(postgres_conn_from_python_connection(connection));
                }
                throw std::invalid_argument("native branch store dialect is not supported: " + dialect);
            }),
            py::keep_alive<1, 3>()
        )
        .def("dialect", &NativeBranchStore::dialect, py::return_value_policy::reference_internal)
        .def("checkout", &NativeBranchStore::checkout, py::keep_alive<0, 1>())
        .def(
            "checkout_segment",
            &NativeBranchStore::checkout_segment,
            py::keep_alive<0, 1>(),
            py::arg("branch_id"),
            py::arg("segment_id"),
            py::arg("live_lo"),
            py::arg("live_hi"),
            py::arg("branch_point")
        )
        .def("ensure", &NativeBranchStore::ensure, py::arg("enable_schema_branching") = false)
        .def(
            "set_create_secondary_indexes",
            &NativeBranchStore::set_create_secondary_indexes,
            py::arg("enabled")
        )
        .def("create_secondary_indexes", &NativeBranchStore::create_secondary_indexes)
        .def(
            "register_table",
            &NativeBranchStore::register_table,
            py::arg("table"),
            py::arg("primary_key"),
            py::arg("enable_schema_branching") = false
        )
        .def(
            "create_index",
            &NativeBranchStore::create_index,
            py::arg("table"),
            py::arg("columns"),
            py::arg("name") = ""
        )
        .def("branches", &NativeBranchStore::branches)
        .def(
            "create_branch",
            &NativeBranchStore::create_branch,
            py::arg("branch_id"),
            py::arg("from_branch"),
            py::arg("terminal") = false,
            py::arg("metadata_json") = "{}",
            py::arg("continuation_percent") = 5,
            py::arg("child_width") = 0,
            py::arg("allocation_strategy") = "adaptive"
        )
        .def(
            "create_branch_from_checkpoint",
            &NativeBranchStore::create_branch_from_checkpoint,
            py::arg("branch_id"),
            py::arg("checkpoint")
        )
        .def("delete_branch", &NativeBranchStore::delete_branch, py::arg("branch_id"))
        .def(
            "update_branch_metadata",
            [](NativeBranchStore &store, const std::string &branch_id, const std::string &metadata_json) {
                return branch_info_to_py(store.update_branch_metadata(branch_id, metadata_json));
            },
            py::arg("branch_id"),
            py::arg("metadata_json")
        )
        .def(
            "get_branch_info",
            [](NativeBranchStore &store, const std::string &branch_id) {
                return branch_info_to_py(store.get_branch(branch_id));
            },
            py::arg("branch_id")
        )
        .def(
            "list_branch_infos",
            [](NativeBranchStore &store) {
                py::list out;
                for (const auto &info : store.list_branches()) {
                    out.append(branch_info_to_py(info));
                }
                return out;
            }
        )
        .def(
            "create_checkpoint",
            [](NativeBranchStore &store,
               const std::string &checkpoint,
               const std::string &branch,
               const std::string &metadata_json,
               int continuation_percent) {
                return checkpoint_info_to_py(
                    store.create_checkpoint(checkpoint, branch, metadata_json, continuation_percent)
                );
            },
            py::arg("checkpoint"),
            py::arg("branch"),
            py::arg("metadata_json") = "{}",
            py::arg("continuation_percent") = 5
        )
        .def(
            "get_checkpoint_info",
            [](NativeBranchStore &store, const std::string &checkpoint) {
                return checkpoint_info_to_py(store.get_checkpoint(checkpoint));
            },
            py::arg("checkpoint")
        )
        .def(
            "list_checkpoint_infos",
            [](NativeBranchStore &store, const std::string &branch) {
                py::list out;
                for (const auto &info : store.list_checkpoints(branch)) {
                    out.append(checkpoint_info_to_py(info));
                }
                return out;
            },
            py::arg("branch") = ""
        )
        .def(
            "prepare_ref_info",
            [](NativeBranchStore &store, std::int64_t segment_id, bool enable_schema_branching) {
                return prepared_ref_info_to_py(
                    store.prepare_ref_info(segment_id, enable_schema_branching)
                );
            },
            py::arg("segment_id"),
            py::arg("enable_schema_branching") = false
        )
        .def("known_schema_tables", &NativeBranchStore::known_schema_tables)
        .def(
            "diff_rows",
            [](NativeBranchStore &store,
               const std::string &left,
               const std::string &right,
               const std::string &table) {
                return row_diff_list_to_py(store.diff_rows(left, right, table));
            },
            py::arg("left"),
            py::arg("right"),
            py::arg("table")
        )
        .def(
            "merge_preview",
            [](NativeBranchStore &store,
               const std::string &source,
               const std::string &target) {
                return merge_preview_to_py(store.merge_preview(source, target));
            },
            py::arg("source"),
            py::arg("target")
        )
        .def(
            "apply_merge_changes",
            [](NativeBranchStore &store,
               const std::string &source,
               const std::string &target,
               const py::list &changes) {
                return store.apply_merge_changes(
                    source,
                    target,
                    merge_changes_from_py(changes)
                );
            },
            py::arg("source"),
            py::arg("target"),
            py::arg("changes")
        )
        .def("merge_apply", &NativeBranchStore::merge_apply)
        .def("lock_branches_for_merge", &NativeBranchStore::lock_branches_for_merge)
        .def("collect_interval_garbage", &NativeBranchStore::collect_interval_garbage)
        .def("commit", &NativeBranchStore::commit)
        .def("rollback", &NativeBranchStore::rollback)
        .def("in_transaction", &NativeBranchStore::in_transaction)
        .def("flush_deferred_schema_indexes", &NativeBranchStore::flush_deferred_schema_indexes)
        .def("clear_deferred_schema_indexes", &NativeBranchStore::clear_deferred_schema_indexes)
        .def(
            "query_sql",
            [](NativeBranchStore &store, const std::string &sql, const py::object &params) {
                std::vector<Value> bound = params_to_values(params);
                auto rows = store.query_sql(sql, bound);
                py::list out;
                for (const auto &row : rows) {
                    py::list item;
                    for (const auto &value : row) item.append(value_to_py(value));
                    out.append(item);
                }
                return out;
            },
            py::arg("sql"),
            py::arg("params") = py::tuple()
        )
        .def(
            "query_sql_dict",
            [](NativeBranchStore &store, const std::string &sql, const py::object &params) {
                std::vector<Value> bound = params_to_values(params);
                auto result = store.query_sql_result(sql, bound);
                py::list out;
                for (const auto &row : result.rows) {
                    py::dict item;
                    for (std::size_t i = 0; i < result.columns.size() && i < row.size(); ++i) {
                        item[result.columns[i].c_str()] = value_to_py(row[i]);
                    }
                    out.append(item);
                }
                return out;
            },
            py::arg("sql"),
            py::arg("params") = py::tuple()
        )
        .def(
            "execute_sql",
            [](NativeBranchStore &store, const std::string &sql, const py::object &params) {
                std::vector<Value> bound = params_to_values(params);
                store.execute_sql(sql, bound);
            },
            py::arg("sql"),
            py::arg("params") = py::tuple()
        );

    native_sql_connection_class
        .def(py::init<const std::string &>())
        .def("dialect", &NativeSqlConnection::dialect, py::return_value_policy::reference_internal)
        .def("commit", &NativeSqlConnection::commit)
        .def("rollback", &NativeSqlConnection::rollback)
        .def("in_transaction", &NativeSqlConnection::in_transaction)
        .def("refresh_catalog", &NativeSqlConnection::refresh_catalog)
        .def(
            "query_sql",
            [](NativeSqlConnection &conn, const std::string &sql, const py::object &params) {
                std::vector<Value> bound = params_to_values(params);
                auto rows = conn.query_sql(sql, bound);
                py::list out;
                for (const auto &row : rows) {
                    py::list item;
                    for (const auto &value : row) item.append(value_to_py(value));
                    out.append(item);
                }
                return out;
            },
            py::arg("sql"),
            py::arg("params") = py::tuple()
        )
        .def(
            "query_sql_dict",
            [](NativeSqlConnection &conn, const std::string &sql, const py::object &params) {
                std::vector<Value> bound = params_to_values(params);
                auto result = conn.query_sql_result(sql, bound);
                py::list out;
                for (const auto &row : result.rows) {
                    py::dict item;
                    for (std::size_t i = 0; i < result.columns.size() && i < row.size(); ++i) {
                        item[result.columns[i].c_str()] = value_to_py(row[i]);
                    }
                    out.append(item);
                }
                return out;
            },
            py::arg("sql"),
            py::arg("params") = py::tuple()
        )
        .def(
            "execute_sql",
            [](NativeSqlConnection &conn, const std::string &sql, const py::object &params) {
                std::vector<Value> bound = params_to_values(params);
                conn.execute_sql(sql, bound);
            },
            py::arg("sql"),
            py::arg("params") = py::tuple()
        );

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
