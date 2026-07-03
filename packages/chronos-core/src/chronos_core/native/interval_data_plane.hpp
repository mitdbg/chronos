#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

#include <libpq-fe.h>
#include <pybind11/pybind11.h>
#include <sqlite3.h>

#include "native_types.hpp"

namespace chronos::native {

class NativeBranchSessionImpl;
class NativeBranchStoreImpl;
class NativeSqlConnectionImpl;
class NativeSqlConnection;

class NativeBranchSession {
  public:
    // A checked-out branch view.  The session is bound to one branch segment
    // and rewrites logical SQL into visible interval scans before delegating to
    // the native SQL driver.  Python should treat this as the execution engine:
    // statement planning, DML splicing, and transaction boundaries all live
    // behind this object so other language frontends can reuse the same core.
    explicit NativeBranchSession(std::unique_ptr<NativeBranchSessionImpl> impl);
    ~NativeBranchSession();
    NativeBranchSession(NativeBranchSession &&) noexcept;
    NativeBranchSession &operator=(NativeBranchSession &&) noexcept;
    NativeBranchSession(const NativeBranchSession &) = delete;
    NativeBranchSession &operator=(const NativeBranchSession &) = delete;

    std::vector<std::vector<IntervalValue>> query_visible(
        const std::string &logical_table,
        const std::vector<std::string> &columns,
        const std::string &where_sql,
        const std::vector<IntervalValue> &params,
        const std::string &suffix_sql = ""
    );
    IntervalQueryResult query(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );
    IntervalQueryResult explain(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );
    std::string rewrite_query(const std::string &sql);
    std::int64_t execute(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );
    std::int64_t execute_schema(const std::string &sql);
    void upsert_rows(
        const std::string &logical_table,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const IntervalRows &rows
    );
    void delete_rows(
        const std::string &logical_table,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const IntervalRows &rows
    );
    void begin();
    void commit();
    void rollback();
    bool in_transaction() const;

  private:
    std::unique_ptr<NativeBranchSessionImpl> impl_;
};

class NativeBranchStore {
  public:
    // Store-level control plane.  For single-store SQLite/Postgres deployments
    // data and metadata use the same driver.  For polystore deployments, the
    // data driver may be DuckDB while the metadata driver remains a
    // transactional row store; branch lifecycles, segment allocation, schema
    // bindings, merge publish, and garbage collection are still owned here.
    explicit NativeBranchStore(const std::string &database_url);
    NativeBranchStore(const std::string &data_url, const std::string &metadata_url);
    NativeBranchStore(const std::string &data_url, sqlite3 *metadata_db);
    NativeBranchStore(const std::string &data_url, PGconn *metadata_conn);
    NativeBranchStore(NativeSqlConnection &data_conn, const std::string &metadata_url);
    NativeBranchStore(NativeSqlConnection &data_conn, sqlite3 *metadata_db);
    NativeBranchStore(NativeSqlConnection &data_conn, PGconn *metadata_conn);
    explicit NativeBranchStore(sqlite3 *db);
    explicit NativeBranchStore(PGconn *conn);
    ~NativeBranchStore();
    NativeBranchStore(NativeBranchStore &&) noexcept;
    NativeBranchStore &operator=(NativeBranchStore &&) noexcept;
    NativeBranchStore(const NativeBranchStore &) = delete;
    NativeBranchStore &operator=(const NativeBranchStore &) = delete;

    const std::string &dialect() const;
    NativeBranchSession checkout(const std::string &branch_id);
    NativeBranchSession checkout_segment(
        const std::string &branch_id,
        std::int64_t segment_id,
        const std::string &live_lo,
        const std::string &live_hi,
        const std::string &branch_point
    );
    void ensure(bool enable_schema_branching = false);
    void set_create_secondary_indexes(bool enabled);
    bool create_secondary_indexes() const;
    void register_table(
        const std::string &table,
        const std::vector<std::string> &primary_key,
        bool enable_schema_branching = false
    );
    std::string create_index(
        const std::string &table,
        const std::vector<std::string> &columns,
        const std::string &name = ""
    );
    std::vector<std::string> branches();
    void create_branch(
        const std::string &branch_id,
        const std::string &from_branch,
        bool terminal = false,
        const std::string &metadata_json = "{}",
        int continuation_percent = 5,
        int child_width = 0,
        const std::string &allocation_strategy = "adaptive"
    );
    void create_branch_from_checkpoint(
        const std::string &branch_id,
        const std::string &checkpoint
    );
    void delete_branch(const std::string &branch_id);
    NativeBranchInfo update_branch_metadata(
        const std::string &branch_id,
        const std::string &metadata_json
    );
    NativeBranchInfo get_branch(const std::string &branch_id);
    std::vector<NativeBranchInfo> list_branches();
    NativeCheckpointInfo create_checkpoint(
        const std::string &checkpoint,
        const std::string &branch,
        const std::string &metadata_json = "{}",
        int continuation_percent = 5
    );
    NativeCheckpointInfo get_checkpoint(const std::string &checkpoint);
    std::vector<NativeCheckpointInfo> list_checkpoints(const std::string &branch = "");
    NativePreparedRefInfo prepare_ref_info(
        std::int64_t segment_id,
        bool enable_schema_branching = false
    );
    std::vector<std::string> known_schema_tables();
    std::vector<NativeRowDiff> diff_rows(
        const std::string &left,
        const std::string &right,
        const std::string &table
    );
    NativeMergePreview merge_preview(
        const std::string &source,
        const std::string &target
    );
    std::int64_t apply_merge_changes(
        const std::string &source,
        const std::string &target,
        const std::vector<NativeMergeChange> &changes
    );
    std::int64_t merge_apply(const std::string &source, const std::string &target);
    void lock_branches_for_merge(const std::string &source, const std::string &target);
    std::int64_t collect_interval_garbage();
    void commit();
    void rollback();
    bool in_transaction() const;
    void flush_deferred_schema_indexes();
    void clear_deferred_schema_indexes();
    std::vector<std::vector<IntervalValue>> query_sql(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );
    IntervalQueryResult query_sql_result(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );
    void execute_sql(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );

  private:
    std::unique_ptr<NativeBranchStoreImpl> impl_;
};

class NativeSqlConnection {
  public:
    explicit NativeSqlConnection(const std::string &database_url);
    ~NativeSqlConnection();
    NativeSqlConnection(NativeSqlConnection &&) noexcept;
    NativeSqlConnection &operator=(NativeSqlConnection &&) noexcept;
    NativeSqlConnection(const NativeSqlConnection &) = delete;
    NativeSqlConnection &operator=(const NativeSqlConnection &) = delete;

    const std::string &dialect() const;
    void commit();
    void rollback();
    bool in_transaction() const;
    void refresh_catalog();
    std::vector<std::vector<IntervalValue>> query_sql(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );
    IntervalQueryResult query_sql_result(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );
    void execute_sql(
        const std::string &sql,
        const std::vector<IntervalValue> &params = {}
    );

  private:
    friend class NativeBranchStore;
    std::unique_ptr<NativeSqlConnectionImpl> impl_;
};

void bind_interval_data_plane(pybind11::module_ &m);

void set_sql_profile_enabled(bool enabled);
void reset_sql_profile();
NativeSqlProfile snapshot_sql_profile();
void set_sql_trace_enabled(bool enabled);
void reset_sql_trace();
std::vector<NativeSqlTraceEntry> snapshot_sql_trace();

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
);

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
);

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
);

} // namespace chronos::native
