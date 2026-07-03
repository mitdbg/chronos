#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <pybind11/pybind11.h>
#include <sqlite3.h>

#include "native_types.hpp"

namespace chronos::native {

class NativeCopyBranchSessionImpl;
class NativeCopyBranchStoreImpl;

class NativeCopyBranchSession {
  public:
    explicit NativeCopyBranchSession(std::unique_ptr<NativeCopyBranchSessionImpl> impl);
    ~NativeCopyBranchSession();
    NativeCopyBranchSession(NativeCopyBranchSession &&) noexcept;
    NativeCopyBranchSession &operator=(NativeCopyBranchSession &&) noexcept;
    NativeCopyBranchSession(const NativeCopyBranchSession &) = delete;
    NativeCopyBranchSession &operator=(const NativeCopyBranchSession &) = delete;

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
    std::unique_ptr<NativeCopyBranchSessionImpl> impl_;
};

class NativeCopyBranchStore {
  public:
    explicit NativeCopyBranchStore(const std::string &database_url);
    explicit NativeCopyBranchStore(sqlite3 *db);
    ~NativeCopyBranchStore();
    NativeCopyBranchStore(NativeCopyBranchStore &&) noexcept;
    NativeCopyBranchStore &operator=(NativeCopyBranchStore &&) noexcept;
    NativeCopyBranchStore(const NativeCopyBranchStore &) = delete;
    NativeCopyBranchStore &operator=(const NativeCopyBranchStore &) = delete;

    const std::string &dialect() const;
    void ensure();
    void register_table(
        const std::string &table,
        const std::vector<std::string> &primary_key
    );
    std::string create_index(
        const std::string &table,
        const std::vector<std::string> &columns,
        const std::string &name = ""
    );
    NativeCopyBranchSession checkout(const std::string &branch_id);
    NativeCopyBranchSession checkout_checkpoint(const std::string &checkpoint);
    void create_branch(
        const std::string &branch_id,
        const std::string &from_branch,
        const std::string &metadata_json = "{}"
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
        const std::string &metadata_json = "{}"
    );
    NativeCheckpointInfo get_checkpoint(const std::string &checkpoint);
    std::vector<NativeCheckpointInfo> list_checkpoints(const std::string &branch = "");
    IntervalQueryResult visible_rows(
        const std::string &branch_id,
        const std::string &table
    );
    NativeTableInfo table_info(
        const std::string &branch_id,
        const std::string &table
    );
    std::vector<std::string> table_names();
    void commit();
    void rollback();
    bool in_transaction() const;

  private:
    std::unique_ptr<NativeCopyBranchStoreImpl> impl_;
};

void bind_copy_branch_store(pybind11::module_ &m);

} // namespace chronos::native
