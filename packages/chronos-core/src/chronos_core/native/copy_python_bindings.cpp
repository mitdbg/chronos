#include "copy_branch_store.hpp"

namespace chronos::native {

using namespace detail;

namespace {

py::dict copy_branch_info_to_py(const NativeBranchInfo &info) {
    py::dict out;
    out["branch_id"] = info.branch_id;
    out["current_ref"] = info.current_ref;
    out["created_at"] = info.created_at;
    out["metadata_json"] = info.metadata_json;
    return out;
}

py::dict copy_checkpoint_info_to_py(const NativeCheckpointInfo &info) {
    py::dict out;
    out["checkpoint_id"] = info.checkpoint_id;
    out["branch_id"] = info.branch_id;
    out["ref"] = info.ref;
    out["created_at"] = info.created_at;
    out["metadata_json"] = info.metadata_json;
    return out;
}

py::dict copy_table_info_to_py(const NativeTableInfo &meta) {
    py::dict out;
    out["table_name"] = meta.logical_name;
    out["physical_table"] = meta.physical_name;
    out["pk_columns"] = meta.primary_key;
    out["columns"] = meta.columns;
    out["column_defs"] = meta.column_defs;
    return out;
}

py::list copy_query_result_to_py_dicts(const IntervalQueryResult &result) {
    py::list out;
    for (const auto &row : result.rows) {
        py::dict item;
        for (std::size_t i = 0; i < result.columns.size() && i < row.size(); ++i) {
            item[result.columns[i].c_str()] = value_to_py(row[i]);
        }
        out.append(item);
    }
    return out;
}

} // namespace

void bind_copy_branch_store(py::module_ &m) {
    py::class_<NativeCopyBranchSession>(m, "NativeCopyBranchSession")
        .def(
            "query",
            [](NativeCopyBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                return copy_query_result_to_py_dicts(session.query(bound.sql, bound.positional_params));
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def(
            "explain",
            [](NativeCopyBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                return copy_query_result_to_py_dicts(session.explain(bound.sql, bound.positional_params));
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def(
            "rewrite_query",
            [](NativeCopyBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                return session.rewrite_query(bound.sql);
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def(
            "execute",
            [](NativeCopyBranchSession &session, const std::string &sql, const py::object &params) {
                BoundSql bound = bind_sql_params(sql, params);
                return session.execute(bound.sql, bound.positional_params);
            },
            py::arg("sql"),
            py::arg("params") = py::dict()
        )
        .def(
            "upsert_rows",
            [](NativeCopyBranchSession &session,
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
            [](NativeCopyBranchSession &session,
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
        .def("begin", &NativeCopyBranchSession::begin)
        .def("commit", &NativeCopyBranchSession::commit)
        .def("rollback", &NativeCopyBranchSession::rollback)
        .def("in_transaction", &NativeCopyBranchSession::in_transaction);

    py::class_<NativeCopyBranchStore>(m, "NativeCopyBranchStore")
        .def(py::init<const std::string &>())
        .def_static(
            "from_connection",
            [](const std::string &dialect, const py::object &connection) {
                if (dialect == "sqlite") {
                    return NativeCopyBranchStore(sqlite_db_from_python_connection(connection));
                }
                throw std::invalid_argument(
                    "native copy branch store from_connection supports SQLite; "
                    "PostgreSQL uses database URL construction"
                );
            },
            py::arg("dialect"),
            py::arg("connection"),
            py::keep_alive<0, 2>()
        )
        .def("dialect", &NativeCopyBranchStore::dialect, py::return_value_policy::reference_internal)
        .def("ensure", &NativeCopyBranchStore::ensure)
        .def("register_table", &NativeCopyBranchStore::register_table, py::arg("table"), py::arg("primary_key"))
        .def(
            "create_index",
            &NativeCopyBranchStore::create_index,
            py::arg("table"),
            py::arg("columns"),
            py::arg("name") = ""
        )
        .def("checkout", &NativeCopyBranchStore::checkout, py::keep_alive<0, 1>(), py::arg("branch_id"))
        .def(
            "checkout_checkpoint",
            &NativeCopyBranchStore::checkout_checkpoint,
            py::keep_alive<0, 1>(),
            py::arg("checkpoint")
        )
        .def(
            "create_branch",
            &NativeCopyBranchStore::create_branch,
            py::arg("branch_id"),
            py::arg("from_branch"),
            py::arg("metadata_json") = "{}"
        )
        .def(
            "create_branch_from_checkpoint",
            &NativeCopyBranchStore::create_branch_from_checkpoint,
            py::arg("branch_id"),
            py::arg("checkpoint")
        )
        .def("delete_branch", &NativeCopyBranchStore::delete_branch, py::arg("branch_id"))
        .def(
            "update_branch_metadata",
            [](NativeCopyBranchStore &store, const std::string &branch_id, const std::string &metadata_json) {
                return copy_branch_info_to_py(store.update_branch_metadata(branch_id, metadata_json));
            },
            py::arg("branch_id"),
            py::arg("metadata_json")
        )
        .def(
            "get_branch_info",
            [](NativeCopyBranchStore &store, const std::string &branch_id) {
                return copy_branch_info_to_py(store.get_branch(branch_id));
            },
            py::arg("branch_id")
        )
        .def(
            "list_branch_infos",
            [](NativeCopyBranchStore &store) {
                py::list out;
                for (const auto &info : store.list_branches()) {
                    out.append(copy_branch_info_to_py(info));
                }
                return out;
            }
        )
        .def(
            "create_checkpoint",
            [](NativeCopyBranchStore &store,
               const std::string &checkpoint,
               const std::string &branch,
               const std::string &metadata_json) {
                return copy_checkpoint_info_to_py(
                    store.create_checkpoint(checkpoint, branch, metadata_json)
                );
            },
            py::arg("checkpoint"),
            py::arg("branch"),
            py::arg("metadata_json") = "{}"
        )
        .def(
            "get_checkpoint_info",
            [](NativeCopyBranchStore &store, const std::string &checkpoint) {
                return copy_checkpoint_info_to_py(store.get_checkpoint(checkpoint));
            },
            py::arg("checkpoint")
        )
        .def(
            "list_checkpoint_infos",
            [](NativeCopyBranchStore &store, const std::string &branch) {
                py::list out;
                for (const auto &info : store.list_checkpoints(branch)) {
                    out.append(copy_checkpoint_info_to_py(info));
                }
                return out;
            },
            py::arg("branch") = ""
        )
        .def(
            "visible_rows",
            [](NativeCopyBranchStore &store, const std::string &branch_id, const std::string &table) {
                return copy_query_result_to_py_dicts(store.visible_rows(branch_id, table));
            },
            py::arg("branch_id"),
            py::arg("table")
        )
        .def(
            "table_info",
            [](NativeCopyBranchStore &store, const std::string &branch_id, const std::string &table) {
                return copy_table_info_to_py(store.table_info(branch_id, table));
            },
            py::arg("branch_id"),
            py::arg("table")
        )
        .def("table_names", &NativeCopyBranchStore::table_names)
        .def("commit", &NativeCopyBranchStore::commit)
        .def("rollback", &NativeCopyBranchStore::rollback)
        .def("in_transaction", &NativeCopyBranchStore::in_transaction);
}

} // namespace chronos::native
