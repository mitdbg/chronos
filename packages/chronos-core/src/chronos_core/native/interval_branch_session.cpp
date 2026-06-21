class NativeBranchSessionImpl {
  public:
    NativeBranchSessionImpl(NativeBranchStoreImpl &store, std::string branch_id)
        : store_(store), branch_id_(std::move(branch_id)), segment_(store_.load_segment(branch_id_)) {}

    NativeBranchSessionImpl(
        NativeBranchStoreImpl &store,
        std::string branch_id,
        NativeBranchSegment segment
    ) : store_(store), branch_id_(std::move(branch_id)), segment_(std::move(segment)) {}

    std::vector<std::vector<Value>> query_visible(
        const std::string &logical_table,
        const std::vector<std::string> &columns,
        const std::string &where_sql,
        const std::vector<Value> &params,
        const std::string &suffix_sql
    ) {
        std::string sql = "SELECT " + select_columns_sql(columns) + " FROM " +
            quote_ident(physical_table_for(logical_table)) + " WHERE " + where_sql +
            " AND live_lo <= ? AND ? < live_hi AND deleted = FALSE " + suffix_sql;
        std::vector<Value> bound = params;
        bound.push_back(segment_.branch_point);
        bound.push_back(segment_.branch_point);
        return store_.driver().query(sql, bound);
    }

    QueryResult query(const std::string &sql, const std::vector<Value> &params) {
        if (!is_select_query_sql(sql)) {
            throw std::runtime_error("native branch query only accepts SELECT");
        }
        return store_.driver().query_result(rewrite_select_sql(sql), params);
    }

    std::int64_t execute(const std::string &sql, const std::vector<Value> &params) {
        PgProtobufParseResult parsed(sql);
        PgQuery__Node *stmt = parsed.single_statement();
        switch (stmt->node_case) {
        case PG_QUERY__NODE__NODE_CREATE_STMT:
        case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT:
        case PG_QUERY__NODE__NODE_DROP_STMT:
        case PG_QUERY__NODE__NODE_INDEX_STMT:
            return execute_schema_statement(stmt);
        default:
            break;
        }
        const CachedNativeStatement &statement = cached_statement(sql);
        switch (statement.kind) {
        case NativeStatementKind::Insert:
            return execute_insert(statement.insert, params);
        case NativeStatementKind::Update:
            return execute_update(statement.update, params);
        case NativeStatementKind::Delete:
            return execute_delete(statement.del, params);
        }
        throw std::runtime_error("unsupported native branch SQL statement");
    }

    std::int64_t execute_schema(const std::string &sql) {
        PgProtobufParseResult parsed(sql);
        return execute_schema_statement(parsed.single_statement());
    }

    void upsert_rows(
        const std::string &logical_table,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &pk_columns,
        const NativeRows &rows,
        bool deleted
    ) {
        store_.driver().interval_upsert(
            physical_table_for(logical_table),
            columns,
            pk_columns,
            rows,
            segment_.live_lo,
            segment_.live_hi,
            segment_.segment_id,
            deleted,
            !store_.driver().in_transaction()
        );
    }

    void begin() {
        if (store_.driver().in_transaction()) {
            in_transaction_ = true;
            return;
        }
        store_.driver().execute(store_.dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        in_transaction_ = true;
    }
    void commit() {
        store_.driver().execute("COMMIT");
        in_transaction_ = false;
    }
    void rollback() {
        try {
            store_.driver().execute("ROLLBACK");
        } catch (...) {
        }
        in_transaction_ = false;
    }
    bool in_transaction() const { return in_transaction_; }

    void commit_if_started(bool started_tx) {
        if (started_tx) {
            store_.driver().execute("COMMIT");
        }
    }

    void rollback_if_started(bool started_tx) {
        if (!started_tx) return;
        try {
            store_.driver().execute("ROLLBACK");
        } catch (...) {
        }
    }

  private:
    void clear_schema_caches() {
        table_metas_cache_.reset();
        known_table_names_cache_.reset();
        table_meta_by_name_.clear();
        select_rewrite_cache_.clear();
        store_.clear_statement_plan_cache();
    }

    const std::vector<NativeTableMeta> &table_metas_for_session() {
        if (!table_metas_cache_) {
            table_metas_cache_ = store_.load_table_metas(segment_);
            table_meta_by_name_.clear();
            for (const auto &meta : *table_metas_cache_) {
                table_meta_by_name_.emplace(meta.logical_name, meta);
            }
        }
        return *table_metas_cache_;
    }

    void warm_physical_table_plans() {
        if (store_.dialect() != "sqlite") return;
        for (const auto &meta : table_metas_for_session()) {
            if (meta.columns.empty()) continue;
            // SQLite invalidates schema and statement caches after DDL.  Prepare
            // a zero-row visible-table query now so the first user read after a
            // branch-local schema change does not pay that one-time reparse.
            store_.driver().query(
                "SELECT " + quote_ident(meta.columns.front()) +
                " FROM " + quote_ident(meta.physical_name) +
                " WHERE live_lo <= ? AND ? < live_hi AND deleted = FALSE AND 0",
                {segment_.branch_point, segment_.branch_point}
            );
        }
    }

    std::string rewrite_select_sql(const std::string &sql) {
        auto cached = select_rewrite_cache_.find(sql);
        if (cached != select_rewrite_cache_.end()) {
            return cached->second;
        }
        if (select_rewrite_cache_.size() > 512) {
            select_rewrite_cache_.clear();
        }
        std::unordered_map<std::string, std::string> replacements;
        for (const auto &meta : table_metas_for_session()) {
            const std::string cols = select_columns_sql(meta.columns);
            // Logical table references are replaced by visible-table subqueries.
            // The interval predicate is fixed by this session's branch point,
            // so cached rewrites are safe until the session is checked out
            // again or schema metadata changes.
            replacements.emplace(
                meta.logical_name,
                "SELECT " + cols + " FROM " + quote_ident(meta.physical_name) +
                    " WHERE " + visible_where_sql(segment_.branch_point)
            );
        }
        validate_referenced_tables_visible(sql, replacements);
        auto inserted = select_rewrite_cache_.emplace(sql, rewrite_visible_tables(sql, replacements));
        return inserted.first->second;
    }

    void validate_referenced_tables_visible(
        const std::string &sql,
        const std::unordered_map<std::string, std::string> &active_tables
    ) {
        const auto referenced = scan_referenced_table_names(sql);
        if (referenced.empty()) return;
        if (!known_table_names_cache_) {
            known_table_names_cache_ = std::unordered_set<std::string>();
            for (const auto &name : store_.known_schema_table_names()) {
                known_table_names_cache_->insert(name);
            }
        }
        for (const auto &table : referenced) {
            if (known_table_names_cache_->find(table) != known_table_names_cache_->end() &&
                active_tables.find(table) == active_tables.end()) {
                throw std::runtime_error("chronos_table_not_registered: " + table);
            }
        }
    }

    static std::unordered_set<std::string> scan_referenced_table_names(const std::string &sql) {
        std::unordered_set<std::string> names;
        bool single_quote = false;
        bool expect_table = false;
        for (std::size_t i = 0; i < sql.size();) {
            const char ch = sql[i];
            if (ch == '\'' && !single_quote) {
                single_quote = true;
                ++i;
                continue;
            }
            if (single_quote) {
                if (ch == '\'') single_quote = false;
                ++i;
                continue;
            }
            if (ch == '"') {
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
                    names.insert(ident);
                    expect_table = false;
                }
                i = j < sql.size() ? j + 1 : j;
                continue;
            }
            if (ch == '(') {
                expect_table = false;
                ++i;
                continue;
            }
            if (!is_identifier_start(ch)) {
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
            if (expect_table) {
                std::string table = ident;
                if (j < sql.size() && sql[j] == '.' && j + 1 < sql.size() && is_identifier_start(sql[j + 1])) {
                    std::size_t k = j + 2;
                    while (k < sql.size() && is_identifier_part(sql[k])) ++k;
                    table = ident + "." + sql.substr(j + 1, k - j - 1);
                    j = k;
                }
                names.insert(table);
                expect_table = false;
                i = j;
                continue;
            }
            expect_table = upper == "FROM" || upper == "JOIN" || upper == "UPDATE" || upper == "INTO";
            i = j;
        }
        return names;
    }

    NativeTableMeta table_meta_for(const std::string &logical_table) {
        auto found = table_meta_by_name_.find(logical_table);
        if (found != table_meta_by_name_.end()) {
            return found->second;
        }
        NativeTableMeta meta = store_.load_table_meta(logical_table, segment_);
        table_meta_by_name_.emplace(logical_table, meta);
        return meta;
    }

    QueryResult select_visible_rows(
        const NativeTableMeta &meta,
        const std::string &where_sql,
        const std::vector<Value> &where_params,
        const std::vector<std::string> &columns
    ) {
        std::string sql = "SELECT " + select_columns_sql(columns) + " FROM " +
            quote_ident(meta.physical_name) + " WHERE ";
        if (!trim_copy(where_sql).empty()) {
            sql += "(" + where_sql + ") AND ";
        }
        sql += visible_where_sql(segment_.branch_point);
        return store_.driver().query_result(sql, where_params);
    }

    NativeInsertPlan build_insert_plan(PgQuery__InsertStmt *stmt) {
        if (!stmt || !stmt->relation) throw std::runtime_error("INSERT must specify a table");
        if (stmt->with_clause || stmt->n_returning_list > 0) {
            throw std::runtime_error("native INSERT does not support WITH or RETURNING");
        }
        NativeInsertPlan plan;
        plan.table = relation_name(stmt->relation);
        for (std::size_t i = 0; i < stmt->n_cols; ++i) {
            auto *node = stmt->cols[i];
            if (!node || node->node_case != PG_QUERY__NODE__NODE_RES_TARGET || !node->res_target->name) {
                throw std::runtime_error("INSERT must use a simple column list");
            }
            plan.columns.emplace_back(node->res_target->name);
        }
        if (plan.columns.empty()) throw std::runtime_error("INSERT must specify a column list");
        if (stmt->on_conflict_clause) {
            if (stmt->on_conflict_clause->action == PG_QUERY__ON_CONFLICT_ACTION__ONCONFLICT_NOTHING) {
                plan.ignore_conflicts = true;
            } else {
                throw std::runtime_error("native INSERT only supports ON CONFLICT DO NOTHING");
            }
        }
        if (!stmt->select_stmt || stmt->select_stmt->node_case != PG_QUERY__NODE__NODE_SELECT_STMT) {
            throw std::runtime_error("native INSERT only supports INSERT ... VALUES");
        }
        auto *select = stmt->select_stmt->select_stmt;
        if (!select || select->n_values_lists == 0 || select->n_from_clause || select->where_clause) {
            throw std::runtime_error("native INSERT only supports tuple VALUES");
        }
        for (std::size_t i = 0; i < select->n_values_lists; ++i) {
            auto *tuple_node = select->values_lists[i];
            if (!tuple_node || tuple_node->node_case != PG_QUERY__NODE__NODE_LIST) {
                throw std::runtime_error("native INSERT only supports tuple VALUES");
            }
            auto *items = tuple_node->list;
            if (items->n_items != plan.columns.size()) {
                throw std::runtime_error("INSERT column/value count mismatch");
            }
            std::vector<PgQuery__Node *> tuple;
            tuple.reserve(items->n_items);
            for (std::size_t j = 0; j < items->n_items; ++j) tuple.push_back(items->items[j]);
            plan.value_tuples.push_back(std::move(tuple));
        }
        return plan;
    }

    NativeUpdatePlan build_update_plan(PgQuery__UpdateStmt *stmt) {
        if (!stmt || !stmt->relation) throw std::runtime_error("UPDATE must specify a table");
        if (stmt->with_clause || stmt->n_from_clause > 0 || stmt->n_returning_list > 0) {
            throw std::runtime_error("native UPDATE does not support WITH, FROM, or RETURNING");
        }
        NativeUpdatePlan plan;
        plan.table = relation_name(stmt->relation);
        plan.where = stmt->where_clause;
        for (std::size_t i = 0; i < stmt->n_target_list; ++i) {
            auto *node = stmt->target_list[i];
            if (!node || node->node_case != PG_QUERY__NODE__NODE_RES_TARGET ||
                !node->res_target->name || node->res_target->n_indirection > 0 ||
                !node->res_target->val) {
                throw std::runtime_error("native UPDATE only supports simple column assignments");
            }
            plan.assignments.emplace_back(node->res_target->name, node->res_target->val);
        }
        return plan;
    }

    NativeDeletePlan build_delete_plan(PgQuery__DeleteStmt *stmt) {
        if (!stmt || !stmt->relation) throw std::runtime_error("DELETE must specify a table");
        if (stmt->with_clause || stmt->n_using_clause > 0 || stmt->n_returning_list > 0) {
            throw std::runtime_error("native DELETE does not support WITH, USING, or RETURNING");
        }
        return {relation_name(stmt->relation), stmt->where_clause};
    }

    CachedNativeStatement cached_statement(const std::string &sql) {
        return store_.cached_statement_plan(sql, [&]() {
            CachedNativeStatement cached;
            cached.parsed = std::make_shared<PgProtobufParseResult>(sql);
            PgQuery__Node *stmt = cached.parsed->single_statement();
            switch (stmt->node_case) {
            case PG_QUERY__NODE__NODE_INSERT_STMT:
                cached.kind = NativeStatementKind::Insert;
                cached.insert = build_insert_plan(stmt->insert_stmt);
                break;
            case PG_QUERY__NODE__NODE_UPDATE_STMT:
                cached.kind = NativeStatementKind::Update;
                cached.update = build_update_plan(stmt->update_stmt);
                break;
            case PG_QUERY__NODE__NODE_DELETE_STMT:
                cached.kind = NativeStatementKind::Delete;
                cached.del = build_delete_plan(stmt->delete_stmt);
                break;
            default:
                throw std::runtime_error("unsupported native branch SQL statement");
            }
            return cached;
        });
    }

    bool segment_is_root() {
        auto rows = store_.driver().query(
            "SELECT parent_segment_id FROM _chronos_branch_interval_segments WHERE segment_id = ?",
            {segment_.segment_id}
        );
        return !rows.empty() && std::holds_alternative<std::monostate>(rows[0][0]);
    }

    bool schema_version_private_to_current_ref(const NativeTableMeta &meta) {
        if (!meta.has_schema_binding || meta.schema_version_id.empty()) return false;
        return store_.schema_version_private_to_ref(
            branch_id_,
            meta.logical_name,
            meta.schema_version_id,
            segment_
        );
    }

    bool private_schema_table_is_canonical(const NativeTableMeta &meta) {
        if (!schema_version_private_to_current_ref(meta)) return false;
        return meta.ddl_op != "register" || segment_is_root();
    }

    std::int64_t execute_schema_statement(PgQuery__Node *stmt) {
        const bool started_tx = !store_.driver().in_transaction();
        if (started_tx) store_.driver().execute(store_.dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            lock_current_branch_for_schema_change();
            std::int64_t result = 0;
            switch (stmt->node_case) {
            case PG_QUERY__NODE__NODE_CREATE_STMT:
                result = execute_create_table_ddl(stmt->create_stmt);
                break;
            case PG_QUERY__NODE__NODE_INDEX_STMT:
                result = execute_create_index_ddl(stmt->index_stmt);
                break;
            case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT:
                result = execute_alter_table_ddl(stmt->alter_table_stmt);
                break;
            case PG_QUERY__NODE__NODE_DROP_STMT:
                result = execute_drop_ddl(stmt->drop_stmt);
                break;
            default:
                throw std::runtime_error("unsupported native branch schema statement");
            }
            clear_schema_caches();
            // Refresh the schema/table cache before publishing the DDL result.
            // Branch-local DDL changes the active physical table for this
            // session; if we defer this metadata load, the next user SELECT pays
            // the cold schema-binding lookup and visible-table rewrite setup.
            // Doing it while the DDL transaction is still open keeps the first
            // post-DDL read on the same fast path as steady-state reads.
            (void)table_metas_for_session();
            warm_physical_table_plans();
            if (started_tx) store_.driver().execute("COMMIT");
            return result;
        } catch (...) {
            if (started_tx) {
                try {
                    store_.driver().execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    void lock_current_branch_for_schema_change() {
        if (store_.dialect() != "postgres") return;
        auto rows = store_.driver().query(
            "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = ? FOR UPDATE",
            {branch_id_}
        );
        if (rows.empty()) {
            throw std::runtime_error("branch not found: " + branch_id_);
        }
    }

    std::int64_t execute_create_index_ddl(PgQuery__IndexStmt *stmt) {
        if (!stmt || stmt->unique || stmt->primary || stmt->where_clause || stmt->n_index_including_params > 0) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        const std::string index_name = stmt->idxname ? stmt->idxname : "";
        const std::string table = relation_name(stmt->relation);
        std::vector<std::string> columns;
        for (std::size_t i = 0; i < stmt->n_index_params; ++i) {
            PgQuery__Node *node = stmt->index_params[i];
            if (!node || node->node_case != PG_QUERY__NODE__NODE_INDEX_ELEM || !node->index_elem || node->index_elem->expr) {
                throw std::runtime_error("unsupported native branch schema statement");
            }
            columns.emplace_back(node->index_elem->name ? node->index_elem->name : "");
        }
        if (index_name.empty() || columns.empty()) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        NativeTableMeta meta = table_meta_for(table);
        for (const auto &column : columns) {
            (void)column_index(meta.columns, column);
        }
        auto existing = store_.driver().query(
            "SELECT table_name, columns FROM _chronos_branch_indexes "
            "WHERE backend = 'interval' AND index_name = ?",
            {index_name}
        );
        if (existing.empty()) {
            store_.driver().execute(
                "INSERT INTO _chronos_branch_indexes (index_name, table_name, columns, backend) "
                "VALUES (?, ?, ?, 'interval')",
                {index_name, table, json_string_array(columns)}
            );
        } else if (native_as_string(existing[0][0]) != table || native_as_string(existing[0][1]) != json_string_array(columns)) {
            throw std::runtime_error("index already exists with different definition: " + index_name);
        }
        store_.create_physical_logical_indexes(table, index_name, columns);
        return 0;
    }

    std::int64_t execute_drop_index_ddl(PgQuery__DropStmt *stmt) {
        if (!stmt || stmt->remove_type != PG_QUERY__OBJECT_TYPE__OBJECT_INDEX || stmt->n_objects != 1) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        const std::string index_name = drop_object_name(stmt->objects[0]);
        auto rows = store_.driver().query(
            "SELECT table_name FROM _chronos_branch_indexes WHERE backend = 'interval' AND index_name = ?",
            {index_name}
        );
        if (rows.empty()) {
            if (stmt->missing_ok) return 0;
            throw std::runtime_error("table is not registered for interval branching: " + index_name);
        }
        const std::string table = native_as_string(rows[0][0]);
        for (const auto &meta : store_.known_physical_metas_for_table(table)) {
            store_.driver().execute(
                "DROP INDEX IF EXISTS " +
                quote_ident(store_.physical_logical_index_name(meta, table, index_name))
            );
        }
        store_.driver().execute(
            "DELETE FROM _chronos_branch_indexes WHERE backend = 'interval' AND index_name = ?",
            {index_name}
        );
        return 0;
    }

    std::int64_t execute_create_table_ddl(PgQuery__CreateStmt *stmt) {
        if (!stmt || !stmt->relation || stmt->n_constraints > 0 || stmt->n_inh_relations > 0) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        const std::string table = relation_name(stmt->relation);
        auto active = store_.active_binding_for_table(table, segment_);
        if (active && native_as_int((*active)[1]) == 0) {
            throw std::runtime_error("branch already exists: " + table);
        }
        std::vector<std::string> columns;
        std::vector<std::string> defs;
        std::vector<std::string> pk_columns;
        for (std::size_t i = 0; i < stmt->n_table_elts; ++i) {
            PgQuery__Node *node = stmt->table_elts[i];
            if (!node || node->node_case != PG_QUERY__NODE__NODE_COLUMN_DEF || !node->column_def) {
                throw std::runtime_error("unsupported native branch schema statement");
            }
            column_def_parts(node->column_def, true, columns, defs, pk_columns);
        }
        if (pk_columns.empty()) {
            throw std::runtime_error("CREATE TABLE requires an inline primary key");
        }
        const std::string physical = store_.physical_schema_table_name(table);
        store_.create_interval_physical_table(physical, defs, pk_columns);
        const std::string schema_id = store_.record_schema_version(
            table,
            physical,
            pk_columns,
            columns,
            defs,
            "create_table",
            ""
        );
        store_.splice_table_binding(table, schema_id, false, segment_);
        return 0;
    }

    std::int64_t execute_alter_table_ddl(PgQuery__AlterTableStmt *stmt) {
        if (!stmt || stmt->objtype != PG_QUERY__OBJECT_TYPE__OBJECT_TABLE || stmt->n_cmds != 1) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        const std::string table = relation_name(stmt->relation);
        NativeTableMeta old_meta = table_meta_for(table);
        auto active = store_.active_binding_for_table(table, segment_);
        if (!active || native_as_int((*active)[1]) != 0) {
            throw std::runtime_error("table is not registered for interval branching: " + table);
        }
        PgQuery__Node *cmd_node = stmt->cmds[0];
        if (!cmd_node || cmd_node->node_case != PG_QUERY__NODE__NODE_ALTER_TABLE_CMD || !cmd_node->alter_table_cmd) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        PgQuery__AlterTableCmd *cmd = cmd_node->alter_table_cmd;
        switch (cmd->subtype) {
        case PG_QUERY__ALTER_TABLE_TYPE__AT_AddColumn:
            return execute_alter_add_column(table, old_meta, native_as_string((*active)[0]), cmd);
        case PG_QUERY__ALTER_TABLE_TYPE__AT_DropColumn:
            return execute_alter_drop_column(table, old_meta, native_as_string((*active)[0]), cmd);
        case PG_QUERY__ALTER_TABLE_TYPE__AT_AlterColumnType:
            return execute_alter_column_type(table, old_meta, native_as_string((*active)[0]), cmd);
        default:
            throw std::runtime_error("unsupported native branch schema statement");
        }
    }

    std::int64_t execute_drop_ddl(PgQuery__DropStmt *stmt) {
        if (!stmt) throw std::runtime_error("unsupported native branch schema statement");
        if (stmt->remove_type == PG_QUERY__OBJECT_TYPE__OBJECT_INDEX) {
            return execute_drop_index_ddl(stmt);
        }
        if (stmt->remove_type != PG_QUERY__OBJECT_TYPE__OBJECT_TABLE || stmt->n_objects != 1) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        const std::string table = drop_object_name(stmt->objects[0]);
        auto active = store_.active_binding_for_table(table, segment_);
        if (!active) {
            throw std::runtime_error("table is not registered for interval branching: " + table);
        }
        store_.splice_table_binding(table, "", true, segment_);
        return 0;
    }

    std::int64_t execute_alter_add_column(
        const std::string &table,
        const NativeTableMeta &old_meta,
        const std::string &schema_version_id,
        PgQuery__AlterTableCmd *cmd
    ) {
        if (!cmd->def || cmd->def->node_case != PG_QUERY__NODE__NODE_COLUMN_DEF || !cmd->def->column_def) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        std::vector<std::string> add_columns;
        std::vector<std::string> add_defs;
        std::vector<std::string> ignored_pk;
        column_def_parts(cmd->def->column_def, false, add_columns, add_defs, ignored_pk);
        const std::string column = add_columns.front();
        if (std::find(old_meta.columns.begin(), old_meta.columns.end(), column) != old_meta.columns.end()) {
            throw std::runtime_error("column already exists: " + column);
        }
        std::string default_sql = column_default_sql(cmd->def->column_def);
        NativeTableMeta new_meta = old_meta;
        new_meta.columns.push_back(column);
        new_meta.column_defs.push_back(add_defs.front());
        // Private schema versions are mutated in place because no existing
        // branch/checkpoint/fork-base can still observe the old definition.
        if (store_.schema_version_private_to_ref(branch_id_, table, schema_version_id, segment_)) {
            store_.driver().execute(
                "ALTER TABLE " + quote_ident(old_meta.physical_name) + " ADD COLUMN " + add_defs.front()
            );
            store_.update_schema_version_metadata(schema_version_id, new_meta, "alter_table_add_column");
            return 0;
        }
        // Shared schema versions get a fresh physical table and a binding
        // splice. This preserves old readers while the current branch switches
        // to the new schema over its interval.
        const std::string physical = store_.physical_schema_table_name(table);
        new_meta.physical_name = physical;
        store_.create_interval_physical_table(physical, new_meta.column_defs, new_meta.pk_columns, false);
        const std::string new_schema_id = store_.record_schema_version(
            table,
            physical,
            new_meta.pk_columns,
            new_meta.columns,
            new_meta.column_defs,
            "alter_table_add_column",
            schema_version_id
        );
        std::unordered_map<std::string, std::string> defaults;
        if (!default_sql.empty()) defaults.emplace(column, default_sql);
        store_.copy_visible_rows_to_schema_version(old_meta, new_meta, segment_, defaults);
        store_.splice_table_binding(table, new_schema_id, false, segment_);
        create_schema_version_secondary_indexes(new_meta, table);
        return 0;
    }

    std::int64_t execute_alter_drop_column(
        const std::string &table,
        const NativeTableMeta &old_meta,
        const std::string &schema_version_id,
        PgQuery__AlterTableCmd *cmd
    ) {
        const std::string column = cmd->name ? cmd->name : "";
        if (column.empty()) throw std::runtime_error("unsupported native branch schema statement");
        auto column_it = std::find(old_meta.columns.begin(), old_meta.columns.end(), column);
        if (column_it == old_meta.columns.end()) {
            throw std::runtime_error("table is not registered for interval branching: column missing from " + table + ": " + column);
        }
        if (std::find(old_meta.pk_columns.begin(), old_meta.pk_columns.end(), column) != old_meta.pk_columns.end()) {
            throw std::runtime_error("dropping primary key columns is not supported");
        }
        NativeTableMeta new_meta = old_meta;
        std::vector<std::string> columns;
        std::vector<std::string> defs;
        for (std::size_t i = 0; i < old_meta.columns.size(); ++i) {
            if (old_meta.columns[i] == column) continue;
            columns.push_back(old_meta.columns[i]);
            defs.push_back(old_meta.column_defs[i]);
        }
        new_meta.columns = std::move(columns);
        new_meta.column_defs = std::move(defs);
        if (store_.schema_version_private_to_ref(branch_id_, table, schema_version_id, segment_)) {
            store_.driver().execute(
                "ALTER TABLE " + quote_ident(old_meta.physical_name) + " DROP COLUMN " + quote_ident(column)
            );
            store_.update_schema_version_metadata(schema_version_id, new_meta, "alter_table_drop_column");
            return 0;
        }
        const std::string physical = store_.physical_schema_table_name(table);
        new_meta.physical_name = physical;
        store_.create_interval_physical_table(physical, new_meta.column_defs, new_meta.pk_columns, false);
        const std::string new_schema_id = store_.record_schema_version(
            table,
            physical,
            new_meta.pk_columns,
            new_meta.columns,
            new_meta.column_defs,
            "alter_table_drop_column",
            schema_version_id
        );
        store_.copy_visible_rows_to_schema_version(old_meta, new_meta, segment_);
        store_.splice_table_binding(table, new_schema_id, false, segment_);
        create_schema_version_secondary_indexes(new_meta, table);
        return 0;
    }

    std::int64_t execute_alter_column_type(
        const std::string &table,
        const NativeTableMeta &old_meta,
        const std::string &schema_version_id,
        PgQuery__AlterTableCmd *cmd
    ) {
        const std::string column = cmd->name ? cmd->name : "";
        if (column.empty() || !cmd->def || cmd->def->node_case != PG_QUERY__NODE__NODE_COLUMN_DEF || !cmd->def->column_def) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        std::size_t idx = column_index(old_meta.columns, column);
        const std::string type_sql = type_name_sql(cmd->def->column_def->type_name);
        NativeTableMeta new_meta = old_meta;
        new_meta.column_defs[idx] = replace_column_type_sql(old_meta.column_defs[idx], column, type_sql);
        const std::string using_sql = cmd->def->column_def->raw_default
            ? expression_sql(cmd->def->column_def->raw_default)
            : "CAST(" + quote_ident(column) + " AS " + type_sql + ")";
        if (store_.dialect() == "postgres" &&
            store_.schema_version_private_to_ref(branch_id_, table, schema_version_id, segment_)) {
            store_.driver().execute(
                "ALTER TABLE " + quote_ident(old_meta.physical_name) +
                " ALTER COLUMN " + quote_ident(column) + " TYPE " + type_sql +
                " USING " + using_sql
            );
            store_.update_schema_version_metadata(schema_version_id, new_meta, "alter_table_alter_column_type");
            return 0;
        }
        const std::string physical = store_.physical_schema_table_name(table);
        new_meta.physical_name = physical;
        store_.create_interval_physical_table(physical, new_meta.column_defs, new_meta.pk_columns, false);
        const std::string new_schema_id = store_.record_schema_version(
            table,
            physical,
            new_meta.pk_columns,
            new_meta.columns,
            new_meta.column_defs,
            "alter_table_alter_column_type",
            schema_version_id
        );
        store_.copy_visible_rows_to_schema_version(old_meta, new_meta, segment_, {}, {{column, using_sql}});
        store_.splice_table_binding(table, new_schema_id, false, segment_);
        create_schema_version_secondary_indexes(new_meta, table);
        return 0;
    }

    std::string drop_object_name(PgQuery__Node *node) {
        if (!node || node->node_case != PG_QUERY__NODE__NODE_LIST || !node->list || node->list->n_items == 0) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        std::vector<std::string> parts;
        for (std::size_t i = 0; i < node->list->n_items; ++i) {
            parts.push_back(node_string_name(node->list->items[i]));
        }
        return join_strings(parts, ".");
    }

    void column_def_parts(
        PgQuery__ColumnDef *column_def,
        bool allow_primary_key,
        std::vector<std::string> &columns,
        std::vector<std::string> &defs,
        std::vector<std::string> &pk_columns
    ) {
        if (!column_def || !column_def->colname) {
            throw std::runtime_error("unsupported native branch schema statement");
        }
        std::string default_sql;
        bool primary_key = false;
        for (std::size_t i = 0; i < column_def->n_constraints; ++i) {
            PgQuery__Node *node = column_def->constraints[i];
            if (!node || node->node_case != PG_QUERY__NODE__NODE_CONSTRAINT || !node->constraint) {
                throw std::runtime_error("unsupported native branch schema statement");
            }
            PgQuery__Constraint *constraint = node->constraint;
            if (allow_primary_key && constraint->contype == PG_QUERY__CONSTR_TYPE__CONSTR_PRIMARY) {
                primary_key = true;
                continue;
            }
            if (constraint->contype == PG_QUERY__CONSTR_TYPE__CONSTR_DEFAULT) {
                default_sql = expression_sql(constraint->raw_expr);
                continue;
            }
            throw std::runtime_error("column constraints other than inline primary key and constant DEFAULT are not supported");
        }
        const std::string column = column_def->colname;
        std::string definition = quote_ident(column) + " " + type_name_sql(column_def->type_name);
        if (!default_sql.empty()) definition += " DEFAULT " + default_sql;
        columns.push_back(column);
        defs.push_back(definition);
        if (primary_key) pk_columns.push_back(column);
    }

    std::string column_default_sql(PgQuery__ColumnDef *column_def) {
        if (!column_def) return "";
        for (std::size_t i = 0; i < column_def->n_constraints; ++i) {
            PgQuery__Node *node = column_def->constraints[i];
            if (node && node->node_case == PG_QUERY__NODE__NODE_CONSTRAINT &&
                node->constraint &&
                node->constraint->contype == PG_QUERY__CONSTR_TYPE__CONSTR_DEFAULT) {
                return expression_sql(node->constraint->raw_expr);
            }
        }
        return "";
    }

    std::optional<std::string> sql_value_expression_for_update(
        const PgQuery__Node *node,
        const std::vector<Value> &params,
        std::vector<Value> &bound,
        const std::string &row_alias
    ) {
        if (!node) return std::nullopt;
        switch (node->node_case) {
        case PG_QUERY__NODE__NODE_PARAM_REF: {
            int index = node->param_ref->number - 1;
            if (index < 0 || static_cast<std::size_t>(index) >= params.size()) {
                return std::nullopt;
            }
            bound.push_back(params[static_cast<std::size_t>(index)]);
            return "?";
        }
        case PG_QUERY__NODE__NODE_COLUMN_REF:
            return row_alias + "." + quote_ident(column_ref_name(node->column_ref));
        case PG_QUERY__NODE__NODE_A_CONST: {
            auto *c = node->a_const;
            if (!c || c->isnull) return "NULL";
            switch (c->val_case) {
            case PG_QUERY__A__CONST__VAL_IVAL:
                return std::to_string(c->ival->ival);
            case PG_QUERY__A__CONST__VAL_FVAL:
                return c->fval->fval ? std::string(c->fval->fval) : "0";
            case PG_QUERY__A__CONST__VAL_BOOLVAL:
                return c->boolval->boolval ? "TRUE" : "FALSE";
            case PG_QUERY__A__CONST__VAL_SVAL:
                return sql_string_literal(c->sval->sval ? c->sval->sval : "");
            default:
                return std::nullopt;
            }
        }
        case PG_QUERY__NODE__NODE_A_EXPR: {
            auto *expr = node->a_expr;
            if (!expr) return std::nullopt;
            auto left = sql_value_expression_for_update(expr->lexpr, params, bound, row_alias);
            auto right = sql_value_expression_for_update(expr->rexpr, params, bound, row_alias);
            if (!left || !right) return std::nullopt;
            return "(" + *left + " " + operator_name(expr) + " " + *right + ")";
        }
        case PG_QUERY__NODE__NODE_TYPE_CAST: {
            auto inner = sql_value_expression_for_update(
                node->type_cast->arg,
                params,
                bound,
                row_alias
            );
            if (!inner) return std::nullopt;
            return *inner + "::" + type_name_sql(node->type_cast->type_name);
        }
        case PG_QUERY__NODE__NODE_SQLVALUE_FUNCTION: {
            auto *expr = node->sqlvalue_function;
            if (!expr) return std::nullopt;
            switch (expr->op) {
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIMESTAMP:
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIMESTAMP_N:
                return "CURRENT_TIMESTAMP";
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIMESTAMP:
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIMESTAMP_N:
                return "LOCALTIMESTAMP";
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_DATE:
                return "CURRENT_DATE";
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIME:
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_CURRENT_TIME_N:
                return "CURRENT_TIME";
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIME:
            case PG_QUERY__SQLVALUE_FUNCTION_OP__SVFOP_LOCALTIME_N:
                return "LOCALTIME";
            default:
                return std::nullopt;
            }
        }
        default:
            return std::nullopt;
        }
    }

    std::string comma_join_prefixed_columns(
        const std::string &alias,
        const std::vector<std::string> &columns
    ) {
        std::vector<std::string> parts;
        parts.reserve(columns.size());
        for (const auto &column : columns) {
            parts.push_back(alias + "." + quote_ident(column));
        }
        return join_strings(parts, ", ");
    }

    std::optional<std::int64_t> try_postgres_point_update_cte(
        const NativeUpdatePlan &plan,
        const NativeTableMeta &meta,
        const std::vector<Value> &params
    ) {
        if (store_.dialect() != "postgres") return std::nullopt;
        if (plan.assignments.empty()) return std::nullopt;
        const auto point_key = point_key_values_from_predicate(plan.where, meta, params);
        if (!point_key) return std::nullopt;
        for (const auto &[column, _] : plan.assignments) {
            if (std::find(meta.pk_columns.begin(), meta.pk_columns.end(), column) != meta.pk_columns.end()) {
                return std::nullopt;
            }
            (void)column_index(meta.columns, column);
        }

        std::vector<Value> bound;
        bound.reserve(point_key->size() + params.size() + 16);
        const std::string table = quote_ident(meta.physical_name);
        const std::string data_cols = comma_join_quoted(meta.columns);
        const std::string all_cols = data_cols + ", \"live_lo\", \"live_hi\", \"writer_segment_id\", \"deleted\"";

        std::string key_where;
        std::string key_join;
        for (std::size_t i = 0; i < meta.pk_columns.size(); ++i) {
            if (i) {
                key_where += " AND ";
                key_join += " AND ";
            }
            key_where += quote_ident(meta.pk_columns[i]) + " = ?";
            key_join += "p." + quote_ident(meta.pk_columns[i]) + " = v." + quote_ident(meta.pk_columns[i]);
            bound.push_back((*point_key)[i]);
        }
        bound.push_back(segment_.branch_point);
        bound.push_back(segment_.branch_point);
        bound.push_back(segment_.live_hi);
        bound.push_back(segment_.live_lo);
        bound.push_back(segment_.live_lo);
        bound.push_back(segment_.live_lo);
        bound.push_back(segment_.live_hi);
        bound.push_back(segment_.live_hi);

        std::unordered_map<std::string, PgQuery__Node *> assignment_by_column;
        assignment_by_column.reserve(plan.assignments.size());
        for (const auto &[column, expr] : plan.assignments) {
            assignment_by_column.emplace(column, expr);
        }

        std::vector<std::string> replacement_exprs;
        replacement_exprs.reserve(meta.columns.size());
        for (const auto &column : meta.columns) {
            if (auto found = assignment_by_column.find(column); found != assignment_by_column.end()) {
                auto expr = sql_value_expression_for_update(found->second, params, bound, "v");
                if (!expr) return std::nullopt;
                replacement_exprs.push_back(*expr);
            } else {
                replacement_exprs.push_back("v." + quote_ident(column));
            }
        }
        bound.push_back(segment_.live_lo);
        bound.push_back(segment_.live_hi);
        bound.push_back(segment_.segment_id);

        const std::string sql =
            "WITH visible AS ("
            "SELECT " + data_cols + " FROM " + table +
            " WHERE " + key_where +
            " AND live_lo <= ? AND ? < live_hi AND deleted = FALSE FOR UPDATE"
            "), deleted_rows AS ("
            "DELETE FROM " + table + " AS p USING visible AS v "
            "WHERE " + key_join +
            " AND p.live_lo < ? AND ? < p.live_hi "
            "RETURNING " + comma_join_prefixed_columns("p", meta.columns) +
            ", p.live_lo, p.live_hi, p.writer_segment_id, p.deleted"
            "), left_rows AS ("
            "INSERT INTO " + table + " (" + all_cols + ") "
            "SELECT " + data_cols + ", live_lo, ?, writer_segment_id, deleted "
            "FROM deleted_rows WHERE live_lo < ? RETURNING 1"
            "), right_rows AS ("
            "INSERT INTO " + table + " (" + all_cols + ") "
            "SELECT " + data_cols + ", ?, live_hi, writer_segment_id, deleted "
            "FROM deleted_rows WHERE ? < live_hi RETURNING 1"
            "), replacement AS ("
            "INSERT INTO " + table + " (" + all_cols + ") "
            "SELECT " + join_strings(replacement_exprs, ", ") +
            ", ?, ?, ?, FALSE FROM visible AS v "
            "WHERE EXISTS (SELECT 1 FROM deleted_rows) RETURNING 1"
            ") SELECT COUNT(*) FROM replacement";

        QueryResult result = store_.driver().query_result(sql, bound);
        if (result.rows.empty() || result.rows[0].empty()) {
            return std::int64_t{0};
        }
        return native_as_int(result.rows[0][0]);
    }

    std::string replace_column_type_sql(
        const std::string &definition,
        const std::string &column,
        const std::string &type_sql
    ) {
        const std::string marker = " DEFAULT ";
        const std::size_t pos = definition.find(marker);
        const std::string default_clause = pos == std::string::npos ? "" : definition.substr(pos);
        return quote_ident(column) + " " + type_sql + default_clause;
    }

    void create_schema_version_secondary_indexes(const NativeTableMeta &meta, const std::string &table) {
        // Schema-version copies are ordinary interval tables. They must carry
        // the physical interval indexes used by visible-row scans and staged
        // writes, plus every registered logical index that applies to the new
        // column set.
        std::vector<std::string> sqls{
            store_.pk_hi_index_sql(meta.physical_name, meta.pk_columns),
            store_.writer_segment_index_sql(meta.physical_name, meta.pk_columns),
        };
        auto rows = store_.driver().query(
            "SELECT index_name, columns FROM _chronos_branch_indexes "
            "WHERE backend = 'interval' AND table_name = ?",
            {table}
        );
        for (const auto &row : rows) {
            std::vector<std::string> columns = parse_json_string_array(native_as_string(row[1]));
            bool missing = false;
            for (const auto &column : columns) {
                if (std::find(meta.columns.begin(), meta.columns.end(), column) == meta.columns.end()) {
                    missing = true;
                    break;
                }
            }
            if (missing) continue;
            columns.push_back("live_lo");
            columns.push_back("live_hi");
            columns.push_back("deleted");
            sqls.push_back(
                "CREATE INDEX IF NOT EXISTS " +
                quote_ident(store_.physical_logical_index_name(meta, table, native_as_string(row[0]))) +
                " ON " + quote_ident(meta.physical_name) +
                " (" + comma_join_quoted(columns) + ")"
            );
        }
        store_.defer_or_execute_schema_index_sqls(sqls);
    }

    std::optional<std::int64_t> try_update_private_schema_table_in_place(
        const NativeUpdatePlan &plan,
        const NativeTableMeta &meta,
        const std::vector<Value> &params
    ) {
        if (!private_schema_table_is_canonical(meta)) return std::nullopt;
        for (const auto &[column, _] : plan.assignments) {
            if (std::find(meta.pk_columns.begin(), meta.pk_columns.end(), column) != meta.pk_columns.end()) {
                return std::nullopt;
            }
            (void)column_index(meta.columns, column);
        }
        const auto point_key = point_key_values_from_predicate(plan.where, meta, params);
        QueryResult current = point_key
            ? select_visible_rows(meta, key_where_sql(meta.pk_columns), *point_key, meta.columns)
            : select_visible_rows(meta, "", {}, meta.columns);
        SubqueryEvaluator evaluator = [this, &params](const PgQuery__SelectStmt *select) {
            return this->evaluate_select_values(select, params);
        };
        std::int64_t count = 0;
        const bool started_tx = !store_.driver().in_transaction();
        if (started_tx) store_.driver().execute(store_.dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            for (const auto &row : current.rows) {
                auto row_map = row_map_for(meta.columns, row);
                if (!eval_ast_predicate(plan.where, row_map, params, &evaluator)) continue;
                std::string sql = "UPDATE " + quote_ident(meta.physical_name) + " SET ";
                std::vector<Value> bound;
                for (std::size_t i = 0; i < plan.assignments.size(); ++i) {
                    if (i) sql += ", ";
                    sql += quote_ident(plan.assignments[i].first) + " = ?";
                    bound.push_back(eval_ast_value(plan.assignments[i].second, row_map, params, &evaluator));
                }
                if (!plan.assignments.empty()) sql += ", ";
                sql += quote_ident("writer_segment_id") + " = ?, " + quote_ident("deleted") + " = FALSE WHERE ";
                bound.push_back(segment_.segment_id);
                for (std::size_t i = 0; i < meta.pk_columns.size(); ++i) {
                    if (i) sql += " AND ";
                    sql += quote_ident(meta.pk_columns[i]) + " = ?";
                    bound.push_back(row[column_index(meta.columns, meta.pk_columns[i])]);
                }
                count += store_.driver().execute_changes(sql, bound);
            }
            commit_if_started(started_tx);
        } catch (...) {
            rollback_if_started(started_tx);
            throw;
        }
        return count;
    }

    std::optional<std::int64_t> try_delete_private_schema_table_in_place(
        const NativeDeletePlan &plan,
        const NativeTableMeta &meta,
        const std::vector<Value> &params
    ) {
        if (!private_schema_table_is_canonical(meta)) return std::nullopt;
        const auto point_key = point_key_values_from_predicate(plan.where, meta, params);
        QueryResult current = point_key
            ? select_visible_rows(meta, key_where_sql(meta.pk_columns), *point_key, meta.columns)
            : select_visible_rows(meta, "", {}, meta.columns);
        SubqueryEvaluator evaluator = [this, &params](const PgQuery__SelectStmt *select) {
            return this->evaluate_select_values(select, params);
        };
        std::int64_t count = 0;
        const bool started_tx = !store_.driver().in_transaction();
        if (started_tx) store_.driver().execute(store_.dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            for (const auto &row : current.rows) {
                auto row_map = row_map_for(meta.columns, row);
                if (!eval_ast_predicate(plan.where, row_map, params, &evaluator)) continue;
                std::string sql = "DELETE FROM " + quote_ident(meta.physical_name) + " WHERE ";
                std::vector<Value> bound;
                for (std::size_t i = 0; i < meta.pk_columns.size(); ++i) {
                    if (i) sql += " AND ";
                    sql += quote_ident(meta.pk_columns[i]) + " = ?";
                    bound.push_back(row[column_index(meta.columns, meta.pk_columns[i])]);
                }
                count += store_.driver().execute_changes(sql, bound);
            }
            commit_if_started(started_tx);
        } catch (...) {
            rollback_if_started(started_tx);
            throw;
        }
        return count;
    }

    std::int64_t execute_insert(const NativeInsertPlan &plan, const std::vector<Value> &params) {
        NativeTableMeta meta = table_meta_for(plan.table);
        const auto defaults = column_default_values(meta);
        NativeRows rows;
        std::unordered_map<std::string, Value> empty_row;
        for (const auto &tuple : plan.value_tuples) {
            std::unordered_map<std::string, Value> partial;
            for (std::size_t i = 0; i < plan.columns.size(); ++i) {
                partial[plan.columns[i]] = eval_ast_value(tuple[i], empty_row, params);
            }
            std::vector<Value> row;
            row.reserve(meta.columns.size());
            for (const auto &column : meta.columns) {
                auto found = partial.find(column);
                if (found != partial.end()) {
                    row.push_back(found->second);
                } else if (auto default_found = defaults.find(column); default_found != defaults.end()) {
                    row.push_back(default_found->second);
                } else {
                    row.push_back(Value(std::monostate{}));
                }
            }
            rows.push_back(std::move(row));
        }
        std::unordered_map<NativeRowKey, bool, NativeRowKeyHash> seen;
        NativeRows insert_rows;
        for (const auto &row : rows) {
            NativeRowKey key;
            for (const auto &pk : meta.pk_columns) {
                key.values.push_back(row[column_index(meta.columns, pk)]);
            }
            if (seen.find(key) != seen.end()) {
                if (plan.ignore_conflicts) continue;
                throw std::runtime_error("duplicate key value violates unique constraint");
            }
            seen.emplace(key, true);
            std::string where;
            std::vector<Value> key_params;
            for (std::size_t i = 0; i < meta.pk_columns.size(); ++i) {
                if (i) where += " AND ";
                where += quote_ident(meta.pk_columns[i]) + " = ?";
                key_params.push_back(row[column_index(meta.columns, meta.pk_columns[i])]);
            }
            if (!select_visible_rows(meta, where, key_params, meta.pk_columns).rows.empty()) {
                if (plan.ignore_conflicts) continue;
                throw std::runtime_error("duplicate key value violates unique constraint");
            }
            insert_rows.push_back(row);
        }
        if (insert_rows.empty()) return 0;
        // Physical PK uniqueness includes live_lo, so the database cannot
        // enforce logical uniqueness for a branch view.  After probing visible
        // rows above, INSERT is just an interval upsert into the session segment.
        const bool started_tx = !store_.driver().in_transaction();
        if (started_tx) store_.driver().execute(store_.dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            upsert_rows(plan.table, meta.columns, meta.pk_columns, insert_rows, false);
            commit_if_started(started_tx);
        } catch (...) {
            rollback_if_started(started_tx);
            throw;
        }
        return static_cast<std::int64_t>(insert_rows.size());
    }

    std::vector<Value> evaluate_select_values(const PgQuery__SelectStmt *select, const std::vector<Value> &params) {
        if (!select || select->n_from_clause != 1 || select->n_target_list != 1 ||
            select->n_group_clause || select->having_clause || select->n_values_lists ||
            select->n_sort_clause || select->limit_count || select->limit_offset) {
            throw std::runtime_error("unsupported native subquery shape");
        }
        auto *from_node = select->from_clause[0];
        if (!from_node || from_node->node_case != PG_QUERY__NODE__NODE_RANGE_VAR) {
            throw std::runtime_error("native subquery must read one table");
        }
        auto *target_node = select->target_list[0];
        if (!target_node || target_node->node_case != PG_QUERY__NODE__NODE_RES_TARGET ||
            !target_node->res_target->val) {
            throw std::runtime_error("native subquery must select one expression");
        }
        NativeTableMeta meta = table_meta_for(relation_name(from_node->range_var));
        QueryResult visible = select_visible_rows(meta, "", {}, meta.columns);
        SubqueryEvaluator evaluator = [this, &params](const PgQuery__SelectStmt *nested) {
            return this->evaluate_select_values(nested, params);
        };
        std::vector<Value> out;
        for (const auto &row : visible.rows) {
            auto row_map = row_map_for(meta.columns, row);
            if (eval_ast_predicate(select->where_clause, row_map, params, &evaluator)) {
                out.push_back(eval_ast_value(target_node->res_target->val, row_map, params, &evaluator));
            }
        }
        return out;
    }

    std::int64_t execute_update(const NativeUpdatePlan &plan, const std::vector<Value> &params) {
        NativeTableMeta meta = table_meta_for(plan.table);
        if (auto private_count = try_update_private_schema_table_in_place(plan, meta, params)) {
            return *private_count;
        }
        if (auto point_count = try_postgres_point_update_cte(plan, meta, params)) {
            return *point_count;
        }
        const auto point_key = point_key_values_from_predicate(plan.where, meta, params);
        QueryResult current = point_key
            ? select_visible_rows(meta, key_where_sql(meta.pk_columns), *point_key, meta.columns)
            : select_visible_rows(meta, "", {}, meta.columns);
        // General UPDATE materializes the current branch-visible rows, evaluates
        // assignments in memory using libpg_query's expression tree, and then
        // routes replacements through the same interval upsert path as INSERT.
        // Point updates may take a faster SQL CTE above, but the result is the
        // same physical splice.
        SubqueryEvaluator evaluator = [this, &params](const PgQuery__SelectStmt *select) {
            return this->evaluate_select_values(select, params);
        };
        NativeRows replacement_rows;
        replacement_rows.reserve(current.rows.size());
        for (auto row : current.rows) {
            auto row_map = row_map_for(meta.columns, row);
            if (!eval_ast_predicate(plan.where, row_map, params, &evaluator)) {
                continue;
            }
            for (const auto &[column, expr] : plan.assignments) {
                row[column_index(meta.columns, column)] = eval_ast_value(expr, row_map, params, &evaluator);
            }
            replacement_rows.push_back(std::move(row));
        }
        if (!replacement_rows.empty()) {
            const bool started_tx = !store_.driver().in_transaction();
            if (started_tx) store_.driver().execute(store_.dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
            try {
                upsert_rows(plan.table, meta.columns, meta.pk_columns, replacement_rows, false);
                commit_if_started(started_tx);
            } catch (...) {
                rollback_if_started(started_tx);
                throw;
            }
        }
        return static_cast<std::int64_t>(replacement_rows.size());
    }

    std::int64_t execute_delete(const NativeDeletePlan &plan, const std::vector<Value> &params) {
        NativeTableMeta meta = table_meta_for(plan.table);
        if (auto private_count = try_delete_private_schema_table_in_place(plan, meta, params)) {
            return *private_count;
        }
        const auto point_key = point_key_values_from_predicate(plan.where, meta, params);
        QueryResult visible = point_key
            ? select_visible_rows(meta, key_where_sql(meta.pk_columns), *point_key, meta.columns)
            : select_visible_rows(meta, "", {}, meta.columns);
        // DELETE writes tombstone versions over the current branch interval.
        // Inherited physical rows are not removed globally; readers outside the
        // deleting branch interval continue resolving to the preserved fragments.
        SubqueryEvaluator evaluator = [this, &params](const PgQuery__SelectStmt *select) {
            return this->evaluate_select_values(select, params);
        };
        NativeRows rows;
        for (const auto &row : visible.rows) {
            auto row_map = row_map_for(meta.columns, row);
            if (eval_ast_predicate(plan.where, row_map, params, &evaluator)) {
                rows.push_back(row);
            }
        }
        if (!rows.empty()) {
            const bool started_tx = !store_.driver().in_transaction();
            if (started_tx) store_.driver().execute(store_.dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
            try {
                upsert_rows(plan.table, meta.columns, meta.pk_columns, rows, true);
                commit_if_started(started_tx);
            } catch (...) {
                rollback_if_started(started_tx);
                throw;
            }
        }
        return static_cast<std::int64_t>(rows.size());
    }

    std::string physical_table_for(const std::string &logical_table) {
        return table_meta_for(logical_table).physical_name;
    }

    NativeBranchStoreImpl &store_;
    std::string branch_id_;
    NativeBranchSegment segment_;
    bool in_transaction_ = false;
    std::optional<std::vector<NativeTableMeta>> table_metas_cache_;
    std::optional<std::unordered_set<std::string>> known_table_names_cache_;
    std::unordered_map<std::string, NativeTableMeta> table_meta_by_name_;
    std::unordered_map<std::string, std::string> select_rewrite_cache_;
};

} // namespace chronos::native
