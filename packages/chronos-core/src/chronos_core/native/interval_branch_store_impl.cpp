// NativeBranchStoreImpl class-body implementation fragments.
//
// This file is included inside NativeBranchStoreImpl in interval_branch_store.cpp.
// It keeps public/private access sections explicit because these
// methods share the store's drivers, metadata caches, and merge helpers.
//
// Interval metadata table map
// ---------------------------
// Core branch/interval tables:
//   _chronos_branch_interval_branches
//       Schema:
//         branch_id TEXT PRIMARY KEY
//         current_segment_id INTEGER NOT NULL
//         parent_branch_id TEXT
//         child_count INTEGER NOT NULL DEFAULT 0
//         branch_kind TEXT NOT NULL DEFAULT 'mutable'
//         created_at TEXT NOT NULL
//         metadata TEXT NOT NULL
//       One row per branch. current_segment_id is the branch head and therefore
//       the atomic visibility pointer. Fork and branch-transaction commit
//       operations move this pointer with compare-and-swap style UPDATEs.
//
//   _chronos_branch_transaction_commits
//       Schema:
//         merge_segment_id INTEGER PRIMARY KEY
//         continuation_segment_id INTEGER NOT NULL
//         target_branch_id TEXT NOT NULL UNIQUE
//         old_target_segment_id INTEGER NOT NULL
//         source_branch_id TEXT
//         participant_stores TEXT NOT NULL
//         created_at TEXT NOT NULL
//         metadata TEXT NOT NULL DEFAULT '{}'
//       One active commit record per target branch. This is the branch
//       transaction commit reservation: it names the unpublished merge and
//       continuation segments while commit writes are being installed. The
//       UNIQUE target constraint prevents competing commit segment pairs, so visible reads can
//       keep using the fast interval predicate without a reachability join.
//
//   _chronos_branch_interval_segments
//       Schema:
//         segment_id INTEGER PRIMARY KEY
//         parent_segment_id INTEGER
//         owner_branch_id TEXT
//         segment_kind TEXT NOT NULL DEFAULT 'mutable'
//         live_lo <interval_type> NOT NULL
//         live_hi <interval_type> NOT NULL
//         branch_point <interval_type> NOT NULL
//         created_at TEXT NOT NULL
//         metadata TEXT NOT NULL
//         CHECK (live_lo <= branch_point)
//         CHECK (branch_point < live_hi)
//       The interval allocation tree.  Each segment owns [live_lo, live_hi) and
//       exposes branch_point as the read timestamp used by visible-row queries.
//       Current segment_kind values are deliberately few: mutable, fork_base,
//       merge, and checkpoint.  A branch transaction commit writes source
//       deltas into an immutable merge segment, then publishes a mutable
//       continuation segment as the branch head.
//
//   _chronos_branch_interval_checkpoints
//       Schema:
//         checkpoint_id TEXT PRIMARY KEY
//         branch_id TEXT NOT NULL
//         segment_id INTEGER NOT NULL
//         created_at TEXT NOT NULL
//         metadata TEXT NOT NULL
//       Stable named references to segment ids plus checkpoint metadata.  The
//       segment row is marked segment_kind='checkpoint' so GC preserves it.
//
// Compatibility registries:
//   _chronos_branch_tables
//       Schema:
//         table_name TEXT PRIMARY KEY
//         physical_table TEXT NOT NULL
//         pk_columns TEXT NOT NULL       JSON string array
//         columns TEXT NOT NULL          JSON string array
//         column_defs TEXT NOT NULL      JSON string array
//         backend TEXT NOT NULL
//       Logical table registry.  Maps user table name to the physical interval
//       table and stores primary-key/schema JSON used by native sessions and by
//       legacy Python compatibility helpers.
//
//   _chronos_branch_indexes
//       Schema:
//         backend TEXT NOT NULL
//         index_name TEXT NOT NULL
//         table_name TEXT NOT NULL
//         columns TEXT NOT NULL          JSON string array
//         PRIMARY KEY (backend, index_name)
//       Logical secondary-index registry.  Used to recreate physical indexes
//       when schema branching creates a private physical table version.
//
// Schema-branching tables, created lazily only when schema branching is enabled:
//   _chronos_branch_table_schema_versions
//       Schema:
//         backend TEXT NOT NULL
//         table_name TEXT NOT NULL
//         schema_version_id TEXT NOT NULL
//         parent_schema_version_id TEXT
//         physical_table TEXT NOT NULL
//         pk_columns TEXT NOT NULL       JSON string array
//         columns TEXT NOT NULL          JSON string array
//         column_defs TEXT NOT NULL      JSON string array
//         ddl_op TEXT NOT NULL
//         created_at TEXT NOT NULL
//         metadata TEXT NOT NULL
//         PRIMARY KEY (backend, schema_version_id)
//       Immutable schema records for logical tables.  DDL creates a new version
//       unless the current version is private to the branch.
//
//   _chronos_branch_table_bindings
//       Schema:
//         backend TEXT NOT NULL
//         table_name TEXT NOT NULL
//         schema_version_id TEXT
//         tombstone INTEGER NOT NULL DEFAULT 0
//         live_lo <interval_type> NOT NULL
//         live_hi <interval_type> NOT NULL
//         created_at TEXT NOT NULL
//         metadata TEXT NOT NULL
//         PRIMARY KEY (backend, table_name, live_lo)
//       Interval-versioned logical table -> schema_version binding.  A tombstone
//       binding represents branch-local DROP TABLE without deleting inherited
//       rows for other branches.
//
// Allocators:
//   _chronos_branch_interval_segment_id_seq        PostgreSQL sequence
//   _chronos_branch_interval_segment_id_alloc      SQLite singleton row
//       SQLite schema:
//         singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
//         next_segment_id INTEGER NOT NULL
//       Monotonic segment-id allocation.  Gaps are safe; interval bounds, not
//       segment id order, define visibility.
//
// Physical user-data interval tables:
//   _chronos_b_interval_<logical_table>
//       Schema:
//         <user columns...>
//         live_lo <interval_type> NOT NULL
//         live_hi <interval_type> NOT NULL
//         writer_segment_id INTEGER NOT NULL
//         deleted BOOLEAN NOT NULL DEFAULT FALSE
//         PRIMARY KEY (<pk columns...>, live_lo)
//       Secondary indexes:
//         (<pk columns...>, live_hi)
//         (writer_segment_id, <pk columns...>) on non-Postgres backends
//         (writer_segment_id) WHERE writer_segment_id > 1 on Postgres
//       These optional data-plane indexes default to enabled. The physical
//       table names are stored in _chronos_branch_tables and are created by
//       this metadata/control layer.
//




// -----------------------------------------------------------------------------
// Metadata-plane table registration and logical secondary index registry
// Source: interval_branch_store_control.cpp
// -----------------------------------------------------------------------------
    std::string postgres_column_type(const std::vector<Value> &row) {
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

    std::pair<std::vector<std::string>, std::vector<std::string>> table_defs(const std::string &table) {
        std::vector<std::string> columns;
        std::vector<std::string> defs;
        if (driver().dialect() == "postgres") {
            auto [schema, table_name] = split_postgres_table_name(table);
            auto rows = driver().query(
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
                defs.push_back(quote_ident(name) + " " + postgres_column_type(row));
            }
            return {columns, defs};
        }
        auto rows = driver().query("PRAGMA table_info(" + quote_table_name(table) + ")");
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

    void ensure_segment_id_allocator() {
        if (metadata_dialect() == "postgres") {
            // Segment ids are globally monotonic inside the metadata plane.
            // Postgres can allocate them from a sequence inside the same
            // transaction that carves a branch or branch-transaction segment pair.
            driver_->execute(
                "CREATE SEQUENCE IF NOT EXISTS _chronos_branch_interval_segment_id_seq "
                "AS integer START WITH 2"
            );
            driver_->execute(
                "SELECT setval("
                "'_chronos_branch_interval_segment_id_seq', "
                "GREATEST(2, "
                "COALESCE((SELECT MAX(segment_id) + 1 "
                "FROM _chronos_branch_interval_segments), 2), "
                "(SELECT last_value FROM _chronos_branch_interval_segment_id_seq)), true)"
            );
            return;
        }
        // SQLite has no sequence object, so one singleton row is updated under
        // BEGIN IMMEDIATE.  Gaps are harmless: ids are identifiers, not version
        // order, and the interval bounds carry the visibility semantics.
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_interval_segment_id_alloc ("
            "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
            "next_segment_id INTEGER NOT NULL)"
        );
        driver_->execute(
            "INSERT OR IGNORE INTO _chronos_branch_interval_segment_id_alloc "
            "(singleton, next_segment_id) "
            "SELECT 1, COALESCE((SELECT MAX(segment_id) + 1 "
            "FROM _chronos_branch_interval_segments), 2)"
        );
    }

    void ensure_branch_transaction_commit_table() {
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_transaction_commits ("
            "merge_segment_id INTEGER PRIMARY KEY, "
            "continuation_segment_id INTEGER NOT NULL, "
            "target_branch_id TEXT NOT NULL UNIQUE, "
            "old_target_segment_id INTEGER NOT NULL, "
            "source_branch_id TEXT, "
            "participant_stores TEXT NOT NULL, "
            "created_at TEXT NOT NULL, "
            "metadata TEXT NOT NULL DEFAULT '{}')"
        );
    }

    void ensure_session_epoch_tables() {
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_session_barriers ("
            "branch_id TEXT PRIMARY KEY, "
            "barrier_id TEXT NOT NULL, "
            "operation TEXT NOT NULL, "
            "created_at_ms BIGINT NOT NULL)"
        );
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_sessions ("
            "session_id TEXT PRIMARY KEY, "
            "branch_id TEXT NOT NULL, "
            "session_epoch BIGINT NOT NULL DEFAULT 0, "
            "required_epoch BIGINT NOT NULL DEFAULT 0, "
            "barrier_id TEXT, "
            "status TEXT NOT NULL DEFAULT 'active', "
            "lease_expires_ms BIGINT NOT NULL)"
        );
        driver_->execute(
            "CREATE INDEX IF NOT EXISTS _chronos_idx_branch_sessions_live "
            "ON _chronos_branch_sessions (branch_id, lease_expires_ms)"
        );
        driver_->execute(
            "CREATE INDEX IF NOT EXISTS _chronos_idx_branch_sessions_barrier "
            "ON _chronos_branch_sessions (barrier_id, status)"
        );
    }

    void ensure_metadata(bool enable_schema_branching) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            // The Python adapter and the native store can use different
            // PostgreSQL connections.  Serialize bootstrap on the connection
            // that actually issues the CREATE TABLE statements.
            lock_schema_metadata();
            const std::string interval_type = interval_sql_type();
            // Logical table registry.  Each user-facing table name maps to one
            // physical interval table plus JSON-encoded schema/PK metadata.
            // Existing Python compatibility helpers depend on this table name.
            driver_->execute(
                "CREATE TABLE IF NOT EXISTS _chronos_branch_tables ("
                "table_name TEXT PRIMARY KEY, "
                "physical_table TEXT NOT NULL, "
                "pk_columns TEXT NOT NULL, "
                "columns TEXT NOT NULL, "
                "column_defs TEXT NOT NULL, "
                "backend TEXT NOT NULL)"
            );
            // Logical secondary index registry.  Schema-branching DDL uses this
            // to recreate physical indexes when a private schema copy is made.
            driver_->execute(
                "CREATE TABLE IF NOT EXISTS _chronos_branch_indexes ("
                "backend TEXT NOT NULL, "
                "index_name TEXT NOT NULL, "
                "table_name TEXT NOT NULL, "
                "columns TEXT NOT NULL, "
                "PRIMARY KEY (backend, index_name))"
            );
            // Mutable branch heads.  current_segment_id is the only value a
            // checkout must read to get its branch read/write point.  Atomic
            // merge publish is implemented by installing merge/continuation segments
            // and then swapping this branch head in the metadata transaction.
            driver_->execute(
                "CREATE TABLE IF NOT EXISTS _chronos_branch_interval_branches ("
                "branch_id TEXT PRIMARY KEY, "
                "current_segment_id INTEGER NOT NULL, "
                "parent_branch_id TEXT, "
                "child_count INTEGER NOT NULL DEFAULT 0, "
                "branch_kind TEXT NOT NULL DEFAULT 'mutable', "
                "created_at TEXT NOT NULL, "
                "metadata TEXT NOT NULL)"
            );
            driver_->execute(
                "CREATE INDEX IF NOT EXISTS _chronos_idx_interval_branches_parent "
                "ON _chronos_branch_interval_branches (parent_branch_id)"
            );
            ensure_branch_transaction_commit_table();
            ensure_session_epoch_tables();
            // Segment allocation tree.  [live_lo, live_hi) is the write interval
            // owned by the segment; branch_point is the read timestamp that
            // resolves inherited rows.  Forks and merges only add segment rows
            // and move branch heads, which is why data stores do not need their
            // own branch metadata in a polystore deployment.
            driver_->execute(
                "CREATE TABLE IF NOT EXISTS _chronos_branch_interval_segments ("
                "segment_id INTEGER PRIMARY KEY, "
                "parent_segment_id INTEGER, "
                "owner_branch_id TEXT, "
                "segment_kind TEXT NOT NULL DEFAULT 'mutable', "
                "live_lo " + interval_type + " NOT NULL, "
                "live_hi " + interval_type + " NOT NULL, "
                "branch_point " + interval_type + " NOT NULL, "
                "created_at TEXT NOT NULL, "
                "metadata TEXT NOT NULL, "
                "CHECK (live_lo <= branch_point), "
                "CHECK (branch_point < live_hi))"
            );
            ensure_segment_id_allocator();
            auto main = driver_->query(
                "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = 'main' LIMIT 1"
            );
            if (main.empty()) {
                const std::string now = current_timestamp_string();
                driver_->execute(
                    "INSERT INTO _chronos_branch_interval_segments "
                    "(segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi, "
                    " branch_point, created_at, metadata) "
                    "VALUES (1, NULL, 'main', 'mutable', 0, ?, ?, ?, '{}')",
                    {max_interval_value(), root_branch_point_value(), now}
                );
                driver_->execute(
                    "INSERT INTO _chronos_branch_interval_branches "
                    "(branch_id, current_segment_id, parent_branch_id, child_count, branch_kind, created_at, metadata) "
                    "VALUES ('main', 1, NULL, 0, 'mutable', ?, '{}')",
                    {now}
                );
            }
            // Immutable refs.  A checkpoint records the segment id whose
            // branch_point defines the stable read view; marking the segment as
            // checkpoint keeps GC from treating it as transient branch history.
            driver_->execute(
                "CREATE TABLE IF NOT EXISTS _chronos_branch_interval_checkpoints ("
                "checkpoint_id TEXT PRIMARY KEY, "
                "branch_id TEXT NOT NULL, "
                "segment_id INTEGER NOT NULL, "
                "created_at TEXT NOT NULL, "
                "metadata TEXT NOT NULL)"
            );
            driver_->execute(
                "UPDATE _chronos_branch_interval_segments "
                "SET segment_kind = 'checkpoint' "
                "WHERE segment_id IN (SELECT segment_id FROM _chronos_branch_interval_checkpoints) "
                "AND segment_kind = 'mutable'"
            );
            if (enable_schema_branching) {
                ensure_schema_branching_tables();
            }
            if (started_tx) driver_->execute("COMMIT");
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    std::string ensure_base_schema_version(const NativeTableMeta &meta, const std::string &ddl_op) {
        ensure_schema_branching_tables();
        auto existing = driver_->query(
            "SELECT schema_version_id "
            "FROM _chronos_branch_table_schema_versions "
            "WHERE backend = 'interval' AND table_name = ? AND physical_table = ?",
            {meta.logical_name, meta.physical_name}
        );
        if (!existing.empty()) return native_as_string(existing[0][0]);
        const std::string schema_id = schema_version_id(meta.logical_name);
        const std::string now = current_timestamp_string();
        driver_->execute(
            "INSERT INTO _chronos_branch_table_schema_versions "
            "(backend, table_name, schema_version_id, parent_schema_version_id, physical_table, "
            " pk_columns, columns, column_defs, ddl_op, created_at, metadata) "
            "VALUES ('interval', ?, ?, NULL, ?, ?, ?, ?, ?, ?, '{}')",
            {
                meta.logical_name,
                schema_id,
                meta.physical_name,
                json_string_array(meta.pk_columns),
                json_string_array(meta.columns),
                json_string_array(meta.column_defs),
                ddl_op,
                now,
            }
        );
        driver_->execute(
            "INSERT INTO _chronos_branch_table_bindings "
            "(backend, table_name, schema_version_id, tombstone, live_lo, live_hi, created_at, metadata) "
            "VALUES ('interval', ?, ?, 0, 0, ?, ?, '{}')",
            {meta.logical_name, schema_id, max_interval_value(), now}
        );
        return schema_id;
    }

    void register_table(const std::string &table, const std::vector<std::string> &primary_key, bool enable_schema_branching) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            driver().refresh_catalog();
            auto [columns, defs] = table_defs(table);
            std::unordered_set<std::string> column_set(columns.begin(), columns.end());
            for (const auto &pk : primary_key) {
                if (column_set.find(pk) == column_set.end()) {
                    throw std::runtime_error("primary key columns missing from " + table + ": " + pk);
                }
            }
            auto existing = driver_->query(
                "SELECT physical_table, pk_columns, columns, column_defs "
                "FROM _chronos_branch_tables WHERE backend = 'interval' AND table_name = ?",
                {table}
            );
            if (!existing.empty()) {
                NativeTableMeta meta{
                    table,
                    native_as_string(existing[0][0]),
                    parse_json_string_array(native_as_string(existing[0][2])),
                    parse_json_string_array(native_as_string(existing[0][1])),
                    parse_json_string_array(native_as_string(existing[0][3])),
                    "",
                    "",
                    false,
                };
                std::unordered_set<std::string> old_columns(meta.columns.begin(), meta.columns.end());
                bool changed = false;
                for (std::size_t i = 0; i < columns.size(); ++i) {
                    if (old_columns.find(columns[i]) != old_columns.end()) continue;
                    driver().execute(
                        "ALTER TABLE " + quote_ident(meta.physical_name) +
                        " ADD COLUMN " + defs[i]
                    );
                    meta.columns.push_back(columns[i]);
                    meta.column_defs.push_back(defs[i]);
                    changed = true;
                }
                if (changed) {
                    driver_->execute(
                        "UPDATE _chronos_branch_tables SET columns = ?, column_defs = ? "
                        "WHERE backend = 'interval' AND table_name = ?",
                        {json_string_array(meta.columns), json_string_array(meta.column_defs), table}
                    );
                }
                if (enable_schema_branching) {
                    ensure_base_schema_version(meta, "register");
                }
                if (create_secondary_indexes()) {
                    driver().execute(pk_hi_index_sql(meta.physical_name, meta.pk_columns));
                    if (create_writer_segment_index()) {
                        driver().execute(writer_segment_index_sql(meta.physical_name, meta.pk_columns));
                    }
                }
                if (started_tx) driver_->execute("COMMIT");
                return;
            }

            const std::string physical = "_chronos_b_interval_" + physical_table_suffix(table);
            create_interval_physical_table(physical, defs, primary_key, create_secondary_indexes());
            std::vector<std::string> target_columns = columns;
            target_columns.push_back("live_lo");
            target_columns.push_back("live_hi");
            target_columns.push_back("writer_segment_id");
            target_columns.push_back("deleted");
            driver().execute(
                "INSERT INTO " + quote_ident(physical) +
                " (" + comma_join_quoted(target_columns) + ") "
                "SELECT " + comma_join_quoted(columns) +
                ", 0, ?, 1, FALSE FROM " + quote_table_name(table),
                {initial_record_live_hi_value()}
            );
            driver_->execute(
                "INSERT INTO _chronos_branch_tables "
                "(table_name, physical_table, pk_columns, columns, column_defs, backend) "
                "VALUES (?, ?, ?, ?, ?, 'interval')",
                {
                    table,
                    physical,
                    json_string_array(primary_key),
                    json_string_array(columns),
                    json_string_array(defs),
                }
            );
            NativeTableMeta meta{table, physical, columns, primary_key, defs, "", "", false};
            if (enable_schema_branching) {
                ensure_base_schema_version(meta, "register");
            }
            if (started_tx) driver_->execute("COMMIT");
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    std::vector<std::string> branches() {
        auto rows = driver_->query("SELECT branch_id FROM _chronos_branch_interval_branches ORDER BY branch_id");
        std::vector<std::string> out;
        for (auto &row : rows) out.push_back(native_as_string(row[0]));
        return out;
    }
    NativeBranchSegment load_segment(const std::string &branch_id) {
        auto rows = driver_->query(
            "SELECT s.segment_id, s.live_lo, s.live_hi, s.branch_point, b.branch_kind "
            "FROM _chronos_branch_interval_branches b "
            "JOIN _chronos_branch_interval_segments s ON s.segment_id = b.current_segment_id "
            "WHERE b.branch_id = ?",
            {branch_id}
        );
        if (rows.empty()) throw std::runtime_error("branch not found: " + branch_id);
        return {
            native_as_int(rows[0][0]),
            native_as_string(rows[0][1]),
            native_as_string(rows[0][2]),
            native_as_string(rows[0][3]),
            native_as_string(rows[0][4]),
        };
    }

    NativeBranchSegment load_segment_by_id(std::int64_t segment_id) {
        auto rows = driver_->query(
            "SELECT segment_id, live_lo, live_hi, branch_point "
            "FROM _chronos_branch_interval_segments "
            "WHERE segment_id = ?",
            {segment_id}
        );
        if (rows.empty()) {
            throw std::runtime_error("segment not found: " + std::to_string(segment_id));
        }
        return {
            native_as_int(rows[0][0]),
            native_as_string(rows[0][1]),
            native_as_string(rows[0][2]),
            native_as_string(rows[0][3]),
        };
    }

    std::vector<NativeTableMeta> load_base_table_metas() {
        auto rows = driver_->query(
            "SELECT table_name, physical_table, pk_columns, columns, column_defs "
            "FROM _chronos_branch_tables "
            "WHERE backend = 'interval' "
            "ORDER BY table_name"
        );
        std::vector<NativeTableMeta> metas;
        metas.reserve(rows.size());
        for (const auto &row : rows) {
            metas.push_back(
                NativeTableMeta{
                    native_as_string(row[0]),
                    native_as_string(row[1]),
                    parse_json_string_array(native_as_string(row[3])),
                    parse_json_string_array(native_as_string(row[2])),
                    parse_json_string_array(native_as_string(row[4])),
                    "",
                    "",
                    false,
                }
            );
        }
        return metas;
    }

    NativeTableMeta load_table_meta(const std::string &logical_table, const NativeBranchSegment &segment) {
        if (table_exists("_chronos_branch_table_bindings")) {
            auto rows = driver_->query(
                "SELECT v.physical_table, v.pk_columns, v.columns, v.column_defs, "
                "       b.schema_version_id, v.ddl_op "
                "FROM _chronos_branch_table_bindings b "
                "JOIN _chronos_branch_table_schema_versions v "
                "  ON v.backend = b.backend AND v.schema_version_id = b.schema_version_id "
                "WHERE b.backend = 'interval' "
                "  AND b.table_name = ? "
                "  AND b.tombstone = 0 "
                "  AND b.live_lo <= ? "
                "  AND ? < b.live_hi "
                "ORDER BY b.live_lo DESC "
                "LIMIT 1",
                {logical_table, segment.branch_point, segment.branch_point}
            );
            if (!rows.empty()) {
                return {
                    logical_table,
                    native_as_string(rows[0][0]),
                    parse_json_string_array(native_as_string(rows[0][2])),
                    parse_json_string_array(native_as_string(rows[0][1])),
                    parse_json_string_array(native_as_string(rows[0][3])),
                    native_as_string(rows[0][4]),
                    native_as_string(rows[0][5]),
                    true,
                };
            }
        }
        auto rows = driver_->query(
            "SELECT physical_table, pk_columns, columns, column_defs "
            "FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = ? "
            "LIMIT 1",
            {logical_table}
        );
        if (rows.empty()) {
            throw std::runtime_error("table is not registered for interval branching: " + logical_table);
        }
        return {
            logical_table,
            native_as_string(rows[0][0]),
            parse_json_string_array(native_as_string(rows[0][2])),
            parse_json_string_array(native_as_string(rows[0][1])),
            parse_json_string_array(native_as_string(rows[0][3])),
            "",
            "",
            false,
        };
    }

    std::vector<NativeTableMeta> load_table_metas(const NativeBranchSegment &segment) {
        std::unordered_map<std::string, NativeTableMeta> by_table;
        if (table_exists("_chronos_branch_table_bindings")) {
            // Bulk-load active schema-version bindings.  The old path first
            // listed table names and then loaded every table one-by-one, which
            // made the first query after a schema change pay an avoidable N+1
            // metadata cost.  Once the schema-binding table exists it is the
            // authoritative visibility plane: falling back to the legacy base
            // registry would incorrectly resurrect branch-local DROP TABLE
            // tombstones.
            auto active_rows = driver_->query(
                "SELECT b.table_name, v.physical_table, v.pk_columns, v.columns, v.column_defs, "
                "       b.schema_version_id, v.ddl_op "
                "FROM _chronos_branch_table_bindings b "
                "JOIN _chronos_branch_table_schema_versions v "
                "  ON v.backend = b.backend AND v.schema_version_id = b.schema_version_id "
                "WHERE b.backend = 'interval' "
                "  AND b.tombstone = 0 "
                "  AND b.live_lo <= ? "
                "  AND ? < b.live_hi "
                "ORDER BY b.table_name, b.live_lo DESC",
                {segment.branch_point, segment.branch_point}
            );
            for (const auto &row : active_rows) {
                const std::string table = native_as_string(row[0]);
                if (by_table.find(table) != by_table.end()) continue;
                by_table.emplace(
                    table,
                    NativeTableMeta{
                        table,
                        native_as_string(row[1]),
                        parse_json_string_array(native_as_string(row[3])),
                        parse_json_string_array(native_as_string(row[2])),
                        parse_json_string_array(native_as_string(row[4])),
                        native_as_string(row[5]),
                        native_as_string(row[6]),
                        true,
                    }
                );
            }
            std::vector<NativeTableMeta> metas;
            metas.reserve(by_table.size());
            for (auto &[_, meta] : by_table) {
                metas.push_back(std::move(meta));
            }
            std::sort(metas.begin(), metas.end(), [](const NativeTableMeta &left, const NativeTableMeta &right) {
                return left.logical_name < right.logical_name;
            });
            return metas;
        }

        auto base_rows = driver_->query(
            "SELECT table_name, physical_table, pk_columns, columns, column_defs "
            "FROM _chronos_branch_tables "
            "WHERE backend = 'interval' "
            "ORDER BY table_name"
        );
        for (const auto &row : base_rows) {
            const std::string table = native_as_string(row[0]);
            if (by_table.find(table) != by_table.end()) continue;
            by_table.emplace(
                table,
                NativeTableMeta{
                    table,
                    native_as_string(row[1]),
                    parse_json_string_array(native_as_string(row[3])),
                    parse_json_string_array(native_as_string(row[2])),
                    parse_json_string_array(native_as_string(row[4])),
                    "",
                    "",
                    false,
                }
            );
        }

        std::vector<NativeTableMeta> metas;
        metas.reserve(by_table.size());
        for (auto &[_, meta] : by_table) {
            metas.push_back(std::move(meta));
        }
        std::sort(metas.begin(), metas.end(), [](const NativeTableMeta &left, const NativeTableMeta &right) {
            return left.logical_name < right.logical_name;
        });
        return metas;
    }

    std::vector<std::string> known_schema_table_names() {
        std::set<std::string> names;
        auto base_rows = driver_->query(
            "SELECT table_name FROM _chronos_branch_tables "
            "WHERE backend = 'interval'"
        );
        for (const auto &row : base_rows) {
            names.insert(native_as_string(row[0]));
        }
        if (table_exists("_chronos_branch_table_bindings")) {
            auto rows = driver_->query(
                "SELECT DISTINCT table_name "
                "FROM _chronos_branch_table_bindings "
                "WHERE backend = 'interval'"
            );
            for (const auto &row : rows) {
                names.insert(native_as_string(row[0]));
            }
        }
        return {names.begin(), names.end()};
    }

    static NativeTableInfo public_table_info(const NativeTableMeta &meta) {
        return NativeTableInfo{
            meta.logical_name,
            meta.physical_name,
            meta.pk_columns,
            meta.columns,
            meta.column_defs,
        };
    }

    NativePreparedRefInfo prepare_ref_info(
        std::int64_t segment_id,
        bool enable_schema_branching
    ) {
        NativeBranchSegment segment = load_segment_by_id(segment_id);

        // Checkout preparation is on the hot path for query-heavy workloads.
        // Keep this as one native metadata pass so Python does not issue its
        // own branch/table-binding SELECTs before every BranchSession checkout.
        std::vector<NativeTableMeta> metas = enable_schema_branching
            ? load_table_metas(segment)
            : load_base_table_metas();

        NativePreparedRefInfo info;
        info.segment = NativeSegmentInfo{
            segment.segment_id,
            segment.live_lo,
            segment.live_hi,
            segment.branch_point,
        };
        info.tables.reserve(metas.size());
        info.known_schema_tables.reserve(metas.size());
        for (const auto &meta : metas) {
            info.tables.push_back(public_table_info(meta));
            if (!enable_schema_branching) {
                info.known_schema_tables.push_back(meta.logical_name);
            }
        }
        if (enable_schema_branching) {
            info.known_schema_tables = known_schema_table_names();
        }
        return info;
    }

    std::string default_index_name(
        const std::string &table,
        const std::vector<std::string> &columns
    ) {
        return "idx_" + table + "_" + join_strings(columns, "_");
    }

    std::vector<NativeTableMeta> known_physical_metas_for_table(const std::string &table) {
        std::unordered_map<std::string, NativeTableMeta> by_physical;
        auto base_rows = driver_->query(
            "SELECT physical_table, pk_columns, columns, column_defs "
            "FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = ?",
            {table}
        );
        for (const auto &row : base_rows) {
            NativeTableMeta meta{
                table,
                native_as_string(row[0]),
                parse_json_string_array(native_as_string(row[2])),
                parse_json_string_array(native_as_string(row[1])),
                parse_json_string_array(native_as_string(row[3])),
                "",
                "",
                false,
            };
            by_physical.emplace(meta.physical_name, std::move(meta));
        }
        if (table_exists("_chronos_branch_table_schema_versions")) {
            auto rows = driver_->query(
                "SELECT physical_table, pk_columns, columns, column_defs "
                "FROM _chronos_branch_table_schema_versions "
                "WHERE backend = 'interval' AND table_name = ?",
                {table}
            );
            for (const auto &row : rows) {
                NativeTableMeta meta{
                    table,
                    native_as_string(row[0]),
                    parse_json_string_array(native_as_string(row[2])),
                    parse_json_string_array(native_as_string(row[1])),
                    parse_json_string_array(native_as_string(row[3])),
                    "",
                    "",
                    true,
                };
                by_physical.emplace(meta.physical_name, std::move(meta));
            }
        }
        std::vector<NativeTableMeta> out;
        out.reserve(by_physical.size());
        for (auto &entry : by_physical) out.push_back(std::move(entry.second));
        return out;
    }

    std::string physical_logical_index_name(
        const NativeTableMeta &meta,
        const std::string &table,
        const std::string &index_name
    ) {
        (void)table;
        if (!meta.has_schema_binding) {
            return "_chronos_idx_interval_" + index_name;
        }
        return "_chronos_idx_interval_sv_" +
            hex_u64(stable_fnv1a64(index_name + ":" + meta.physical_name), 24).substr(0, 24);
    }

    void create_physical_logical_index(
        const NativeTableMeta &meta,
        const std::string &table,
        const std::string &index_name,
        const std::vector<std::string> &columns
    ) {
        for (const auto &column : columns) {
            if (std::find(meta.columns.begin(), meta.columns.end(), column) == meta.columns.end()) {
                return;
            }
        }
        std::vector<std::string> indexed_columns = columns;
        indexed_columns.push_back("live_lo");
        indexed_columns.push_back("live_hi");
        indexed_columns.push_back("deleted");
        driver().execute(
            "CREATE INDEX IF NOT EXISTS " +
            quote_ident(physical_logical_index_name(meta, table, index_name)) +
            " ON " + quote_ident(meta.physical_name) +
            " (" + comma_join_quoted(indexed_columns) + ")"
        );
    }

    void create_physical_logical_indexes(
        const std::string &table,
        const std::string &index_name,
        const std::vector<std::string> &columns
    ) {
        for (const auto &meta : known_physical_metas_for_table(table)) {
            create_physical_logical_index(meta, table, index_name, columns);
        }
    }

    void create_existing_logical_indexes_for_meta(const NativeTableMeta &meta, const std::string &table) {
        auto rows = driver_->query(
            "SELECT index_name, columns FROM _chronos_branch_indexes "
            "WHERE backend = 'interval' AND table_name = ?",
            {table}
        );
        for (const auto &row : rows) {
            create_physical_logical_index(
                meta,
                table,
                native_as_string(row[0]),
                parse_json_string_array(native_as_string(row[1]))
            );
        }
    }

    std::string create_index(
        const std::string &table,
        const std::vector<std::string> &columns,
        const std::string &requested_name
    ) {
        if (columns.empty()) {
            throw std::invalid_argument("index columns cannot be empty");
        }
        auto base_rows = driver_->query(
            "SELECT columns FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = ?",
            {table}
        );
        if (base_rows.empty()) {
            throw std::runtime_error("table is not registered for interval branching: " + table);
        }
        std::unordered_set<std::string> visible_columns;
        for (const auto &column : parse_json_string_array(native_as_string(base_rows[0][0]))) {
            visible_columns.insert(column);
        }
        for (const auto &column : columns) {
            if (visible_columns.find(column) == visible_columns.end()) {
                throw std::runtime_error("index columns missing from " + table + ": " + column);
            }
        }
        const std::string index_name = requested_name.empty()
            ? default_index_name(table, columns)
            : requested_name;

        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            auto existing = driver_->query(
                "SELECT table_name, columns FROM _chronos_branch_indexes "
                "WHERE backend = 'interval' AND index_name = ?",
                {index_name}
            );
            const std::string columns_json = json_string_array(columns);
            if (existing.empty()) {
                driver_->execute(
                    "INSERT INTO _chronos_branch_indexes "
                    "(backend, index_name, table_name, columns) "
                    "VALUES ('interval', ?, ?, ?)",
                    {index_name, table, columns_json}
                );
            } else if (
                native_as_string(existing[0][0]) != table ||
                native_as_string(existing[0][1]) != columns_json
            ) {
                throw std::runtime_error("index already exists with different definition: " + index_name);
            }
            create_physical_logical_indexes(table, index_name, columns);
            if (started_tx) driver_->execute("COMMIT");
            return index_name;
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

// -----------------------------------------------------------------------------
// Branch/checkpoint lifecycle and interval segment allocation
// Source: interval_branch_store_lifecycle.cpp
// -----------------------------------------------------------------------------
    NativeDirectMergeSegment load_direct_segment_by_id(std::int64_t segment_id) {
        auto rows = driver_->query(
            "SELECT segment_id, COALESCE(parent_segment_id, 0), segment_kind, live_lo, live_hi, branch_point "
            "FROM _chronos_branch_interval_segments WHERE segment_id = ?",
            {segment_id}
        );
        if (rows.empty()) throw std::runtime_error("branch not found: segment:" + std::to_string(segment_id));
        return {
            native_as_int(rows[0][0]),
            native_as_int(rows[0][1]),
            native_as_string(rows[0][2]),
            native_as_string(rows[0][3]),
            native_as_string(rows[0][4]),
            native_as_string(rows[0][5]),
        };
    }

    std::vector<std::int64_t> allocate_segment_ids(std::size_t count) {
        if (count == 0) return {};
        if (metadata_dialect() == "postgres") {
            auto rows = driver_->query(
                "SELECT nextval('_chronos_branch_interval_segment_id_seq')::bigint "
                "FROM generate_series(1, ?)",
                {static_cast<std::int64_t>(count)}
            );
            std::vector<std::int64_t> ids;
            ids.reserve(rows.size());
            for (const auto &row : rows) ids.push_back(native_as_int(row[0]));
            if (ids.size() != count) throw std::runtime_error("interval segment id allocator returned wrong count");
            return ids;
        }
        auto rows = driver_->query(
            "UPDATE _chronos_branch_interval_segment_id_alloc "
            "SET next_segment_id = next_segment_id + ? "
            "WHERE singleton = 1 "
            "RETURNING next_segment_id - ?",
            {static_cast<std::int64_t>(count), static_cast<std::int64_t>(count)}
        );
        if (rows.empty()) throw std::runtime_error("interval segment id allocator is missing");
        const std::int64_t start = native_as_int(rows[0][0]);
        std::vector<std::int64_t> ids;
        ids.reserve(count);
        for (std::size_t i = 0; i < count; ++i) ids.push_back(start + static_cast<std::int64_t>(i));
        return ids;
    }

    int branch_depth(const std::string &branch_id) {
        auto rows = driver_->query(
            "WITH RECURSIVE ancestry(branch_id, parent_branch_id, depth) AS ("
            "  SELECT branch_id, parent_branch_id, 0 "
            "  FROM _chronos_branch_interval_branches WHERE branch_id = ? "
            "UNION ALL "
            "  SELECT parent.branch_id, parent.parent_branch_id, ancestry.depth + 1 "
            "  FROM _chronos_branch_interval_branches AS parent "
            "  JOIN ancestry ON parent.branch_id = ancestry.parent_branch_id "
            ") SELECT COALESCE(MAX(depth), 0) FROM ancestry",
            {branch_id}
        );
        if (rows.empty()) throw std::runtime_error("branch not found: " + branch_id);
        return static_cast<int>(native_as_int(rows[0][0]));
    }

    NativeDirectMergeSegment initial_branch_segment(
        const std::string &branch_id,
        const NativeDirectMergeSegment &fallback
    ) {
        auto rows = driver_->query(
            "SELECT segment_id, COALESCE(parent_segment_id, 0), segment_kind, "
            "       live_lo, live_hi, branch_point "
            "FROM _chronos_branch_interval_segments "
            "WHERE owner_branch_id = ? ORDER BY segment_id LIMIT 1",
            {branch_id}
        );
        if (rows.empty()) return fallback;
        return {
            native_as_int(rows[0][0]),
            native_as_int(rows[0][1]),
            native_as_string(rows[0][2]),
            native_as_string(rows[0][3]),
            native_as_string(rows[0][4]),
            native_as_string(rows[0][5]),
        };
    }

    cpp_int choose_interval_child_width(
        const NativeDirectMergeSegment &source_segment,
        const NativeDirectMergeSegment &initial_segment,
        int depth,
        int children_created,
        int fanout,
        bool terminal,
        int fixed_child_width
    ) const {
        const cpp_int active = cpp_int_from_decimal(source_segment.live_hi) -
            cpp_int_from_decimal(source_segment.live_lo) - 1;
        if (active < 2) throw std::runtime_error("interval space exhausted");
        cpp_int child_width;
        if (terminal) {
            child_width = 2;
        } else if (fixed_child_width > 0) {
            child_width = fixed_child_width;
        } else {
            const cpp_int initial = cpp_int_from_decimal(initial_segment.live_hi) -
                cpp_int_from_decimal(initial_segment.live_lo) - 1;
            if (depth < 3) {
                child_width = initial >> reserve_bits_[static_cast<std::size_t>(depth)];
            } else if (fanout > 0) {
                child_width = initial / (fanout + 1);
            } else {
                child_width = active / (harmonic_reserve_ + children_created + 1);
            }
        }
        if (child_width < 2) throw std::runtime_error("interval space exhausted");
        if (child_width > active - 2) child_width = active - 2;
        if (child_width < 2) throw std::runtime_error("interval space exhausted");
        return child_width;
    }

    struct NativeSessionBarrierGuard {
        std::vector<std::string> branches;
        std::string barrier_id;
        std::string delete_parent;
        bool installed = false;
        bool transaction_open = false;
        bool delete_leaf_fast_path = false;
    };

    static std::int64_t session_epoch_now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    static std::string next_session_barrier_id(const std::string &operation) {
        static std::atomic<std::uint64_t> counter{1};
        const auto tick = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto sequence = counter.fetch_add(1, std::memory_order_relaxed);
        return operation + "_" + hex_u64(
            static_cast<std::uint64_t>(tick) ^
            (sequence * 0x9e3779b97f4a7c15ULL),
            16
        );
    }

    std::vector<std::string> session_barrier_branches(
        const std::vector<std::string> &roots,
        bool include_descendants
    ) {
        std::set<std::string> branches(roots.begin(), roots.end());
        if (include_descendants) {
            for (const auto &root : roots) {
                auto rows = driver_->query(
                    "WITH RECURSIVE subtree(branch_id) AS ("
                    "  SELECT branch_id FROM _chronos_branch_interval_branches WHERE branch_id = ? "
                    "UNION ALL "
                    "  SELECT child.branch_id "
                    "  FROM _chronos_branch_interval_branches child "
                    "  JOIN subtree ON child.parent_branch_id = subtree.branch_id"
                    ") SELECT branch_id FROM subtree",
                    {root}
                );
                for (const auto &row : rows) branches.insert(native_as_string(row[0]));
            }
        }
        return {branches.begin(), branches.end()};
    }

    void lock_session_barrier_branches(const std::vector<std::string> &branches) {
        if (branches.empty()) return;
        std::vector<Value> params;
        params.reserve(branches.size());
        for (const auto &branch : branches) params.push_back(branch);
        std::string sql =
            "SELECT branch_id FROM _chronos_branch_interval_branches "
            "WHERE branch_id IN (" + placeholders(params.size()) + ") ORDER BY branch_id";
        if (metadata_dialect() == "postgres") sql += " FOR UPDATE";
        auto rows = driver_->query(sql, params);
        if (rows.size() != branches.size()) {
            throw std::runtime_error("branch not found while establishing session barrier");
        }
    }

    void notify_session_barrier(const std::string &barrier_id) {
        if (metadata_dialect() == "postgres") {
            driver_->query("SELECT pg_notify('chronos_session_epoch', ?)", {barrier_id});
        }
    }

    NativeSessionBarrierGuard begin_session_barrier(
        const std::vector<std::string> &roots,
        const std::string &operation,
        bool include_descendants = false
    ) {
        if (driver_->in_transaction()) {
            throw std::runtime_error(
                "branch management cannot begin inside an existing metadata transaction"
            );
        }
        driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        NativeSessionBarrierGuard guard;
        guard.transaction_open = true;
        try {
            guard.branches = session_barrier_branches(roots, include_descendants);
            lock_session_barrier_branches(guard.branches);
            const std::int64_t now_ms = session_epoch_now_ms();
            std::vector<Value> params;
            params.reserve(guard.branches.size() + 1);
            for (const auto &branch : guard.branches) params.push_back(branch);
            params.push_back(now_ms);
            if (metadata_dialect() != "postgres") {
                driver_->execute(
                    "DELETE FROM _chronos_branch_sessions "
                    "WHERE branch_id IN (" + placeholders(guard.branches.size()) + ") "
                    "AND lease_expires_ms <= ?",
                    params
                );
            }
            params.pop_back();
            auto existing = driver_->query(
                "SELECT branch_id FROM _chronos_branch_session_barriers "
                "WHERE branch_id IN (" + placeholders(guard.branches.size()) + ") LIMIT 1",
                params
            );
            if (!existing.empty()) {
                throw std::runtime_error(
                    "chronos_session_barrier_in_progress: " + native_as_string(existing[0][0])
                );
            }
            if (metadata_dialect() == "postgres") {
                bool all_quiescent = true;
                for (const auto &branch : guard.branches) {
                    auto acquired = driver_->query(
                        "SELECT pg_try_advisory_xact_lock("
                        "hashtext('_chronos_session_epoch'), hashtext(?))",
                        {branch}
                    );
                    all_quiescent = all_quiescent && !acquired.empty() &&
                        native_as_int(acquired[0][0]) != 0;
                }
                if (all_quiescent) {
                    return guard;
                }
            } else {
                params.push_back(now_ms);
                auto sessions = driver_->query(
                    "SELECT session_id FROM _chronos_branch_sessions "
                    "WHERE branch_id IN (" + placeholders(guard.branches.size()) + ") "
                    "AND lease_expires_ms > ? LIMIT 1",
                    params
                );
                if (sessions.empty()) return guard;
            }

            guard.installed = true;
            guard.barrier_id = next_session_barrier_id(operation);
            for (const auto &branch : guard.branches) {
                driver_->execute(
                    "INSERT INTO _chronos_branch_session_barriers "
                    "(branch_id, barrier_id, operation, created_at_ms) VALUES (?, ?, ?, ?)",
                    {branch, guard.barrier_id, operation, now_ms}
                );
            }
            std::vector<Value> update_params{guard.barrier_id};
            for (const auto &branch : guard.branches) update_params.push_back(branch);
            if (metadata_dialect() != "postgres") update_params.push_back(now_ms);
            driver_->execute(
                "UPDATE _chronos_branch_sessions "
                "SET required_epoch = session_epoch + 1, barrier_id = ?, status = 'draining' "
                "WHERE barrier_id IS NULL "
                "AND branch_id IN (" + placeholders(guard.branches.size()) + ") " +
                (metadata_dialect() == "postgres" ? "" : "AND lease_expires_ms > ?"),
                update_params
            );
            notify_session_barrier(guard.barrier_id);
            driver_->execute("COMMIT");
            guard.transaction_open = false;

            if (metadata_dialect() == "postgres") {
                driver_->execute("BEGIN");
                guard.transaction_open = true;
                // Wait for every session's shared advisory fence before
                // taking branch-row locks.  A session can still be finishing
                // a write that holds one of those rows; taking the row lock
                // first would make the barrier wait on the session while the
                // session waits on the barrier's row lock.
                for (const auto &branch : guard.branches) {
                    driver_->query(
                        "SELECT pg_advisory_xact_lock("
                        "hashtext('_chronos_session_epoch'), hashtext(?))",
                        {branch}
                    );
                }
                lock_session_barrier_branches(guard.branches);
                return guard;
            }

            const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(5);
            while (true) {
                const std::int64_t poll_ms = session_epoch_now_ms();
                driver_->execute(
                    "DELETE FROM _chronos_branch_sessions "
                    "WHERE barrier_id = ? AND lease_expires_ms <= ?",
                    {guard.barrier_id, poll_ms}
                );
                auto pending = driver_->query(
                    "SELECT 1 FROM _chronos_branch_sessions "
                    "WHERE barrier_id = ? AND status <> 'quiescent' LIMIT 1",
                    {guard.barrier_id}
                );
                if (pending.empty()) break;
                if (std::chrono::steady_clock::now() >= deadline) {
                    throw std::runtime_error(
                        "timed out waiting for sessions to cross barrier: " + guard.barrier_id
                    );
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
            guard.transaction_open = true;
            lock_session_barrier_branches(guard.branches);
            return guard;
        } catch (...) {
            abort_session_barrier(guard);
            throw;
        }
    }

    NativeSessionBarrierGuard begin_delete_session_barrier(
        const std::string &branch_id
    ) {
        if (metadata_dialect() != "postgres" || driver_->in_transaction()) {
            return begin_session_barrier({branch_id}, "delete", true);
        }
        NativeSessionBarrierGuard guard;
        driver_->execute("BEGIN");
        guard.transaction_open = true;
        try {
            auto rows = driver_->query(
                "SELECT parent.parent_branch_id, parent.child_count, "
                "       NOT EXISTS ("
                "         SELECT 1 FROM _chronos_branch_session_barriers barrier "
                "         WHERE barrier.branch_id = parent.branch_id"
                "       ), "
                "       pg_try_advisory_xact_lock("
                "         hashtext('_chronos_session_epoch'), hashtext(parent.branch_id)) "
                "FROM _chronos_branch_interval_branches parent "
                "WHERE parent.branch_id = ? FOR UPDATE",
                {branch_id}
            );
            if (rows.empty()) {
                throw std::runtime_error("branch not found while establishing session barrier");
            }
            const bool leaf = native_as_int(rows[0][1]) == 0;
            const bool no_barrier = native_as_int(rows[0][2]) != 0;
            const bool fence_acquired = native_as_int(rows[0][3]) != 0;
            if (leaf && no_barrier && fence_acquired) {
                guard.branches = {branch_id};
                guard.delete_parent = native_as_string(rows[0][0]);
                guard.delete_leaf_fast_path = true;
                return guard;
            }
            driver_->execute("ROLLBACK");
            guard.transaction_open = false;
        } catch (...) {
            if (guard.transaction_open && driver_->in_transaction()) {
                try { driver_->execute("ROLLBACK"); } catch (...) {}
                guard.transaction_open = false;
            }
            throw;
        }
        return begin_session_barrier({branch_id}, "delete", true);
    }

    void finish_session_barrier(NativeSessionBarrierGuard &guard) {
        if (!guard.transaction_open) {
            driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
            guard.transaction_open = true;
        }
        if (guard.installed) {
            if (metadata_dialect() != "postgres") {
                driver_->execute(
                    "UPDATE _chronos_branch_sessions "
                    "SET barrier_id = NULL, status = 'active', required_epoch = session_epoch "
                    "WHERE barrier_id = ?",
                    {guard.barrier_id}
                );
            } else {
                driver_->execute(
                    "DELETE FROM _chronos_branch_sessions WHERE lease_expires_ms <= ?",
                    {session_epoch_now_ms()}
                );
            }
            driver_->execute(
                "DELETE FROM _chronos_branch_session_barriers WHERE barrier_id = ?",
                {guard.barrier_id}
            );
            notify_session_barrier(guard.barrier_id);
        }
        driver_->execute("COMMIT");
        guard.transaction_open = false;
    }

    void suspend_session_barrier_transaction(NativeSessionBarrierGuard &guard) {
        if (!guard.transaction_open) return;
        driver_->execute("COMMIT");
        guard.transaction_open = false;
    }

    void abort_session_barrier(NativeSessionBarrierGuard &guard) {
        if (guard.transaction_open && driver_->in_transaction()) {
            try { driver_->execute("ROLLBACK"); } catch (...) {}
            guard.transaction_open = false;
        }
        if (!guard.installed) return;
        try {
            driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
            if (metadata_dialect() != "postgres") {
                driver_->execute(
                    "UPDATE _chronos_branch_sessions "
                    "SET barrier_id = NULL, status = 'active', required_epoch = session_epoch "
                    "WHERE barrier_id = ?",
                    {guard.barrier_id}
                );
            }
            driver_->execute(
                "DELETE FROM _chronos_branch_session_barriers WHERE barrier_id = ?",
                {guard.barrier_id}
            );
            notify_session_barrier(guard.barrier_id);
            driver_->execute("COMMIT");
        } catch (...) {
            if (driver_->in_transaction()) {
                try { driver_->execute("ROLLBACK"); } catch (...) {}
            }
        }
    }

    void create_branch(
        const std::string &branch_id,
        const std::string &from_branch,
        bool terminal,
        const std::string &metadata_json,
        int fanout,
        int child_width
    ) {
        NativeSessionBarrierGuard guard = begin_session_barrier({from_branch}, "fork");
        try {
            create_branch_unfenced(
                branch_id, from_branch, terminal, metadata_json, fanout, child_width
            );
            finish_session_barrier(guard);
        } catch (...) {
            abort_session_barrier(guard);
            throw;
        }
    }

    void create_branch_unfenced(
        const std::string &branch_id,
        const std::string &from_branch,
        bool terminal,
        const std::string &metadata_json,
        int fanout,
        int child_width
    ) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
        const std::string lock_suffix = metadata_dialect() == "postgres" ? " FOR UPDATE" : "";
        std::int64_t continuation_id = 0;
        std::int64_t child_id = 0;
        std::int64_t fork_base_id = 0;
        std::vector<std::vector<Value>> source_rows;
        std::vector<Value> source_segment_row;
        std::string source_branch_kind;
        int children_created = 0;
        if (metadata_dialect() == "postgres") {
            // Lock the source branch head first, then read its segment in a
            // separate statement.  Under READ COMMITTED, a concurrent fork can
            // update current_segment_id while inserting the continuation
            // segment.  A single SELECT ... JOIN ... FOR UPDATE can recheck the
            // locked branch row but still miss the just-committed segment in
            // the statement snapshot, falsely reporting that the source branch
            // does not exist.
            source_rows = driver_->query(
                "SELECT b.current_segment_id, b.branch_kind, b.child_count, "
                "       (SELECT COUNT(*) FROM _chronos_branch_interval_branches WHERE branch_id = ?), "
                "       nextval('_chronos_branch_interval_segment_id_seq')::bigint, "
                "       nextval('_chronos_branch_interval_segment_id_seq')::bigint, "
                "       nextval('_chronos_branch_interval_segment_id_seq')::bigint "
                "FROM _chronos_branch_interval_branches b "
                "WHERE b.branch_id = ?" + lock_suffix,
                {branch_id, from_branch}
            );
            if (source_rows.empty()) {
                throw std::runtime_error("branch not found: " + from_branch);
            }
            if (native_as_int(source_rows[0][3]) > 0) {
                throw std::runtime_error("branch already exists: " + branch_id);
            }
            children_created = static_cast<int>(native_as_int(source_rows[0][2]));
            continuation_id = native_as_int(source_rows[0][4]);
            child_id = native_as_int(source_rows[0][5]);
            fork_base_id = native_as_int(source_rows[0][6]);
            source_branch_kind = native_as_string(source_rows[0][1]);
            auto segment_rows = driver_->query(
                "SELECT segment_id, COALESCE(parent_segment_id, 0), segment_kind, "
                "       live_lo, live_hi, branch_point "
                "FROM _chronos_branch_interval_segments "
                "WHERE segment_id = ?",
                {native_as_int(source_rows[0][0])}
            );
            if (segment_rows.empty()) {
                throw std::runtime_error("branch segment not found: " + from_branch);
            }
            source_segment_row = std::move(segment_rows[0]);
        } else {
            auto existing = driver_->query(
                "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = ? LIMIT 1",
                {branch_id}
            );
            if (!existing.empty()) {
                throw std::runtime_error("branch already exists: " + branch_id);
            }
            source_rows = driver_->query(
                "SELECT b.current_segment_id, b.branch_kind, b.child_count, "
                "       s.segment_id, COALESCE(s.parent_segment_id, 0), s.segment_kind, "
                "       s.live_lo, s.live_hi, s.branch_point "
                "FROM _chronos_branch_interval_branches b "
                "JOIN _chronos_branch_interval_segments s "
                "  ON s.segment_id = b.current_segment_id "
                "WHERE b.branch_id = ?" + lock_suffix,
                {from_branch}
            );
            if (source_rows.empty()) {
                throw std::runtime_error("branch not found: " + from_branch);
            }
            source_branch_kind = native_as_string(source_rows[0][1]);
            children_created = static_cast<int>(native_as_int(source_rows[0][2]));
            source_segment_row.assign(source_rows[0].begin() + 3, source_rows[0].begin() + 9);
        }
        if (source_branch_kind == "terminal") {
            throw std::runtime_error("terminal branch is not branchable: " + from_branch);
        }
        NativeDirectMergeSegment source_segment{
            native_as_int(source_segment_row[0]),
            native_as_int(source_segment_row[1]),
            native_as_string(source_segment_row[2]),
            native_as_string(source_segment_row[3]),
            native_as_string(source_segment_row[4]),
            native_as_string(source_segment_row[5]),
        };

        if (fanout < 0) throw std::invalid_argument("fanout must be non-negative");
        const cpp_int lo = cpp_int_from_decimal(source_segment.live_lo);
        const cpp_int hi = cpp_int_from_decimal(source_segment.live_hi);
        if (hi - lo < 5) {
            throw std::runtime_error("interval space exhausted");
        }
        // Fork carving splits the source interval into three children:
        //   [lo, lo+1)                 fork_base: immutable common ancestor
        //   [lo+1, child_hi)           child branch write interval
        //   [child_hi, hi)             parent continuation interval
        // The parent branch head moves to the continuation; the new branch head
        // points at the child segment.  terminal=true simply gives the child a
        // minimal width, useful for short-lived branch transactions.
        const cpp_int fork_base_hi = lo + 1;
        const bool needs_depth = !terminal && child_width <= 0;
        const int depth = needs_depth ? branch_depth(from_branch) : 0;
        NativeDirectMergeSegment initial_segment = source_segment;
        if (needs_depth && (depth < 3 || fanout > 0)) {
            initial_segment = initial_branch_segment(from_branch, source_segment);
        }
        const cpp_int native_child_width = choose_interval_child_width(
            source_segment,
            initial_segment,
            depth,
            children_created,
            fanout,
            terminal,
            child_width
        );
        const cpp_int child_hi = fork_base_hi + native_child_width;
        if (metadata_dialect() != "postgres") {
            std::vector<std::int64_t> ids = allocate_segment_ids(3);
            continuation_id = ids[0];
            child_id = ids[1];
            fork_base_id = ids[2];
        }
        const cpp_int child_branch_point = fork_base_hi + (child_hi - fork_base_hi) / 2;
        std::string now = "native";
        if (metadata_dialect() == "postgres") {
            auto rows = driver_->query(
                "WITH inserted_segments AS ("
                "INSERT INTO _chronos_branch_interval_segments "
                "(segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi, branch_point, created_at, metadata) "
                "VALUES "
                "(?, ?, ?, 'fork_base', ?, ?, ?, ?, '{}'), "
                "(?, ?, ?, 'mutable', ?, ?, ?, ?, '{}'), "
                "(?, ?, ?, 'mutable', ?, ?, ?, ?, '{}') "
                "RETURNING 1"
                "), updated_parent AS ("
                "UPDATE _chronos_branch_interval_branches "
                "SET current_segment_id = ?, child_count = child_count + 1 "
                "WHERE branch_id = ? AND current_segment_id = ? "
                "RETURNING 1"
                "), inserted_branch AS ("
                "INSERT INTO _chronos_branch_interval_branches "
                "(branch_id, current_segment_id, parent_branch_id, child_count, branch_kind, created_at, metadata) "
                "SELECT ?, ?, ?, 0, ?, ?, ? WHERE EXISTS (SELECT 1 FROM updated_parent) "
                "RETURNING 1"
                ") SELECT "
                "(SELECT COUNT(*) FROM updated_parent), "
                "(SELECT COUNT(*) FROM inserted_branch)",
                {
                    fork_base_id, source_segment.segment_id, std::monostate{},
                    cpp_int_to_decimal(lo), cpp_int_to_decimal(fork_base_hi), cpp_int_to_decimal(lo), now,
                    continuation_id, fork_base_id, from_branch,
                    cpp_int_to_decimal(child_hi), cpp_int_to_decimal(hi), cpp_int_to_decimal(child_hi), now,
                    child_id, fork_base_id, branch_id,
                    cpp_int_to_decimal(fork_base_hi), cpp_int_to_decimal(child_hi), cpp_int_to_decimal(child_branch_point), now,
                    continuation_id, from_branch, source_segment.segment_id,
                    branch_id, child_id, from_branch,
                    terminal ? std::string("terminal") : std::string("mutable"),
                    now,
                    metadata_json.empty() ? std::string("{}") : metadata_json,
                }
            );
            if (rows.empty() || native_as_int(rows[0][0]) != 1 || native_as_int(rows[0][1]) != 1) {
                throw std::runtime_error("branch head changed during native branch creation: " + from_branch);
            }
        } else {
            driver_->execute(
                "INSERT INTO _chronos_branch_interval_segments "
                "(segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi, branch_point, created_at, metadata) "
                "VALUES "
                "(?, ?, ?, 'fork_base', ?, ?, ?, ?, '{}'), "
                "(?, ?, ?, 'mutable', ?, ?, ?, ?, '{}'), "
                "(?, ?, ?, 'mutable', ?, ?, ?, ?, '{}')",
                {
                    fork_base_id, source_segment.segment_id, std::monostate{},
                    cpp_int_to_decimal(lo), cpp_int_to_decimal(fork_base_hi), cpp_int_to_decimal(lo), now,
                    continuation_id, fork_base_id, from_branch,
                    cpp_int_to_decimal(child_hi), cpp_int_to_decimal(hi), cpp_int_to_decimal(child_hi), now,
                    child_id, fork_base_id, branch_id,
                    cpp_int_to_decimal(fork_base_hi), cpp_int_to_decimal(child_hi), cpp_int_to_decimal(child_branch_point), now,
                }
            );
            const std::int64_t updated = driver_->execute_changes(
                "UPDATE _chronos_branch_interval_branches "
                "SET current_segment_id = ?, child_count = child_count + 1 "
                "WHERE branch_id = ? AND current_segment_id = ?",
                {continuation_id, from_branch, source_segment.segment_id}
            );
            if (updated != 1) {
                throw std::runtime_error("branch head changed during native branch creation: " + from_branch);
            }
            driver_->execute(
                "INSERT INTO _chronos_branch_interval_branches "
                "(branch_id, current_segment_id, parent_branch_id, child_count, branch_kind, created_at, metadata) "
                "VALUES (?, ?, ?, 0, ?, ?, ?)",
                {branch_id, child_id, from_branch, terminal ? std::string("terminal") : std::string("mutable"), now, metadata_json.empty() ? std::string("{}") : metadata_json}
            );
        }
        if (started_tx) driver_->execute("COMMIT");
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    void create_branch_from_checkpoint(
        const std::string &branch_id,
        const std::string &checkpoint
    ) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            auto existing = driver_->query(
                "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = ? LIMIT 1",
                {branch_id}
            );
            if (!existing.empty()) {
                throw std::runtime_error("branch already exists: " + branch_id);
            }
            auto cp_rows = driver_->query(
                "SELECT branch_id, segment_id FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ?",
                {checkpoint}
            );
            if (cp_rows.empty()) {
                throw std::runtime_error("branch not found: checkpoint:" + checkpoint);
            }

            const std::string parent_branch_id = native_as_string(cp_rows[0][0]);
            NativeDirectMergeSegment segment = load_direct_segment_by_id(native_as_int(cp_rows[0][1]));
            const cpp_int lo = cpp_int_from_decimal(segment.live_lo);
            const cpp_int hi = cpp_int_from_decimal(segment.live_hi);
            if (hi - lo < 4) {
                throw std::runtime_error("interval space exhausted for checkpoint branch");
            }
            const cpp_int child_lo = lo + (hi - lo) / 2;
            const cpp_int child_hi = hi;
            const std::int64_t child_segment_id = allocate_segment_id();
            const std::string now = current_timestamp_string();
            driver_->execute(
                "INSERT INTO _chronos_branch_interval_segments "
                "(segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi, "
                " branch_point, created_at, metadata) "
                "VALUES (?, ?, ?, 'mutable', ?, ?, ?, ?, '{}')",
                {
                    child_segment_id,
                    segment.segment_id,
                    branch_id,
                    cpp_int_to_decimal(child_lo),
                    cpp_int_to_decimal(child_hi),
                    cpp_int_to_decimal(child_lo + (child_hi - child_lo) / 2),
                    now,
                }
            );
            driver_->execute(
                "INSERT INTO _chronos_branch_interval_branches "
                "(branch_id, current_segment_id, parent_branch_id, child_count, branch_kind, created_at, metadata) "
                "VALUES (?, ?, ?, 0, 'mutable', ?, '{}')",
                {branch_id, child_segment_id, parent_branch_id, now}
            );
            driver_->execute(
                "UPDATE _chronos_branch_interval_branches "
                "SET child_count = child_count + 1 "
                "WHERE branch_id = ?",
                {parent_branch_id}
            );
            if (started_tx) driver_->execute("COMMIT");
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    void delete_branch(const std::string &branch_id) {
        if (branch_id == "main") {
            throw std::runtime_error("main cannot be deleted");
        }
        NativeSessionBarrierGuard guard = begin_delete_session_barrier(branch_id);
        try {
            if (guard.delete_leaf_fast_path) {
                driver_->execute(
                    "DELETE FROM _chronos_branch_interval_branches WHERE branch_id = ?",
                    {branch_id}
                );
                if (!guard.delete_parent.empty()) {
                    driver_->execute(
                        "UPDATE _chronos_branch_interval_branches "
                        "SET child_count = CASE "
                        "  WHEN child_count > 0 THEN child_count - 1 ELSE 0 END "
                        "WHERE branch_id = ?",
                        {guard.delete_parent}
                    );
                }
            } else {
                delete_branch_unfenced(branch_id);
            }
            finish_session_barrier(guard);
        } catch (...) {
            abort_session_barrier(guard);
            throw;
        }
    }

    void delete_branch_unfenced(const std::string &branch_id) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            const std::string lock_suffix = metadata_dialect() == "postgres" ? " FOR UPDATE" : "";
            auto rows = driver_->query(
                "SELECT parent_branch_id, child_count "
                "FROM _chronos_branch_interval_branches "
                "WHERE branch_id = ?" + lock_suffix,
                {branch_id}
            );
            if (rows.empty()) {
                throw std::runtime_error("branch not found: " + branch_id);
            }
            const std::string parent_branch_id = native_as_string(rows[0][0]);
            const std::int64_t child_count = native_as_int(rows[0][1]);
            if (child_count == 0) {
                driver_->execute(
                    "DELETE FROM _chronos_branch_interval_branches WHERE branch_id = ?",
                    {branch_id}
                );
            } else {
                auto branch_rows = driver_->query(
                    "WITH RECURSIVE subtree(branch_id) AS ("
                    "  SELECT branch_id "
                    "  FROM _chronos_branch_interval_branches "
                    "  WHERE branch_id = ? "
                    "UNION ALL "
                    "  SELECT child.branch_id "
                    "  FROM _chronos_branch_interval_branches AS child "
                    "  JOIN subtree ON child.parent_branch_id = subtree.branch_id "
                    "  WHERE child.branch_id <> 'main'"
                    ") "
                    "SELECT branch_id FROM subtree ORDER BY branch_id",
                    {branch_id}
                );
                if (branch_rows.empty()) {
                    throw std::runtime_error("branch not found: " + branch_id);
                }
                std::vector<Value> delete_params;
                delete_params.reserve(branch_rows.size());
                for (const auto &row : branch_rows) {
                    delete_params.push_back(native_as_string(row[0]));
                }
                driver_->execute(
                    "DELETE FROM _chronos_branch_interval_branches "
                    "WHERE branch_id IN (" + placeholders(delete_params.size()) + ")",
                    delete_params
                );
            }
            if (!parent_branch_id.empty()) {
                driver_->execute(
                    "UPDATE _chronos_branch_interval_branches "
                    "SET child_count = CASE WHEN child_count > 0 THEN child_count - 1 ELSE 0 END "
                    "WHERE branch_id = ?",
                    {parent_branch_id}
                );
            }
            if (started_tx) driver_->execute("COMMIT");
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    std::int64_t collect_interval_garbage() {
        if (
            metadata_dialect() == "sqlite"
            && dialect() == "sqlite"
            && !in_transaction()
        ) {
            return collect_interval_garbage_sqlite_batched();
        }
        const bool started_metadata_tx = !driver_->in_transaction();
        const bool started_data_tx = data_driver_ != nullptr && !data_driver_->in_transaction();
        if (started_metadata_tx) {
            driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        }
        if (started_data_tx) {
            data_driver_->execute(data_driver_->dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        }
        try {
            const std::vector<std::int64_t> dead_segment_ids = interval_dead_segment_ids();
            if (dead_segment_ids.empty()) {
                if (started_data_tx) data_driver_->execute("COMMIT");
                if (started_metadata_tx) driver_->execute("COMMIT");
                return 0;
            }
            for (const auto &physical : interval_physical_tables_for_gc()) {
                delete_writer_rows_for_segments(physical, dead_segment_ids);
            }
            delete_dead_interval_segments(dead_segment_ids);
            if (started_data_tx) data_driver_->execute("COMMIT");
            if (started_metadata_tx) driver_->execute("COMMIT");
            return static_cast<std::int64_t>(dead_segment_ids.size());
        } catch (...) {
            if (started_data_tx) {
                try {
                    data_driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            if (started_metadata_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    chronos::native::NativeBranchInfo get_branch_info(const std::string &branch_id) {
        auto rows = driver_->query(
            "SELECT branch_id, current_segment_id, created_at, metadata "
            "FROM _chronos_branch_interval_branches "
            "WHERE branch_id = ?",
            {branch_id}
        );
        if (rows.empty()) {
            throw std::runtime_error("branch not found: " + branch_id);
        }
        return branch_info_from_row(rows[0]);
    }

    std::vector<chronos::native::NativeBranchInfo> list_branch_infos() {
        auto rows = driver_->query(
            "SELECT branch_id, current_segment_id, created_at, metadata "
            "FROM _chronos_branch_interval_branches "
            "ORDER BY branch_id"
        );
        std::vector<chronos::native::NativeBranchInfo> infos;
        infos.reserve(rows.size());
        for (const auto &row : rows) infos.push_back(branch_info_from_row(row));
        return infos;
    }

    chronos::native::NativeBranchInfo update_branch_metadata(
        const std::string &branch_id,
        const std::string &metadata_json
    ) {
        const std::int64_t changed = driver_->execute_changes(
            "UPDATE _chronos_branch_interval_branches SET metadata = ? WHERE branch_id = ?",
            {metadata_json.empty() ? std::string("{}") : metadata_json, branch_id}
        );
        if (changed == 0) {
            throw std::runtime_error("branch not found: " + branch_id);
        }
        return get_branch_info(branch_id);
    }

    chronos::native::NativeCheckpointInfo create_checkpoint(
        const std::string &checkpoint,
        const std::string &branch,
        const std::string &metadata_json
    ) {
        NativeSessionBarrierGuard guard = begin_session_barrier(
            {branch}, "checkpoint"
        );
        try {
            auto info = create_checkpoint_unfenced(checkpoint, branch, metadata_json);
            finish_session_barrier(guard);
            return info;
        } catch (...) {
            abort_session_barrier(guard);
            throw;
        }
    }

    chronos::native::NativeCheckpointInfo create_checkpoint_unfenced(
        const std::string &checkpoint,
        const std::string &branch,
        const std::string &metadata_json
    ) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            auto existing = driver_->query(
                "SELECT 1 FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ? LIMIT 1",
                {checkpoint}
            );
            if (!existing.empty()) {
                throw std::runtime_error("branch already exists: " + checkpoint);
            }
            const std::string lock_suffix = metadata_dialect() == "postgres" ? " FOR UPDATE" : "";
            auto source_rows = driver_->query(
                "SELECT current_segment_id, branch_kind, "
                "       (SELECT COUNT(*) FROM _chronos_branch_interval_checkpoints "
                "        WHERE branch_id = ?) "
                "FROM _chronos_branch_interval_branches "
                "WHERE branch_id = ?" + lock_suffix,
                {branch, branch}
            );
            if (source_rows.empty()) {
                throw std::runtime_error("branch not found: " + branch);
            }
            if (native_as_string(source_rows[0][1]) == "terminal") {
                throw std::runtime_error("terminal branch cannot be checkpointed: " + branch);
            }
            NativeDirectMergeSegment source_segment = load_direct_segment_by_id(native_as_int(source_rows[0][0]));
            const int checkpoints_created = static_cast<int>(native_as_int(source_rows[0][2]));
            auto [continuation, snapshot] = split_checkpoint_segment(
                source_segment,
                checkpoints_created
            );
            const std::string now = current_timestamp_string();
            insert_interval_segment(continuation, source_segment.segment_id, branch, "mutable", now);
            insert_interval_segment(snapshot, source_segment.segment_id, "", "checkpoint", now);
            driver_->execute(
                "UPDATE _chronos_branch_interval_branches "
                "SET current_segment_id = ? "
                "WHERE branch_id = ?",
                {continuation.segment_id, branch}
            );
            driver_->execute(
                "INSERT INTO _chronos_branch_interval_checkpoints "
                "(checkpoint_id, branch_id, segment_id, created_at, metadata) "
                "VALUES (?, ?, ?, ?, ?)",
                {
                    checkpoint,
                    branch,
                    snapshot.segment_id,
                    now,
                    metadata_json.empty() ? std::string("{}") : metadata_json,
                }
            );
            if (started_tx) driver_->execute("COMMIT");
            return {checkpoint, branch, std::to_string(snapshot.segment_id), now, metadata_json.empty() ? std::string("{}") : metadata_json};
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    chronos::native::NativeCheckpointInfo get_checkpoint_info(const std::string &checkpoint) {
        auto rows = driver_->query(
            "SELECT checkpoint_id, branch_id, segment_id, created_at, metadata "
            "FROM _chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = ?",
            {checkpoint}
        );
        if (rows.empty()) {
            throw std::runtime_error("branch not found: checkpoint:" + checkpoint);
        }
        return checkpoint_info_from_row(rows[0]);
    }

    std::vector<chronos::native::NativeCheckpointInfo> list_checkpoint_infos(const std::string &branch) {
        std::vector<std::vector<Value>> rows;
        if (branch.empty()) {
            rows = driver_->query(
                "SELECT checkpoint_id, branch_id, segment_id, created_at, metadata "
                "FROM _chronos_branch_interval_checkpoints "
                "ORDER BY created_at, checkpoint_id"
            );
        } else {
            rows = driver_->query(
                "SELECT checkpoint_id, branch_id, segment_id, created_at, metadata "
                "FROM _chronos_branch_interval_checkpoints "
                "WHERE branch_id = ? "
                "ORDER BY created_at, checkpoint_id",
                {branch}
            );
        }
        std::vector<chronos::native::NativeCheckpointInfo> infos;
        infos.reserve(rows.size());
        for (const auto &row : rows) infos.push_back(checkpoint_info_from_row(row));
        return infos;
    }

// -----------------------------------------------------------------------------
// Branch-local schema metadata and physical schema-version tables
// Source: interval_branch_store_schema.cpp
// -----------------------------------------------------------------------------
    std::string interval_sql_type() const {
        if (dialect() != "postgres") return "BIGINT";
        switch (interval_coordinate_bits_) {
        case 32:
            return "INTEGER";
        case 64:
            // Use the same PostgreSQL numeric representation as wider domains;
            // it also lets physical record bounds use Infinity for open ends.
            return "NUMERIC(20,0)";
        case 128:
            return "NUMERIC(39,0)";
        case 256:
            return "NUMERIC(78,0)";
        case 512:
            return "NUMERIC(155,0)";
        case 1024:
            return "NUMERIC(309,0)";
        default:
            return "NUMERIC(32,0)";
        }
    }

    bool uses_postgres_numeric_intervals() const {
        return dialect() == "postgres" &&
            interval_coordinate_bits_ != 32;
    }

    std::string physical_live_hi_sql_type() const {
        // PostgreSQL accepts Infinity only in an unconstrained NUMERIC column.
        // Branch allocation metadata remains finite and uses interval_sql_type().
        return uses_postgres_numeric_intervals() ? "NUMERIC" : interval_sql_type();
    }

    std::string initial_record_live_hi_value() const {
        // Record intervals can be open-ended. Branch intervals retain a finite
        // ceiling because fork placement performs integer arithmetic on them.
        return uses_postgres_numeric_intervals() ? "Infinity" : max_interval_value();
    }

    std::string max_interval_value() const {
        if (dialect() != "postgres") return "9000000000000000000";
        if (interval_coordinate_bits_ == 0) return "10000000000000000000000000000000";
        return cpp_int_to_decimal((cpp_int(1) << (interval_coordinate_bits_ - 1)) - 1);
    }

    std::string root_branch_point_value() const {
        if (dialect() != "postgres") return "4500000000000000000";
        if (interval_coordinate_bits_ == 0) return "5000000000000000000000000000000";
        return cpp_int_to_decimal(((cpp_int(1) << (interval_coordinate_bits_ - 1)) - 1) / 2);
    }

    std::string schema_version_id(const std::string &table) {
        return "sv_" + identifier_token(table) + "_" + unique_schema_suffix();
    }

    std::string physical_schema_table_name(const std::string &table) {
        return "_chronos_b_interval_" + physical_table_suffix(table) + "_" + unique_schema_suffix().substr(0, 8);
    }

    void lock_schema_metadata() {
        if (metadata_dialect() == "postgres") {
            driver_->execute("SELECT pg_advisory_xact_lock(1720812901, 19840717)");
        }
    }

    void lock_schema_binding_table(const std::string &table) {
        if (metadata_dialect() != "postgres") return;
        const std::int64_t key = static_cast<std::int32_t>(stable_fnv1a64(table) & 0xffffffffULL);
        driver_->execute("SELECT pg_advisory_xact_lock(1720812902, ?::int)", {key});
    }

    void ensure_schema_branching_tables() {
        const std::string interval_type = interval_sql_type();
        // Schema versions are immutable descriptions of logical table shape.
        // A schema-changing DDL either mutates a private version in place or
        // records a fresh version and binds it to the branch interval below.
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_table_schema_versions ("
            "backend TEXT NOT NULL, "
            "table_name TEXT NOT NULL, "
            "schema_version_id TEXT NOT NULL, "
            "parent_schema_version_id TEXT, "
            "physical_table TEXT NOT NULL, "
            "pk_columns TEXT NOT NULL, "
            "columns TEXT NOT NULL, "
            "column_defs TEXT NOT NULL, "
            "ddl_op TEXT NOT NULL, "
            "created_at TEXT NOT NULL, "
            "metadata TEXT NOT NULL, "
            "PRIMARY KEY (backend, schema_version_id))"
        );
        // Interval-versioned logical table bindings.  The active schema for a
        // branch is the row whose [live_lo, live_hi) contains the branch point.
        // Tombstone bindings represent branch-local DROP TABLE without removing
        // inherited data or affecting sibling branches.
        driver_->execute(
            "CREATE TABLE IF NOT EXISTS _chronos_branch_table_bindings ("
            "backend TEXT NOT NULL, "
            "table_name TEXT NOT NULL, "
            "schema_version_id TEXT, "
            "tombstone INTEGER NOT NULL DEFAULT 0, "
            "live_lo " + interval_type + " NOT NULL, "
            "live_hi " + interval_type + " NOT NULL, "
            "created_at TEXT NOT NULL, "
            "metadata TEXT NOT NULL, "
            "PRIMARY KEY (backend, table_name, live_lo))"
        );
    }

    std::string pk_hi_index_sql(const std::string &physical, const std::vector<std::string> &pk_columns) {
        return "CREATE INDEX IF NOT EXISTS " + quote_ident("idx_" + physical + "_pk_hi") +
            " ON " + quote_ident(physical) + " (" + comma_join_quoted(pk_columns) + ", live_hi)";
    }

    std::string writer_segment_index_sql(const std::string &physical, const std::vector<std::string> &pk_columns) {
        if (dialect() == "postgres") {
            return "CREATE INDEX IF NOT EXISTS " + quote_ident("idx_" + physical + "_writer_segment") +
                " ON " + quote_ident(physical) + " (writer_segment_id) WHERE writer_segment_id > 1";
        }
        std::string sql = "CREATE INDEX IF NOT EXISTS " + quote_ident("idx_" + physical + "_writer_segment") +
            " ON " + quote_ident(physical) + " (writer_segment_id, " + comma_join_quoted(pk_columns) + ")";
        return sql;
    }

    std::string primary_key_sql(const std::string &physical, const std::vector<std::string> &pk_columns) {
        return "ALTER TABLE " + quote_ident(physical) +
            " ADD PRIMARY KEY (" + comma_join_quoted(pk_columns) + ", live_lo)";
    }

    void add_interval_primary_key(const std::string &physical, const std::vector<std::string> &pk_columns) {
        driver().execute(primary_key_sql(physical, pk_columns));
    }

    int postgres_schema_copy_fillfactor() const {
        // Schema-copy tables are commonly followed by a full-table backfill.
        // Leaving heap room lets PostgreSQL use HOT updates and avoid rewriting
        // interval indexes for every copied row.
        return 50;
    }

    void create_interval_physical_table(
        const std::string &physical,
        const std::vector<std::string> &column_defs,
        const std::vector<std::string> &pk_columns,
        bool create_secondary_indexes = true,
        bool create_primary_key = true,
        int postgres_fillfactor = 0
    ) {
        // Physical interval tables keep user columns unchanged and append the
        // four versioning columns used by every relational data plane:
        // live_lo/live_hi bound row visibility, writer_segment_id identifies
        // the writer for diff/merge, and deleted stores logical tombstones.
        std::string sql =
            "CREATE TABLE " + quote_ident(physical) + " ("
            + join_strings(column_defs, ", ") +
            ", live_lo " + interval_sql_type() + " NOT NULL"
            ", live_hi " + physical_live_hi_sql_type() + " NOT NULL"
            ", writer_segment_id INTEGER NOT NULL"
            ", deleted BOOLEAN NOT NULL DEFAULT FALSE";
        if (create_primary_key) {
            sql += ", PRIMARY KEY (" + comma_join_quoted(pk_columns) + ", live_lo)";
        }
        sql += ")";
        if (dialect() == "postgres" && postgres_fillfactor > 0 && postgres_fillfactor < 100) {
            sql += " WITH (fillfactor = " + std::to_string(postgres_fillfactor) + ")";
        }
        driver().execute(sql);
        if (create_secondary_indexes) {
            driver().execute(pk_hi_index_sql(physical, pk_columns));
            if (create_writer_segment_index()) {
                driver().execute(writer_segment_index_sql(physical, pk_columns));
            }
        }
    }

    std::string record_schema_version(
        const std::string &table,
        const std::string &physical,
        const std::vector<std::string> &pk_columns,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &column_defs,
        const std::string &ddl_op,
        const std::string &parent_schema_version_id
    ) {
        const std::string id = schema_version_id(table);
        Value parent = parent_schema_version_id.empty() ? Value(std::monostate{}) : Value(parent_schema_version_id);
        driver_->execute(
            "INSERT INTO _chronos_branch_table_schema_versions "
            "(backend, table_name, schema_version_id, parent_schema_version_id, physical_table, "
            " pk_columns, columns, column_defs, ddl_op, created_at, metadata) "
            "VALUES ('interval', ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')",
            {
                table,
                id,
                parent,
                physical,
                json_string_array(pk_columns),
                json_string_array(columns),
                json_string_array(column_defs),
                ddl_op,
                current_timestamp_string(),
            }
        );
        return id;
    }

    void update_schema_version_metadata(
        const std::string &schema_version_id,
        const NativeTableMeta &meta,
        const std::string &ddl_op
    ) {
        driver_->execute(
            "UPDATE _chronos_branch_table_schema_versions "
            "SET pk_columns = ?, columns = ?, column_defs = ?, ddl_op = ?, metadata = '{}' "
            "WHERE backend = 'interval' AND schema_version_id = ?",
            {
                json_string_array(meta.pk_columns),
                json_string_array(meta.columns),
                json_string_array(meta.column_defs),
                ddl_op,
                schema_version_id,
            }
        );
    }

    std::optional<std::vector<Value>> active_binding_for_table(
        const std::string &table,
        const NativeBranchSegment &segment
    ) {
        ensure_schema_branching_tables();
        auto rows = driver_->query(
            "SELECT schema_version_id, tombstone, live_lo, live_hi, metadata "
            "FROM _chronos_branch_table_bindings "
            "WHERE backend = 'interval' "
            "  AND table_name = ? "
            "  AND live_lo <= ? "
            "  AND ? < live_hi "
            "LIMIT 1",
            {table, segment.branch_point, segment.branch_point}
        );
        if (rows.empty()) return std::nullopt;
        return rows[0];
    }

    void insert_table_binding(
        const std::string &table,
        const std::string &schema_version_id,
        bool tombstone,
        const std::string &live_lo,
        const std::string &live_hi,
        const std::string &metadata
    ) {
        Value schema_id = schema_version_id.empty() ? Value(std::monostate{}) : Value(schema_version_id);
        driver_->execute(
            "INSERT INTO _chronos_branch_table_bindings "
            "(backend, table_name, schema_version_id, tombstone, live_lo, live_hi, created_at, metadata) "
            "VALUES ('interval', ?, ?, ?, ?, ?, ?, ?)",
            {
                table,
                schema_id,
                static_cast<std::int64_t>(tombstone ? 1 : 0),
                live_lo,
                live_hi,
                current_timestamp_string(),
                metadata.empty() ? std::string("{}") : metadata,
            }
        );
    }

    void splice_table_binding(
        const std::string &table,
        const std::string &schema_version_id,
        bool tombstone,
        const NativeBranchSegment &segment
    ) {
        // Schema bindings use the same interval splice idea as row data: delete
        // every overlapping binding row, preserve left/right remainders, then
        // insert the branch-local replacement over the current segment interval.
        // Readers choose a schema by testing their branch point against this
        // interval map, so the metadata publish step is a single SQL transaction.
        lock_schema_binding_table(table);
        auto rows = driver_->query(
            "SELECT schema_version_id, tombstone, live_lo, live_hi, metadata "
            "FROM _chronos_branch_table_bindings "
            "WHERE backend = 'interval' "
            "  AND table_name = ? "
            "  AND live_lo < ? "
            "  AND ? < live_hi "
            "ORDER BY live_lo" + std::string(metadata_dialect() == "postgres" ? " FOR UPDATE" : ""),
            {table, segment.live_hi, segment.live_lo}
        );
        for (const auto &row : rows) {
            const std::string row_schema = native_as_string(row[0]);
            const bool row_tombstone = native_as_int(row[1]) != 0;
            const std::string a = native_as_string(row[2]);
            const std::string b = native_as_string(row[3]);
            const std::string metadata = native_as_string(row[4]);
            const cpp_int ai = cpp_int_from_decimal(a);
            const cpp_int bi = cpp_int_from_decimal(b);
            const cpp_int ulo = cpp_int_from_decimal(segment.live_lo);
            const cpp_int uhi = cpp_int_from_decimal(segment.live_hi);
            const cpp_int overlap_lo = ai > ulo ? ai : ulo;
            const cpp_int overlap_hi = bi < uhi ? bi : uhi;
            driver_->execute(
                "DELETE FROM _chronos_branch_table_bindings "
                "WHERE backend = 'interval' AND table_name = ? AND live_lo = ?",
                {table, a}
            );
            if (ai < overlap_lo) {
                insert_table_binding(table, row_schema, row_tombstone, a, cpp_int_to_decimal(overlap_lo), metadata);
            }
            if (overlap_hi < bi) {
                insert_table_binding(table, row_schema, row_tombstone, cpp_int_to_decimal(overlap_hi), b, metadata);
            }
        }
        insert_table_binding(table, schema_version_id, tombstone, segment.live_lo, segment.live_hi, "{}");
    }

    bool schema_version_private_to_ref(
        const std::string &branch_id,
        const std::string &table,
        const std::string &schema_version_id,
        const NativeBranchSegment &segment
    ) {
        // In-place DDL is only safe when no branch, checkpoint, or fork-base can
        // still resolve to this schema version. Otherwise Chronos must create a
        // fresh physical table and splice the schema binding so existing refs
        // continue to see their original schema.
        if (schema_version_id.empty()) return false;
        auto current = driver_->query(
            "SELECT current_segment_id FROM _chronos_branch_interval_branches WHERE branch_id = ?",
            {branch_id}
        );
        if (current.empty() || native_as_int(current[0][0]) != segment.segment_id) return false;
        auto other_branch = driver_->query(
            "SELECT 1 "
            "FROM _chronos_branch_interval_branches b "
            "JOIN _chronos_branch_interval_segments s ON s.segment_id = b.current_segment_id "
            "JOIN _chronos_branch_table_bindings tb "
            "  ON tb.live_lo <= s.branch_point AND s.branch_point < tb.live_hi "
            "WHERE b.branch_id <> ? "
            "  AND tb.backend = 'interval' "
            "  AND tb.table_name = ? "
            "  AND tb.schema_version_id = ? "
            "  AND tb.tombstone = 0 "
            "LIMIT 1",
            {branch_id, table, schema_version_id}
        );
        if (!other_branch.empty()) return false;
        auto checkpoint = driver_->query(
            "SELECT 1 "
            "FROM _chronos_branch_interval_checkpoints cp "
            "JOIN _chronos_branch_interval_segments s ON s.segment_id = cp.segment_id "
            "JOIN _chronos_branch_table_bindings tb "
            "  ON tb.live_lo <= s.branch_point AND s.branch_point < tb.live_hi "
            "WHERE tb.backend = 'interval' "
            "  AND tb.table_name = ? "
            "  AND tb.schema_version_id = ? "
            "  AND tb.tombstone = 0 "
            "LIMIT 1",
            {table, schema_version_id}
        );
        if (!checkpoint.empty()) return false;
        auto fork_base = driver_->query(
            "SELECT 1 "
            "FROM _chronos_branch_interval_segments s "
            "JOIN _chronos_branch_table_bindings tb "
            "  ON tb.live_lo <= s.branch_point AND s.branch_point < tb.live_hi "
            "WHERE s.segment_kind = 'fork_base' "
            "  AND tb.backend = 'interval' "
            "  AND tb.table_name = ? "
            "  AND tb.schema_version_id = ? "
            "  AND tb.tombstone = 0 "
            "LIMIT 1",
            {table, schema_version_id}
        );
        return fork_base.empty();
    }

    void copy_visible_rows_to_schema_version(
        const NativeTableMeta &old_meta,
        const NativeTableMeta &new_meta,
        const NativeBranchSegment &segment,
        const std::unordered_map<std::string, std::string> &default_sql_by_column = {},
        const std::unordered_map<std::string, std::string> &select_sql_by_column = {}
    ) {
        // Copy only rows visible at the branch point into the new physical table.
        // The copied rows receive the new segment's interval, which makes the
        // schema version switch branch-local without changing reader predicates.
        std::vector<std::string> target_columns = new_meta.columns;
        target_columns.push_back("live_lo");
        target_columns.push_back("live_hi");
        target_columns.push_back("writer_segment_id");
        target_columns.push_back("deleted");
        std::unordered_set<std::string> old_columns(old_meta.columns.begin(), old_meta.columns.end());
        std::vector<std::string> select_exprs;
        for (const auto &column : new_meta.columns) {
            if (auto found = select_sql_by_column.find(column); found != select_sql_by_column.end()) {
                select_exprs.push_back(found->second + " AS " + quote_ident(column));
            } else if (old_columns.find(column) != old_columns.end()) {
                select_exprs.push_back(quote_ident(column));
            } else if (auto default_found = default_sql_by_column.find(column); default_found != default_sql_by_column.end()) {
                select_exprs.push_back(default_found->second + " AS " + quote_ident(column));
            } else {
                select_exprs.push_back("NULL AS " + quote_ident(column));
            }
        }
        select_exprs.push_back("?");
        select_exprs.push_back("?");
        select_exprs.push_back("?");
        select_exprs.push_back("FALSE");
        driver().execute(
            "INSERT INTO " + quote_ident(new_meta.physical_name) +
            " (" + comma_join_quoted(target_columns) + ") "
            "SELECT " + join_strings(select_exprs, ", ") +
            " FROM " + quote_ident(old_meta.physical_name) +
            " WHERE live_lo <= ? AND ? < live_hi AND deleted = FALSE",
            {segment.live_lo, segment.live_hi, segment.segment_id, segment.branch_point, segment.branch_point}
        );
    }

// -----------------------------------------------------------------------------
// Fast conflict-free merge apply path
// Source: interval_branch_store_direct_merge.cpp
// -----------------------------------------------------------------------------
    struct NativeBranchCommitReservation {
        NativeDirectMergeSegment merge_segment;
        NativeDirectMergeSegment continuation_segment;
        bool has_commit_record = false;
    };

    std::int64_t merge_apply(const std::string &source, const std::string &target) {
        return merge_apply_excluding_first_key_values(source, target, "", {});
    }

    std::int64_t merge_apply_excluding_first_key_values(
        const std::string &source,
        const std::string &target,
        const std::string &excluded_table,
        const std::vector<std::int64_t> &excluded_values
    ) {
        NativeSessionBarrierGuard guard = begin_session_barrier(
            {source, target}, "merge"
        );
        try {
            if (guard.installed) suspend_session_barrier_transaction(guard);
            const auto applied = merge_apply_excluding_first_key_values_unfenced(
                source, target, excluded_table, excluded_values
            );
            finish_session_barrier(guard);
            return applied;
        } catch (...) {
            abort_session_barrier(guard);
            throw;
        }
    }

    std::int64_t merge_apply_excluding_first_key_values_unfenced(
        const std::string &source,
        const std::string &target,
        const std::string &excluded_table,
        const std::vector<std::int64_t> &excluded_values
    ) {
        const std::unordered_set<std::int64_t> excluded_first_keys(
            excluded_values.begin(),
            excluded_values.end()
        );
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        bool metadata_tx_open = started_tx;
        try {
            NativeMergePlan merge_plan = load_merge_plan(source, target);
            NativeDirectMergeSegment source_segment = merge_plan.source;
            NativeDirectMergeSegment target_segment = merge_plan.target;
            NativeDirectMergeSegment base_segment = merge_plan.base;
            const NativeDirectMergeSegment original_target_segment = target_segment;

            // Merge is staged into an immutable merge segment, followed by a
            // mutable continuation segment that becomes the new branch head.
            // Old readers that already checked out the target keep resolving
            // against the old branch head; new readers see merge rows through
            // the continuation branch point.
            struct PendingMergeTable {
                NativeTableMeta meta;
                NativeRows upserts;
                NativeRows deletes;
            };
            std::vector<PendingMergeTable> pending_tables;
            std::int64_t applied = 0;
            NativeBranchSegment base_read{
                base_segment.segment_id,
                base_segment.live_lo,
                base_segment.live_hi,
                base_segment.branch_point,
            };
            NativeBranchSegment source_read{
                source_segment.segment_id,
                source_segment.live_lo,
                source_segment.live_hi,
                source_segment.branch_point,
            };
            NativeBranchSegment target_read{
                original_target_segment.segment_id,
                original_target_segment.live_lo,
                original_target_segment.live_hi,
                original_target_segment.branch_point,
            };

            for (const auto &meta : load_table_metas(target_read)) {
                if (meta.has_schema_binding) {
                    throw std::runtime_error("chronos_native_merge_unsupported: schema-branching merge");
                }
                std::vector<std::int64_t> candidate_writer_segments = merge_plan.source_writer_segments;
                candidate_writer_segments.insert(
                    candidate_writer_segments.end(),
                    merge_plan.target_writer_segments.begin(),
                    merge_plan.target_writer_segments.end()
                );
                std::vector<NativeRowKey> keys = candidate_keys_for_writer_segments(
                    meta,
                    candidate_writer_segments
                );
                if (meta.logical_name == excluded_table && !excluded_first_keys.empty()) {
                    keys.erase(
                        std::remove_if(
                            keys.begin(),
                            keys.end(),
                            [&excluded_first_keys](const NativeRowKey &key) {
                                if (key.values.empty()) return false;
                                const auto *value =
                                    std::get_if<std::int64_t>(&key.values.front());
                                return value != nullptr &&
                                    excluded_first_keys.find(*value) !=
                                        excluded_first_keys.end();
                            }),
                        keys.end()
                    );
                }
                if (keys.empty()) continue;

                // Only keys touched by either side since the fork base can
                // affect merge output.  For each key we compare the base,
                // source, and target visible row states: source-only changes
                // are applied; target changes conflicting with source changes
                // reject the merge.
                auto [base_rows, source_rows, target_rows] = visible_rows_by_key_for_merge(
                    meta,
                    keys,
                    base_segment.branch_point,
                    source_segment.branch_point,
                    target_segment.branch_point
                );

                NativeRows upserts;
                NativeRows deletes;
                for (const auto &key : keys) {
                    const NativeMergeRowState base = row_state_for_key(base_rows, key);
                    const NativeMergeRowState source_state = row_state_for_key(source_rows, key);
                    const NativeMergeRowState target_state = row_state_for_key(target_rows, key);
                    if (row_state_equal(source_state, target_state)) {
                        if (!row_state_equal(source_state, base)) {
                            throw std::runtime_error("chronos_native_merge_conflict");
                        }
                        continue;
                    }
                    if (row_state_equal(source_state, base)) {
                        continue;
                    }
                    if (!row_state_equal(target_state, base)) {
                        throw std::runtime_error("chronos_native_merge_conflict");
                    }
                    if (source_state.present) {
                        upserts.push_back(source_state.row);
                    } else {
                        deletes.push_back(tombstone_row_for_key(meta, key));
                    }
                }

                if (upserts.empty() && deletes.empty()) continue;
                applied += static_cast<std::int64_t>(upserts.size() + deletes.size());
                pending_tables.push_back(PendingMergeTable{
                    meta,
                    std::move(upserts),
                    std::move(deletes),
                });
            }

            if (applied > 0) {
                NativeBranchCommitReservation reservation = reserve_branch_transaction_commit(
                    original_target_segment,
                    source,
                    target
                );
                const bool committed_split_reservation = split_store() && started_tx;
                if (committed_split_reservation) {
                    // Split stores need the reservation durable before data
                    // writes so a node failure leaves a recoverable commit
                    // record. Single-store commits keep reservation, data
                    // writes, and publish inside one native transaction.
                    driver_->execute("COMMIT");
                    metadata_tx_open = false;
                }
                const bool started_data_tx = split_store() && !driver().in_transaction();
                if (started_data_tx) {
                    driver().execute(dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
                }
                bool data_committed = !started_data_tx;
                try {
                    for (const auto &pending : pending_tables) {
                        const auto &meta = pending.meta;
                        if (!pending.deletes.empty()) {
                            driver().interval_upsert(
                                meta.physical_name,
                                meta.columns,
                                meta.pk_columns,
                                pending.deletes,
                                reservation.merge_segment.live_lo,
                                reservation.merge_segment.live_hi,
                                reservation.merge_segment.segment_id,
                                true,
                                false,
                                IntervalWriteMode::Upsert,
                                reservation.merge_segment.branch_point
                            );
                        }
                        if (!pending.upserts.empty()) {
                            driver().interval_upsert(
                                meta.physical_name,
                                meta.columns,
                                meta.pk_columns,
                                pending.upserts,
                                reservation.merge_segment.live_lo,
                                reservation.merge_segment.live_hi,
                                reservation.merge_segment.segment_id,
                                false,
                                false,
                                IntervalWriteMode::Upsert,
                                reservation.merge_segment.branch_point
                            );
                        }
                    }
                    if (started_data_tx) driver().execute("COMMIT");
                    data_committed = true;
                } catch (...) {
                    if (started_data_tx) {
                        try {
                            driver().execute("ROLLBACK");
                        } catch (...) {
                        }
                    }
                    if (committed_split_reservation && !data_committed) {
                        cleanup_branch_transaction_commit_reservation(reservation, target);
                    }
                    throw;
                }

                if (committed_split_reservation) {
                    driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
                    metadata_tx_open = true;
                }
                // This is the only visibility flip: the target branch moves
                // from its old mutable segment to the mutable continuation.
                // The merge segment stays immutable underneath it.
                publish_branch_transaction_commit(reservation, target);
            }
            if (started_tx && metadata_tx_open) driver_->execute("COMMIT");
            metadata_tx_open = false;
            return applied;
        } catch (...) {
            if (started_tx && metadata_tx_open) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    void acquire_external_transaction_lock(const std::string &target) {
        if (metadata_dialect() != "postgres") return;
        const std::int32_t key = static_cast<std::int32_t>(
            stable_fnv1a64(target) & 0xffffffffULL
        );
        if (external_transaction_lock_.has_value()) {
            if (external_transaction_lock_->second == target) return;
            throw std::runtime_error(
                "chronos_native_merge_conflict: connection already owns a target reservation"
            );
        }
        // A session-level advisory lock blocks at the relational metadata
        // plane until the previous publisher releases the target.  It is
        // deliberately held across reservation, staging, and publication;
        // the active commit row remains the crash-visible recovery record.
        driver_->execute(
            "SELECT pg_advisory_lock(1720812903, ?::int)",
            {key}
        );
        external_transaction_lock_ = std::make_pair(key, target);
    }

    void release_external_transaction_lock() {
        if (!external_transaction_lock_.has_value()) return;
        const std::int32_t key = external_transaction_lock_->first;
        external_transaction_lock_.reset();
        if (metadata_dialect() != "postgres") return;
        try {
            driver_->execute(
                "SELECT pg_advisory_unlock(1720812903, ?::int)",
                {key}
            );
        } catch (...) {
            // Connection teardown releases session locks even if the unlock
            // itself cannot be issued after an earlier SQL error.
        }
    }

    void wait_for_external_commit_slot(const std::string &target) {
        if (metadata_dialect() == "postgres") return;
        constexpr auto wait_step = std::chrono::milliseconds(10);
        constexpr auto wait_limit = std::chrono::seconds(60);
        const auto deadline = std::chrono::steady_clock::now() + wait_limit;
        while (true) {
            auto active = driver_->query(
                "SELECT merge_segment_id "
                "FROM _chronos_branch_transaction_commits "
                "WHERE target_branch_id = ? LIMIT 1",
                {target}
            );
            if (active.empty()) return;
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(
                    "chronos_native_merge_conflict: timed out waiting for target branch commit"
                );
            }
            std::this_thread::sleep_for(wait_step);
        }
    }

    chronos::native::NativeBranchTransaction reserve_external_branch_transaction(
        const std::string &source,
        const std::string &target,
        const std::string &participant_stores,
        const std::string &metadata_json
    ) {
        if (external_session_barrier_.has_value()) {
            throw std::runtime_error(
                "chronos_native_merge_conflict: connection already owns a session barrier"
            );
        }
        NativeSessionBarrierGuard session_guard = begin_session_barrier(
            {source, target}, "merge"
        );
        // The reservation must be durable while participant stores stage data.
        // The barrier rows, rather than this transaction, retain the fence.
        suspend_session_barrier_transaction(session_guard);
        bool started_tx = false;
        try {
            acquire_external_transaction_lock(target);
            wait_for_external_commit_slot(target);
            started_tx = !driver_->in_transaction();
            if (started_tx) {
                driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
            }
            NativeMergePlan merge_plan = load_merge_plan(source, target, true);
            NativeBranchCommitReservation reservation = reserve_branch_transaction_commit(
                merge_plan.target,
                source,
                target,
                true,
                participant_stores,
                metadata_json
            );
            if (started_tx) driver_->execute("COMMIT");
            external_session_barrier_ = std::move(session_guard);
            return {
                reservation.merge_segment.segment_id,
                reservation.continuation_segment.segment_id,
                reservation.merge_segment.parent_segment_id,
                reservation.merge_segment.live_lo,
                reservation.merge_segment.live_hi,
                reservation.merge_segment.branch_point,
                reservation.continuation_segment.live_lo,
                reservation.continuation_segment.live_hi,
                reservation.continuation_segment.branch_point,
            };
        } catch (...) {
            if (started_tx && driver_->in_transaction()) {
                try { driver_->execute("ROLLBACK"); } catch (...) {}
            }
            release_external_transaction_lock();
            abort_session_barrier(session_guard);
            throw;
        }
    }

    NativeBranchCommitReservation external_reservation(
        const chronos::native::NativeBranchTransaction &transaction
    ) {
        NativeDirectMergeSegment merge_segment{
            transaction.merge_segment_id,
            transaction.old_target_segment_id,
            "merge",
            transaction.merge_live_lo,
            transaction.merge_live_hi,
            transaction.merge_branch_point,
        };
        NativeDirectMergeSegment continuation_segment{
            transaction.continuation_segment_id,
            transaction.merge_segment_id,
            "mutable",
            transaction.continuation_live_lo,
            transaction.continuation_live_hi,
            transaction.continuation_branch_point,
        };
        return {merge_segment, continuation_segment, true};
    }

    std::int64_t stage_external_branch_transaction_changes(
        const chronos::native::NativeBranchTransaction &transaction,
        const std::vector<chronos::native::NativeMergeChange> &changes
    ) {
        if (changes.empty()) return 0;
        const bool started_tx = !driver().in_transaction();
        if (started_tx) {
            driver().execute(dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        }
        try {
            NativeBranchCommitReservation reservation = external_reservation(transaction);
            NativeBranchSegment merge_segment{
                reservation.merge_segment.segment_id,
                reservation.merge_segment.live_lo,
                reservation.merge_segment.live_hi,
                reservation.merge_segment.branch_point,
            };
            std::unordered_map<std::string, std::vector<chronos::native::NativeMergeChange>> by_table;
            for (const auto &change : changes) by_table[change.table].push_back(change);
            std::int64_t applied = 0;
            for (const auto &[table, table_changes] : by_table) {
                NativeTableMeta meta = load_table_meta(table, merge_segment);
                NativeRows upserts;
                NativeRows deletes;
                for (const auto &change : table_changes) {
                    if (change.change == "deleted") {
                        deletes.push_back(tombstone_values_for_key(meta, change));
                    } else {
                        if (!change.has_after) {
                            throw std::runtime_error("merge change is missing source row");
                        }
                        upserts.push_back(values_for_columns(
                            meta.columns,
                            change.after_columns,
                            change.after_values
                        ));
                    }
                }
                if (!deletes.empty()) {
                    driver().interval_upsert(
                        meta.physical_name, meta.columns, meta.pk_columns, deletes,
                        reservation.merge_segment.live_lo,
                        reservation.merge_segment.live_hi,
                        reservation.merge_segment.segment_id,
                        true, false, IntervalWriteMode::Upsert,
                        reservation.merge_segment.branch_point
                    );
                    applied += static_cast<std::int64_t>(deletes.size());
                }
                if (!upserts.empty()) {
                    driver().interval_upsert(
                        meta.physical_name, meta.columns, meta.pk_columns, upserts,
                        reservation.merge_segment.live_lo,
                        reservation.merge_segment.live_hi,
                        reservation.merge_segment.segment_id,
                        false, false, IntervalWriteMode::Upsert,
                        reservation.merge_segment.branch_point
                    );
                    applied += static_cast<std::int64_t>(upserts.size());
                }
            }
            if (started_tx) driver().execute("COMMIT");
            return applied;
        } catch (...) {
            if (started_tx && driver().in_transaction()) {
                try { driver().execute("ROLLBACK"); } catch (...) {}
            }
            throw;
        }
    }

    void publish_external_branch_transaction(
        const std::string &target,
        const chronos::native::NativeBranchTransaction &transaction
    ) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) {
            driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        }
        try {
            publish_branch_transaction_commit(external_reservation(transaction), target);
            if (started_tx) driver_->execute("COMMIT");
            release_external_transaction_lock();
            if (external_session_barrier_.has_value()) {
                finish_session_barrier(*external_session_barrier_);
                external_session_barrier_.reset();
            }
        } catch (...) {
            if (started_tx && driver_->in_transaction()) {
                try { driver_->execute("ROLLBACK"); } catch (...) {}
            }
            release_external_transaction_lock();
            if (external_session_barrier_.has_value()) {
                abort_session_barrier(*external_session_barrier_);
                external_session_barrier_.reset();
            }
            throw;
        }
    }

    void abort_external_branch_transaction(
        const std::string &target,
        const chronos::native::NativeBranchTransaction &transaction
    ) {
        try {
            cleanup_branch_transaction_commit_reservation(
                external_reservation(transaction),
                target
            );
        } catch (...) {
            release_external_transaction_lock();
            if (external_session_barrier_.has_value()) {
                abort_session_barrier(*external_session_barrier_);
                external_session_barrier_.reset();
            }
            throw;
        }
        release_external_transaction_lock();
        if (external_session_barrier_.has_value()) {
            finish_session_barrier(*external_session_barrier_);
            external_session_barrier_.reset();
        }
    }

  private:

// -----------------------------------------------------------------------------
// Shared ancestry, visibility, and row-diff helpers used by diff/merge
// Source: interval_branch_store_merge_helpers.cpp
// -----------------------------------------------------------------------------
    std::int64_t allocate_segment_id() { return allocate_segment_ids(1)[0]; }

    std::vector<std::int64_t> interval_dead_segment_ids() {
        auto rows = driver_->query(
            "WITH RECURSIVE reachable(segment_id) AS ("
            "    SELECT current_segment_id "
            "    FROM _chronos_branch_interval_branches "
            "  UNION "
            "    SELECT segment_id "
            "    FROM _chronos_branch_interval_checkpoints "
            "  UNION "
            "    SELECT merge_segment_id "
            "    FROM _chronos_branch_transaction_commits "
            "  UNION "
            "    SELECT continuation_segment_id "
            "    FROM _chronos_branch_transaction_commits "
            "  UNION "
            "    SELECT parent.segment_id "
            "    FROM _chronos_branch_interval_segments AS child "
            "    JOIN reachable "
            "      ON reachable.segment_id = child.segment_id "
            "    JOIN _chronos_branch_interval_segments AS parent "
            "      ON parent.segment_id = child.parent_segment_id "
            ") "
            "SELECT segment_id "
            "FROM _chronos_branch_interval_segments "
            "WHERE segment_id NOT IN (SELECT segment_id FROM reachable) "
            "ORDER BY segment_id"
        );
        std::vector<std::int64_t> ids;
        ids.reserve(rows.size());
        for (const auto &row : rows) {
            ids.push_back(native_as_int(row[0]));
        }
        return ids;
    }

    std::vector<std::string> interval_physical_tables_for_gc() {
        std::unordered_set<std::string> tables;
        auto rows = driver_->query(
            "SELECT physical_table "
            "FROM _chronos_branch_tables "
            "WHERE backend = 'interval'"
        );
        for (const auto &row : rows) {
            const std::string physical = native_as_string(row[0]);
            if (table_exists(physical)) tables.insert(physical);
        }
        if (table_exists("_chronos_branch_table_schema_versions")) {
            auto schema_rows = driver_->query(
                "SELECT physical_table "
                "FROM _chronos_branch_table_schema_versions "
                "WHERE backend = 'interval'"
            );
            for (const auto &row : schema_rows) {
                const std::string physical = native_as_string(row[0]);
                if (table_exists(physical)) tables.insert(physical);
            }
        }
        std::vector<std::string> out(tables.begin(), tables.end());
        std::sort(out.begin(), out.end());
        return out;
    }

    void delete_writer_rows_for_segments(
        const std::string &physical,
        const std::vector<std::int64_t> &segment_ids
    ) {
        for (std::size_t start = 0; start < segment_ids.size(); start += 1000) {
            const std::size_t count = std::min<std::size_t>(1000, segment_ids.size() - start);
            std::vector<Value> params;
            params.reserve(count);
            bool all_non_root = true;
            for (std::size_t i = 0; i < count; ++i) {
                const std::int64_t segment_id = segment_ids[start + i];
                if (segment_id <= 1) all_non_root = false;
                params.push_back(segment_id);
            }
            std::string sql = "DELETE FROM " + quote_ident(physical) +
                " WHERE writer_segment_id IN (" + placeholders(count) + ")";
            if (dialect() == "postgres" && all_non_root) {
                sql += " AND writer_segment_id > 1";
            }
            driver().execute(
                sql,
                params
            );
        }
    }

    std::int64_t collect_interval_garbage_sqlite_batched() {
        const std::vector<std::int64_t> dead_segment_ids =
            interval_dead_segment_ids();
        if (dead_segment_ids.empty()) return 0;

        constexpr std::size_t segment_batch_size = 500;
        constexpr std::int64_t row_batch_size = 256;
        for (const auto &physical : interval_physical_tables_for_gc()) {
            for (
                std::size_t start = 0;
                start < dead_segment_ids.size();
                start += segment_batch_size
            ) {
                const std::size_t count = std::min<std::size_t>(
                    segment_batch_size,
                    dead_segment_ids.size() - start
                );
                std::vector<Value> params;
                params.reserve(count);
                for (std::size_t offset = 0; offset < count; ++offset) {
                    params.push_back(dead_segment_ids[start + offset]);
                }
                const std::string sql =
                    "DELETE FROM " + quote_ident(physical) +
                    " WHERE rowid IN ("
                    "SELECT rowid FROM " + quote_ident(physical) +
                    " WHERE writer_segment_id IN (" + placeholders(count) + ")"
                    " LIMIT " + std::to_string(row_batch_size) +
                    ")";
                while (true) {
                    driver().execute("BEGIN IMMEDIATE");
                    std::int64_t deleted = 0;
                    try {
                        deleted = driver().execute_changes(sql, params);
                        driver().execute("COMMIT");
                    } catch (...) {
                        try {
                            driver().execute("ROLLBACK");
                        } catch (...) {
                        }
                        throw;
                    }
                    if (deleted == 0) break;
                    // Release the SQLite writer lock between small batches so
                    // foreground branch operations can make progress while
                    // physical reclamation continues in the background.
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
        }

        driver_->execute("BEGIN IMMEDIATE");
        try {
            delete_dead_interval_segments(dead_segment_ids);
            driver_->execute("COMMIT");
        } catch (...) {
            try {
                driver_->execute("ROLLBACK");
            } catch (...) {
            }
            throw;
        }
        return static_cast<std::int64_t>(dead_segment_ids.size());
    }

    void delete_dead_interval_segments(const std::vector<std::int64_t> &segment_ids) {
        for (std::size_t start = 0; start < segment_ids.size(); start += 1000) {
            const std::size_t count = std::min<std::size_t>(1000, segment_ids.size() - start);
            std::vector<Value> params;
            params.reserve(count);
            for (std::size_t i = 0; i < count; ++i) {
                params.push_back(segment_ids[start + i]);
            }
            driver_->execute(
                "DELETE FROM _chronos_branch_interval_segments "
                "WHERE segment_id IN (" + placeholders(count) + ")",
                params
            );
        }
    }

    chronos::native::NativeBranchInfo branch_info_from_row(const std::vector<Value> &row) {
        return {
            native_as_string(row[0]),
            native_as_string(row[1]),
            native_as_string(row[2]),
            native_as_string(row[3]),
        };
    }

    chronos::native::NativeCheckpointInfo checkpoint_info_from_row(const std::vector<Value> &row) {
        return {
            native_as_string(row[0]),
            native_as_string(row[1]),
            native_as_string(row[2]),
            native_as_string(row[3]),
            native_as_string(row[4]),
        };
    }

    void insert_interval_segment(
        const NativeDirectMergeSegment &segment,
        std::int64_t parent_segment_id,
        const std::string &owner_branch_id,
        const std::string &segment_kind,
        const std::string &created_at
    ) {
        Value owner = owner_branch_id.empty() ? Value(std::monostate{}) : Value(owner_branch_id);
        driver_->execute(
            "INSERT INTO _chronos_branch_interval_segments "
            "(segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi, "
            " branch_point, created_at, metadata) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}')",
            {
                segment.segment_id,
                parent_segment_id,
                owner,
                segment_kind,
                segment.live_lo,
                segment.live_hi,
                segment.branch_point,
                created_at,
            }
        );
    }

    std::pair<NativeDirectMergeSegment, NativeDirectMergeSegment> split_checkpoint_segment(
        const NativeDirectMergeSegment &segment,
        int checkpoints_created
    ) {
        const cpp_int lo = cpp_int_from_decimal(segment.live_lo);
        const cpp_int hi = cpp_int_from_decimal(segment.live_hi);
        const cpp_int active = hi - lo - 1;
        if (active < 2) {
            throw std::runtime_error("interval space exhausted");
        }
        const cpp_int width = hi - lo;
        cpp_int snapshot_width = active / (harmonic_reserve_ + checkpoints_created + 1);
        if (snapshot_width < 2) throw std::runtime_error("interval space exhausted");
        if (snapshot_width > active - 2) snapshot_width = active - 2;
        const cpp_int split = lo + snapshot_width;
        std::vector<std::int64_t> ids = allocate_segment_ids(2);
        NativeDirectMergeSegment continuation{
            ids[0],
            segment.segment_id,
            "mutable",
            cpp_int_to_decimal(split),
            cpp_int_to_decimal(hi),
            cpp_int_to_decimal(split + (hi - split) / 2),
        };
        NativeDirectMergeSegment snapshot{
            ids[1],
            segment.segment_id,
            "checkpoint",
            cpp_int_to_decimal(lo),
            cpp_int_to_decimal(split),
            cpp_int_to_decimal(lo + (split - lo) / 2),
        };
        return {continuation, snapshot};
    }

    std::tuple<NativeDirectMergeSegment, NativeDirectMergeSegment, NativeDirectMergeSegment>
    load_direct_sibling_merge_segments(const std::string &source, const std::string &target) {
        const std::string lock_suffix = metadata_dialect() == "postgres" ? " FOR UPDATE" : "";
        auto locked = driver_->query(
            "SELECT branch_id, current_segment_id "
            "FROM _chronos_branch_interval_branches "
            "WHERE branch_id IN (?, ?) "
            "ORDER BY branch_id" + lock_suffix,
            {source, target}
        );
        if (locked.size() != 2) {
            throw std::runtime_error("chronos_native_merge_unsupported: branch not found");
        }
        std::int64_t source_segment_id = 0;
        std::int64_t target_segment_id = 0;
        for (const auto &row : locked) {
            const std::string branch_id = native_as_string(row[0]);
            if (branch_id == source) source_segment_id = native_as_int(row[1]);
            if (branch_id == target) target_segment_id = native_as_int(row[1]);
        }
        if (source_segment_id == 0 || target_segment_id == 0) {
            throw std::runtime_error("chronos_native_merge_unsupported: branch not found");
        }

        auto rows = driver_->query(
            "SELECT "
            "  source.segment_id, source.parent_segment_id, source.segment_kind, "
            "  source.live_lo, source.live_hi, source.branch_point, "
            "  target.segment_id, target.parent_segment_id, target.segment_kind, "
            "  target.live_lo, target.live_hi, target.branch_point, "
            "  base.segment_id, base.parent_segment_id, base.segment_kind, "
            "  base.live_lo, base.live_hi, base.branch_point "
            "FROM _chronos_branch_interval_segments AS source "
            "JOIN _chronos_branch_interval_segments AS target "
            "  ON target.segment_id = ? "
            " AND target.parent_segment_id = source.parent_segment_id "
            "JOIN _chronos_branch_interval_segments AS base "
            "  ON base.segment_id = source.parent_segment_id "
            " AND base.segment_kind = 'fork_base' "
            "WHERE source.segment_id = ?",
            {target_segment_id, source_segment_id}
        );
        if (rows.empty()) {
            throw std::runtime_error("chronos_native_merge_unsupported: non-direct merge ancestry");
        }
        const auto &row = rows[0];
        NativeDirectMergeSegment src{
            native_as_int(row[0]),
            native_as_int(row[1]),
            native_as_string(row[2]),
            native_as_string(row[3]),
            native_as_string(row[4]),
            native_as_string(row[5]),
        };
        NativeDirectMergeSegment dst{
            native_as_int(row[6]),
            native_as_int(row[7]),
            native_as_string(row[8]),
            native_as_string(row[9]),
            native_as_string(row[10]),
            native_as_string(row[11]),
        };
        NativeDirectMergeSegment base{
            native_as_int(row[12]),
            native_as_int(row[13]),
            native_as_string(row[14]),
            native_as_string(row[15]),
            native_as_string(row[16]),
            native_as_string(row[17]),
        };
        if (src.kind != "mutable" || dst.kind != "mutable") {
            throw std::runtime_error("chronos_native_merge_unsupported: non-mutable merge segment");
        }
        return {src, dst, base};
    }

    std::pair<std::int64_t, std::int64_t> locked_current_segment_ids(
        const std::string &source,
        const std::string &target,
        bool for_update = true
    ) {
        const std::string lock_suffix = (for_update && metadata_dialect() == "postgres") ? " FOR UPDATE" : "";
        auto locked = driver_->query(
            "SELECT branch_id, current_segment_id "
            "FROM _chronos_branch_interval_branches "
            "WHERE branch_id IN (?, ?) "
            "ORDER BY branch_id" + lock_suffix,
            {source, target}
        );
        if (locked.size() != 2) {
            throw std::runtime_error("chronos_native_merge_unsupported: branch not found");
        }
        std::int64_t source_segment_id = 0;
        std::int64_t target_segment_id = 0;
        for (const auto &row : locked) {
            const std::string branch_id = native_as_string(row[0]);
            if (branch_id == source) source_segment_id = native_as_int(row[1]);
            if (branch_id == target) target_segment_id = native_as_int(row[1]);
        }
        if (source_segment_id == 0 || target_segment_id == 0) {
            throw std::runtime_error("chronos_native_merge_unsupported: branch not found");
        }
        return {source_segment_id, target_segment_id};
    }

    void lock_branch_rows_for_merge(const std::string &source, const std::string &target) {
        if (metadata_dialect() != "postgres") return;
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute("BEGIN");
        try {
            // Reuse the same deterministic, batched row lock as merge apply.
            // Keeping this behind NativeBranchStore avoids Python issuing
            // metadata SQL directly while preserving BranchSession.merge's
            // existing lock-before-preview behavior.
            (void)locked_current_segment_ids(source, target, true);
            if (started_tx) driver_->execute("COMMIT");
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    std::vector<NativeDirectMergeSegment> segment_ancestry(std::int64_t segment_id) {
        auto rows = driver_->query(
            "WITH RECURSIVE ancestry(segment_id, parent_segment_id, segment_kind, live_lo, live_hi, branch_point, depth) AS ("
            "  SELECT segment_id, COALESCE(parent_segment_id, 0), segment_kind, live_lo, live_hi, branch_point, 0 AS depth "
            "  FROM _chronos_branch_interval_segments "
            "  WHERE segment_id = ? "
            "UNION ALL "
            "  SELECT parent.segment_id, COALESCE(parent.parent_segment_id, 0), parent.segment_kind, "
            "         parent.live_lo, parent.live_hi, parent.branch_point, ancestry.depth + 1 AS depth "
            "  FROM _chronos_branch_interval_segments AS parent "
            "  JOIN ancestry ON parent.segment_id = ancestry.parent_segment_id "
            ") "
            "SELECT segment_id, parent_segment_id, segment_kind, live_lo, live_hi, branch_point "
            "FROM ancestry "
            "ORDER BY depth DESC",
            {segment_id}
        );
        if (rows.empty()) {
            throw std::runtime_error("chronos_native_merge_unsupported: segment not found");
        }
        std::vector<NativeDirectMergeSegment> path;
        path.reserve(rows.size());
        for (const auto &row : rows) {
            path.push_back({
                native_as_int(row[0]),
                native_as_int(row[1]),
                native_as_string(row[2]),
                native_as_string(row[3]),
                native_as_string(row[4]),
                native_as_string(row[5]),
            });
        }
        return path;
    }

    std::optional<NativeDirectMergeSegment> nearest_shared_fork_base(
        const std::vector<NativeDirectMergeSegment> &left,
        const std::vector<NativeDirectMergeSegment> &right
    ) {
        std::unordered_set<std::int64_t> right_ids;
        right_ids.reserve(right.size());
        for (const auto &segment : right) right_ids.insert(segment.segment_id);
        for (auto it = left.rbegin(); it != left.rend(); ++it) {
            if (it->kind == "fork_base" && right_ids.find(it->segment_id) != right_ids.end()) {
                return *it;
            }
        }
        return std::nullopt;
    }

    bool merge_writer_segment_kind(const std::string &kind) {
        return kind == "mutable" || kind == "merge";
    }

    std::vector<std::int64_t> writer_segment_ids_after_base(
        const std::vector<NativeDirectMergeSegment> &path,
        std::int64_t base_segment_id
    ) {
        bool after_base = false;
        std::vector<std::int64_t> ids;
        for (const auto &segment : path) {
            if (segment.segment_id == base_segment_id) {
                after_base = true;
                continue;
            }
            if (after_base && merge_writer_segment_kind(segment.kind)) {
                ids.push_back(segment.segment_id);
            }
        }
        if (!after_base) {
            throw std::runtime_error("chronos_native_merge_unsupported: base is not an ancestor");
        }
        return ids;
    }

    NativeMergePlan load_merge_plan(
        const std::string &source,
        const std::string &target,
        bool for_update = true
    ) {
        // The hot branch-transaction path creates a terminal child branch from
        // the target branch, applies a small write set, then merges it straight
        // back. In interval terms the source child and target continuation are
        // direct siblings whose parent is the immutable fork-base segment. That
        // shape does not need recursive ancestry discovery; the two current
        // segment ids are the only writer segments that can differ.
        if (for_update) {
            try {
                auto [source_segment, target_segment, base_segment] =
                    load_direct_sibling_merge_segments(source, target);
                return NativeMergePlan{
                    source_segment,
                    target_segment,
                    base_segment,
                    {source_segment.segment_id},
                    {target_segment.segment_id},
                };
            } catch (const std::exception &) {
                // Non-direct ancestry is still valid for general merge and
                // diff/merge APIs, so fall through to the complete planner.
            }
        }
        auto [source_segment_id, target_segment_id] = locked_current_segment_ids(
            source,
            target,
            for_update
        );
        auto source_path = segment_ancestry(source_segment_id);
        auto target_path = segment_ancestry(target_segment_id);
        auto base = nearest_shared_fork_base(source_path, target_path);
        if (!base) {
            throw std::runtime_error("chronos_native_merge_unsupported: no shared fork base");
        }
        NativeMergePlan plan{
            source_path.back(),
            target_path.back(),
            *base,
            writer_segment_ids_after_base(source_path, base->segment_id),
            writer_segment_ids_after_base(target_path, base->segment_id),
        };
        if (plan.source.kind != "mutable" || plan.target.kind != "mutable") {
            throw std::runtime_error("chronos_native_merge_unsupported: non-mutable merge segment");
        }
        return plan;
    }

    struct NativeBranchCommitSegments {
        NativeDirectMergeSegment merge_segment;
        NativeDirectMergeSegment continuation_segment;
    };

    NativeBranchCommitSegments merge_commit_segments(const NativeDirectMergeSegment &segment) {
        const std::string branch_point = normalize_decimal(segment.branch_point);
        const std::string live_lo = normalize_decimal(segment.live_lo);
        const std::string live_hi = normalize_decimal(segment.live_hi);
        const cpp_int point = cpp_int_from_decimal(branch_point);
        const cpp_int lo = cpp_int_from_decimal(live_lo);
        const cpp_int hi = cpp_int_from_decimal(live_hi);
        std::vector<std::int64_t> ids = allocate_segment_ids(2);
        const std::int64_t merge_id = ids[0];
        const std::int64_t continuation_id = ids[1];

        // Prefer carving to the right so merge commits advance branch points in
        // the common case.  Merge rows cover the continuation interval, which
        // keeps them visible after later branches from the continuation.
        if (hi - point >= 4) {
            const cpp_int merge_lo = point + 1;
            const cpp_int continuation_lo = point + 2;
            return {
                NativeDirectMergeSegment{
                    merge_id,
                    segment.segment_id,
                    "merge",
                    cpp_int_to_decimal(merge_lo),
                    cpp_int_to_decimal(hi),
                    cpp_int_to_decimal(merge_lo),
                },
                NativeDirectMergeSegment{
                    continuation_id,
                    merge_id,
                    "mutable",
                    cpp_int_to_decimal(continuation_lo),
                    cpp_int_to_decimal(hi),
                    cpp_int_to_decimal(continuation_lo),
                },
            };
        }

        if (point - lo >= 4) {
            const cpp_int continuation_hi = point - 1;
            const cpp_int continuation_point = point - 2;
            return {
                NativeDirectMergeSegment{
                    merge_id,
                    segment.segment_id,
                    "merge",
                    cpp_int_to_decimal(lo),
                    cpp_int_to_decimal(point),
                    cpp_int_to_decimal(continuation_point),
                },
                NativeDirectMergeSegment{
                    continuation_id,
                    merge_id,
                    "mutable",
                    cpp_int_to_decimal(lo),
                    cpp_int_to_decimal(continuation_hi),
                    cpp_int_to_decimal(continuation_point),
                },
            };
        }
        throw std::runtime_error("interval space exhausted");
    }

    NativeBranchCommitReservation reserve_branch_transaction_commit(
        const NativeDirectMergeSegment &original_target_segment,
        const std::string &source,
        const std::string &target,
        bool force_commit_record = false,
        const std::string &participant_stores = "",
        const std::string &metadata_json = "{}"
    ) {
        const bool use_commit_record = force_commit_record || split_store();
        if (use_commit_record) {
            auto active = driver_->query(
                "SELECT merge_segment_id "
                "FROM _chronos_branch_transaction_commits "
                "WHERE target_branch_id = ? "
                "LIMIT 1",
                {target}
            );
            if (!active.empty()) {
                throw std::runtime_error("chronos_native_merge_conflict: target branch commit already in progress");
            }
        }
        NativeBranchCommitSegments segments = merge_commit_segments(original_target_segment);
        insert_branch_transaction_segments(segments, original_target_segment.segment_id, target);
        if (use_commit_record) {
            const std::string participants = participant_stores.empty()
                ? "[\"" + dialect() + "\"]"
                : participant_stores;
            driver_->execute(
                "INSERT INTO _chronos_branch_transaction_commits "
                "(merge_segment_id, continuation_segment_id, target_branch_id, old_target_segment_id, "
                " source_branch_id, participant_stores, created_at, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                {
                    segments.merge_segment.segment_id,
                    segments.continuation_segment.segment_id,
                    target,
                    original_target_segment.segment_id,
                    source,
                    participants,
                    current_timestamp_string(),
                    metadata_json,
                }
            );
        }
        return {segments.merge_segment, segments.continuation_segment, use_commit_record};
    }

    void publish_branch_transaction_commit(
        const NativeBranchCommitReservation &reservation,
        const std::string &target
    ) {
        const std::int64_t changed = driver_->execute_changes(
            "UPDATE _chronos_branch_interval_branches "
            "SET current_segment_id = ? "
            "WHERE branch_id = ? "
            "  AND current_segment_id = ?",
            {
                reservation.continuation_segment.segment_id,
                target,
                reservation.merge_segment.parent_segment_id,
            }
        );
        if (changed != 1) {
            throw std::runtime_error("chronos_native_merge_conflict: target branch head changed");
        }
        if (reservation.has_commit_record) {
            const std::int64_t deleted = driver_->execute_changes(
                "DELETE FROM _chronos_branch_transaction_commits "
                "WHERE merge_segment_id = ? AND target_branch_id = ?",
                {reservation.merge_segment.segment_id, target}
            );
            if (deleted != 1) {
                throw std::runtime_error("chronos_native_merge_internal: missing branch transaction commit record");
            }
        }
    }

    void cleanup_branch_transaction_commit_reservation(
        const NativeBranchCommitReservation &reservation,
        const std::string &target
    ) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
            if (reservation.has_commit_record) {
                driver_->execute(
                    "DELETE FROM _chronos_branch_transaction_commits "
                    "WHERE merge_segment_id = ? AND target_branch_id = ?",
                    {reservation.merge_segment.segment_id, target}
                );
            }
            driver_->execute(
                "DELETE FROM _chronos_branch_interval_segments WHERE segment_id IN (?, ?)",
                {reservation.merge_segment.segment_id, reservation.continuation_segment.segment_id}
            );
            if (started_tx) driver_->execute("COMMIT");
        } catch (...) {
            if (started_tx) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }

    void insert_branch_transaction_segments(
        const NativeBranchCommitSegments &segments,
        std::int64_t parent_segment_id,
        const std::string &target
    ) {
        driver_->execute(
            "INSERT INTO _chronos_branch_interval_segments "
            "(segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi, "
            " branch_point, created_at, metadata) "
            "VALUES "
            "(?, ?, ?, 'merge', ?, ?, ?, ?, '{}'), "
            "(?, ?, ?, 'mutable', ?, ?, ?, ?, '{}')",
            {
                segments.merge_segment.segment_id,
                parent_segment_id,
                target,
                segments.merge_segment.live_lo,
                segments.merge_segment.live_hi,
                segments.merge_segment.branch_point,
                std::string("native"),
                segments.continuation_segment.segment_id,
                segments.merge_segment.segment_id,
                target,
                segments.continuation_segment.live_lo,
                segments.continuation_segment.live_hi,
                segments.continuation_segment.branch_point,
                std::string("native"),
            }
        );
    }

    NativeRowKey key_for_row(const NativeTableMeta &meta, const std::vector<Value> &row) {
        NativeRowKey key;
        key.values.reserve(meta.pk_columns.size());
        for (const auto &pk : meta.pk_columns) {
            key.values.push_back(row[column_index(meta.columns, pk)]);
        }
        return key;
    }

    std::vector<NativeRowKey> candidate_keys_for_writer_segments(
        const NativeTableMeta &meta,
        const std::vector<std::int64_t> &writer_segment_ids
    ) {
        if (writer_segment_ids.empty()) return {};
        std::string sql = "SELECT DISTINCT " + comma_join_quoted(meta.pk_columns) +
            " FROM " + quote_ident(meta.physical_name) + " WHERE writer_segment_id IN (" +
            placeholders(writer_segment_ids.size()) + ")";
        std::vector<Value> params;
        params.reserve(writer_segment_ids.size());
        bool all_non_root = true;
        for (std::int64_t id : writer_segment_ids) {
            if (id <= 1) all_non_root = false;
            params.push_back(id);
        }
        if (dialect() == "postgres" && all_non_root) {
            sql += " AND writer_segment_id > 1";
        }
        auto rows = driver().query(sql, params);
        std::vector<NativeRowKey> keys;
        keys.reserve(rows.size());
        for (auto &row : rows) {
            NativeRowKey key;
            key.values = std::move(row);
            keys.push_back(std::move(key));
        }
        return keys;
    }

    std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> visible_rows_by_key(
        const NativeTableMeta &meta,
        const std::vector<NativeRowKey> &keys,
        const std::string &branch_point
    ) {
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> out;
        auto queries = visible_row_queries(meta, keys, branch_point);
        auto results = driver().query_many(queries);
        for (auto &result : results) {
            for (auto &row : result.rows) {
                out.emplace(key_for_row(meta, row), std::move(row));
            }
        }
        return out;
    }

    std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> visible_snapshot_rows_by_key(
        const NativeTableMeta &meta,
        const std::string &branch_point
    ) {
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> out;
        std::string sql = "SELECT " + comma_join_quoted(meta.columns) +
            " FROM " + quote_ident(meta.physical_name) +
            " WHERE live_lo <= ? AND ? < live_hi AND deleted = FALSE";
        auto rows = driver().query(sql, {branch_point, branch_point});
        out.reserve(rows.size());
        for (auto &row : rows) {
            out.emplace(key_for_row(meta, row), std::move(row));
        }
        return out;
    }

    std::vector<Value> project_row_to_meta(
        const NativeTableMeta &from_meta,
        const std::vector<Value> &row,
        const NativeTableMeta &to_meta
    ) {
        std::unordered_map<std::string, Value> by_column;
        by_column.reserve(from_meta.columns.size());
        for (std::size_t i = 0; i < from_meta.columns.size() && i < row.size(); ++i) {
            by_column.emplace(from_meta.columns[i], row[i]);
        }
        std::vector<Value> projected;
        projected.reserve(to_meta.columns.size());
        for (const auto &column : to_meta.columns) {
            auto found = by_column.find(column);
            projected.push_back(found == by_column.end() ? Value(std::monostate{}) : found->second);
        }
        return projected;
    }

    std::vector<chronos::native::NativeRowDiff> snapshot_diff_rows(
        const std::string &table,
        const NativeTableMeta *left_meta,
        const NativeBranchSegment *left_segment,
        const NativeTableMeta *right_meta,
        const NativeBranchSegment *right_segment
    ) {
        const NativeTableMeta *diff_meta = right_meta ? right_meta : left_meta;
        if (!diff_meta) return {};

        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> left_rows;
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> right_rows;
        if (left_meta && left_segment) {
            auto rows = visible_snapshot_rows_by_key(*left_meta, left_segment->branch_point);
            left_rows.reserve(rows.size());
            for (auto &entry : rows) {
                left_rows.emplace(entry.first, project_row_to_meta(*left_meta, entry.second, *diff_meta));
            }
        }
        if (right_meta && right_segment) {
            auto rows = visible_snapshot_rows_by_key(*right_meta, right_segment->branch_point);
            right_rows.reserve(rows.size());
            for (auto &entry : rows) {
                right_rows.emplace(entry.first, project_row_to_meta(*right_meta, entry.second, *diff_meta));
            }
        }

        std::unordered_map<NativeRowKey, bool, NativeRowKeyHash> key_set;
        key_set.reserve(left_rows.size() + right_rows.size());
        for (const auto &entry : left_rows) key_set.emplace(entry.first, true);
        for (const auto &entry : right_rows) key_set.emplace(entry.first, true);

        std::vector<NativeRowKey> ordered_keys;
        ordered_keys.reserve(key_set.size());
        for (const auto &entry : key_set) ordered_keys.push_back(entry.first);
        std::sort(ordered_keys.begin(), ordered_keys.end(), [&](const NativeRowKey &a, const NativeRowKey &b) {
            return diff_sort_key(a) < diff_sort_key(b);
        });

        std::vector<chronos::native::NativeRowDiff> diffs;
        for (const auto &key : ordered_keys) {
            auto left_found = left_rows.find(key);
            auto right_found = right_rows.find(key);
            const std::vector<Value> *left_row = left_found == left_rows.end() ? nullptr : &left_found->second;
            const std::vector<Value> *right_row = right_found == right_rows.end() ? nullptr : &right_found->second;
            if (auto diff = row_diff_from_left_to_right(*diff_meta, key, left_row, right_row)) {
                diff->table = table;
                diffs.push_back(std::move(*diff));
            }
        }
        return diffs;
    }

    std::tuple<
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash>,
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash>,
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash>
    > visible_rows_by_key_for_merge(
        const NativeTableMeta &meta,
        const std::vector<NativeRowKey> &keys,
        const std::string &base_branch_point,
        const std::string &source_branch_point,
        const std::string &target_branch_point
    ) {
        std::vector<std::pair<std::string, std::vector<Value>>> queries;
        std::vector<int> owners;
        auto append_queries = [&](int owner, const std::string &branch_point) {
            auto branch_queries = visible_row_queries(meta, keys, branch_point);
            for (auto &query : branch_queries) {
                owners.push_back(owner);
                queries.push_back(std::move(query));
            }
        };
        append_queries(0, base_branch_point);
        append_queries(1, source_branch_point);
        append_queries(2, target_branch_point);

        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> base;
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> source;
        std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> target;
        auto results = driver().query_many(queries);
        if (results.size() != owners.size()) {
            throw std::runtime_error("native merge visible-row pipeline returned an unexpected result count");
        }
        for (std::size_t i = 0; i < results.size(); ++i) {
            auto *destination = owners[i] == 0 ? &base : (owners[i] == 1 ? &source : &target);
            for (auto &row : results[i].rows) {
                destination->emplace(key_for_row(meta, row), std::move(row));
            }
        }
        return {std::move(base), std::move(source), std::move(target)};
    }

    std::vector<std::pair<std::string, std::vector<Value>>> visible_row_queries(
        const NativeTableMeta &meta,
        const std::vector<NativeRowKey> &keys,
        const std::string &branch_point
    ) {
        std::vector<std::pair<std::string, std::vector<Value>>> queries;
        if (keys.empty()) return queries;
        queries.reserve((keys.size() + 499) / 500);
        const std::size_t chunk_rows = std::max<std::size_t>(1, 500 / std::max<std::size_t>(1, meta.pk_columns.size()));
        for (std::size_t start = 0; start < keys.size(); start += chunk_rows) {
            const std::size_t count = std::min<std::size_t>(chunk_rows, keys.size() - start);
            std::string where = "(";
            std::vector<Value> params;
            params.reserve(count * meta.pk_columns.size() + 2);
            for (std::size_t row_index = 0; row_index < count; ++row_index) {
                if (row_index) where += " OR ";
                where += "(";
                const auto &key = keys[start + row_index];
                for (std::size_t pk_index = 0; pk_index < meta.pk_columns.size(); ++pk_index) {
                    if (pk_index) where += " AND ";
                    where += quote_ident(meta.pk_columns[pk_index]) + " = ?";
                    params.push_back(key.values[pk_index]);
                }
                where += ")";
            }
            where += ")";
            params.push_back(branch_point);
            params.push_back(branch_point);
            std::string sql = "SELECT " + comma_join_quoted(meta.columns) +
                " FROM " + quote_ident(meta.physical_name) +
                " WHERE " + where + " AND live_lo <= ? AND ? < live_hi AND deleted = FALSE";
            queries.emplace_back(std::move(sql), std::move(params));
        }
        return queries;
    }

    NativeMergeRowState row_state_for_key(
        const std::unordered_map<NativeRowKey, std::vector<Value>, NativeRowKeyHash> &rows,
        const NativeRowKey &key
    ) {
        auto found = rows.find(key);
        if (found == rows.end()) return {};
        return {true, found->second};
    }

    bool row_state_equal(const NativeMergeRowState &left, const NativeMergeRowState &right) {
        if (left.present != right.present) return false;
        if (!left.present) return true;
        return left.row == right.row;
    }

    NativeRows::value_type tombstone_row_for_key(const NativeTableMeta &meta, const NativeRowKey &key) {
        std::vector<Value> row(meta.columns.size(), std::monostate{});
        for (std::size_t i = 0; i < meta.pk_columns.size(); ++i) {
            row[column_index(meta.columns, meta.pk_columns[i])] = key.values[i];
        }
        return row;
    }

    std::vector<std::int64_t> divergent_segment_ids(
        std::int64_t left_segment_id,
        std::int64_t right_segment_id
    ) {
        auto left_path = segment_ancestry(left_segment_id);
        auto right_path = segment_ancestry(right_segment_id);
        std::unordered_set<std::int64_t> left_ids;
        std::unordered_set<std::int64_t> right_ids;
        for (const auto &segment : left_path) left_ids.insert(segment.segment_id);
        for (const auto &segment : right_path) right_ids.insert(segment.segment_id);

        std::vector<std::int64_t> ids;
        for (std::int64_t id : left_ids) {
            if (right_ids.find(id) == right_ids.end()) ids.push_back(id);
        }
        for (std::int64_t id : right_ids) {
            if (left_ids.find(id) == left_ids.end()) ids.push_back(id);
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    std::string diff_sort_key(const NativeRowKey &key) {
        std::string out;
        for (const auto &value : key.values) {
            out.push_back('\x1f');
            out += native_as_string(value);
        }
        return out;
    }

    std::optional<chronos::native::NativeRowDiff> row_diff_from_left_to_right(
        const NativeTableMeta &meta,
        const NativeRowKey &key,
        const std::vector<Value> *left_row,
        const std::vector<Value> *right_row
    ) {
        if (left_row != nullptr && right_row != nullptr && *left_row == *right_row) {
            return std::nullopt;
        }
        chronos::native::NativeRowDiff diff;
        diff.table = meta.logical_name;
        diff.key_columns = meta.pk_columns;
        diff.key_values = key.values;
        diff.columns = meta.columns;
        if (left_row != nullptr) {
            diff.has_before = true;
            diff.before = *left_row;
        }
        if (right_row != nullptr) {
            diff.has_after = true;
            diff.after = *right_row;
        }
        if (left_row == nullptr && right_row != nullptr) {
            diff.change = "added";
        } else if (left_row != nullptr && right_row == nullptr) {
            diff.change = "deleted";
        } else {
            diff.change = "modified";
        }
        return diff;
    }

    std::optional<chronos::native::NativeRowDiff> row_diff_from_target_to_source(
        const NativeTableMeta &meta,
        const NativeRowKey &key,
        const NativeMergeRowState &target,
        const NativeMergeRowState &source,
        bool include_equal = false
    ) {
        const std::vector<Value> *before = target.present ? &target.row : nullptr;
        const std::vector<Value> *after = source.present ? &source.row : nullptr;
        if (include_equal && before != nullptr && after != nullptr && *before == *after) {
            chronos::native::NativeRowDiff diff;
            diff.table = meta.logical_name;
            diff.key_columns = meta.pk_columns;
            diff.key_values = key.values;
            diff.columns = meta.columns;
            diff.has_before = true;
            diff.before = *before;
            diff.has_after = true;
            diff.after = *after;
            diff.change = "modified";
            return diff;
        }
        return row_diff_from_left_to_right(meta, key, before, after);
    }

  public:

// -----------------------------------------------------------------------------
// Native diff, merge preview, and resolved-conflict apply APIs
// Source: interval_branch_store_diff_merge.cpp
// -----------------------------------------------------------------------------
    void lock_branches_for_merge(const std::string &source, const std::string &target) {
        lock_branch_rows_for_merge(source, target);
    }

    std::vector<chronos::native::NativeRowDiff> diff_rows(
        const std::string &left,
        const std::string &right,
        const std::string &table
    ) {
        NativeBranchSegment left_segment = load_segment(left);
        NativeBranchSegment right_segment = load_segment(right);
        std::optional<NativeTableMeta> left_meta;
        std::optional<NativeTableMeta> right_meta;
        try {
            left_meta = load_table_meta(table, left_segment);
        } catch (...) {
        }
        try {
            right_meta = load_table_meta(table, right_segment);
        } catch (...) {
        }
        if (!left_meta && !right_meta) return {};
        if (
            !left_meta ||
            !right_meta ||
            left_meta->physical_name != right_meta->physical_name ||
            left_meta->pk_columns != right_meta->pk_columns ||
            left_meta->columns != right_meta->columns
        ) {
            return snapshot_diff_rows(
                table,
                left_meta ? &*left_meta : nullptr,
                left_meta ? &left_segment : nullptr,
                right_meta ? &*right_meta : nullptr,
                right_meta ? &right_segment : nullptr
            );
        }

        const std::vector<std::int64_t> writer_segments = divergent_segment_ids(
            left_segment.segment_id,
            right_segment.segment_id
        );
        if (writer_segments.empty()) return {};
        std::vector<NativeRowKey> keys = candidate_keys_for_writer_segments(*left_meta, writer_segments);
        if (keys.empty()) return {};

        auto left_rows = visible_rows_by_key(*left_meta, keys, left_segment.branch_point);
        auto right_rows = visible_rows_by_key(*right_meta, keys, right_segment.branch_point);

        std::unordered_map<NativeRowKey, bool, NativeRowKeyHash> key_set;
        key_set.reserve(left_rows.size() + right_rows.size() + keys.size());
        for (const auto &key : keys) key_set.emplace(key, true);
        for (const auto &entry : left_rows) key_set.emplace(entry.first, true);
        for (const auto &entry : right_rows) key_set.emplace(entry.first, true);

        std::vector<NativeRowKey> ordered_keys;
        ordered_keys.reserve(key_set.size());
        for (const auto &entry : key_set) ordered_keys.push_back(entry.first);
        std::sort(ordered_keys.begin(), ordered_keys.end(), [&](const NativeRowKey &a, const NativeRowKey &b) {
            return diff_sort_key(a) < diff_sort_key(b);
        });

        std::vector<chronos::native::NativeRowDiff> diffs;
        for (const auto &key : ordered_keys) {
            auto left_found = left_rows.find(key);
            auto right_found = right_rows.find(key);
            const std::vector<Value> *left_row = left_found == left_rows.end() ? nullptr : &left_found->second;
            const std::vector<Value> *right_row = right_found == right_rows.end() ? nullptr : &right_found->second;
            if (auto diff = row_diff_from_left_to_right(*left_meta, key, left_row, right_row)) {
                diffs.push_back(std::move(*diff));
            }
        }
        return diffs;
    }

    chronos::native::NativeMergePreview merge_preview(
        const std::string &source,
        const std::string &target
    ) {
        return merge_preview_tables(source, target, {});
    }

    chronos::native::NativeMergePreview merge_preview_tables(
        const std::string &source,
        const std::string &target,
        const std::vector<std::string> &tables
    ) {
        const std::unordered_set<std::string> selected_tables(
            tables.begin(), tables.end()
        );
        NativeMergePlan merge_plan = load_merge_plan(source, target, false);
        const NativeDirectMergeSegment &source_segment = merge_plan.source;
        const NativeDirectMergeSegment &target_segment = merge_plan.target;
        const NativeDirectMergeSegment &base_segment = merge_plan.base;

        NativeBranchSegment target_read{
            target_segment.segment_id,
            target_segment.live_lo,
            target_segment.live_hi,
            target_segment.branch_point,
        };

        std::vector<std::int64_t> candidate_writer_segments = merge_plan.source_writer_segments;
        candidate_writer_segments.insert(
            candidate_writer_segments.end(),
            merge_plan.target_writer_segments.begin(),
            merge_plan.target_writer_segments.end()
        );
        std::sort(candidate_writer_segments.begin(), candidate_writer_segments.end());
        candidate_writer_segments.erase(
            std::unique(candidate_writer_segments.begin(), candidate_writer_segments.end()),
            candidate_writer_segments.end()
        );

        chronos::native::NativeMergePreview preview;
        for (const auto &meta : load_table_metas(target_read)) {
            if (!selected_tables.empty() &&
                selected_tables.find(meta.logical_name) == selected_tables.end()) {
                continue;
            }
            if (meta.has_schema_binding) {
                throw std::runtime_error("chronos_native_merge_unsupported: schema-branching merge preview");
            }
            std::vector<NativeRowKey> keys = candidate_keys_for_writer_segments(
                meta,
                candidate_writer_segments
            );
            if (keys.empty()) continue;
            auto [base_rows, source_rows, target_rows] = visible_rows_by_key_for_merge(
                meta,
                keys,
                base_segment.branch_point,
                source_segment.branch_point,
                target_segment.branch_point
            );

            std::unordered_map<NativeRowKey, bool, NativeRowKeyHash> key_set;
            key_set.reserve(keys.size() + base_rows.size() + source_rows.size() + target_rows.size());
            for (const auto &key : keys) key_set.emplace(key, true);
            for (const auto &entry : base_rows) key_set.emplace(entry.first, true);
            for (const auto &entry : source_rows) key_set.emplace(entry.first, true);
            for (const auto &entry : target_rows) key_set.emplace(entry.first, true);

            std::vector<NativeRowKey> ordered_keys;
            ordered_keys.reserve(key_set.size());
            for (const auto &entry : key_set) ordered_keys.push_back(entry.first);
            std::sort(ordered_keys.begin(), ordered_keys.end(), [&](const NativeRowKey &a, const NativeRowKey &b) {
                return diff_sort_key(a) < diff_sort_key(b);
            });

            for (const auto &key : ordered_keys) {
                const NativeMergeRowState base = row_state_for_key(base_rows, key);
                const NativeMergeRowState source_state = row_state_for_key(source_rows, key);
                const NativeMergeRowState target_state = row_state_for_key(target_rows, key);
                if (row_state_equal(source_state, target_state)) {
                    if (!row_state_equal(source_state, base)) {
                        if (auto diff = row_diff_from_target_to_source(
                                meta,
                                key,
                                target_state,
                                source_state,
                                true
                            )) {
                            preview.conflicts.push_back(std::move(*diff));
                        }
                    }
                    continue;
                }
                if (row_state_equal(source_state, base)) {
                    continue;
                }
                if (target_state.present == base.present && row_state_equal(target_state, base)) {
                    if (auto diff = row_diff_from_target_to_source(meta, key, target_state, source_state)) {
                        preview.changes.push_back(std::move(*diff));
                    }
                    continue;
                }
                if (auto diff = row_diff_from_target_to_source(meta, key, target_state, source_state)) {
                    preview.conflicts.push_back(std::move(*diff));
                }
            }
        }
        return preview;
    }

    std::vector<Value> values_for_columns(
        const std::vector<std::string> &columns,
        const std::vector<std::string> &source_columns,
        const std::vector<Value> &source_values
    ) {
        std::unordered_map<std::string, Value> by_column;
        for (std::size_t i = 0; i < source_columns.size() && i < source_values.size(); ++i) {
            by_column.emplace(source_columns[i], source_values[i]);
        }
        std::vector<Value> out;
        out.reserve(columns.size());
        for (const auto &column : columns) {
            auto found = by_column.find(column);
            out.push_back(found == by_column.end() ? Value(std::monostate{}) : found->second);
        }
        return out;
    }

    std::vector<Value> tombstone_values_for_key(
        const NativeTableMeta &meta,
        const chronos::native::NativeMergeChange &change
    ) {
        std::unordered_map<std::string, Value> by_key;
        for (std::size_t i = 0; i < change.key_columns.size() && i < change.key_values.size(); ++i) {
            by_key.emplace(change.key_columns[i], change.key_values[i]);
        }
        std::vector<Value> row(meta.columns.size(), std::monostate{});
        for (const auto &pk : meta.pk_columns) {
            auto found = by_key.find(pk);
            if (found == by_key.end()) {
                throw std::runtime_error("merge change key is missing primary key column: " + pk);
            }
            row[column_index(meta.columns, pk)] = found->second;
        }
        return row;
    }

    std::int64_t apply_merge_changes(
        const std::string &source,
        const std::string &target,
        const std::vector<chronos::native::NativeMergeChange> &changes,
        std::int64_t expected_source_segment_id = 0,
        std::int64_t expected_target_segment_id = 0
    ) {
        if (changes.empty()) return 0;
        NativeSessionBarrierGuard guard = begin_session_barrier(
            {source, target}, "merge"
        );
        try {
            if (guard.installed) suspend_session_barrier_transaction(guard);
            const auto applied = apply_merge_changes_unfenced(
                source,
                target,
                changes,
                expected_source_segment_id,
                expected_target_segment_id
            );
            finish_session_barrier(guard);
            return applied;
        } catch (...) {
            abort_session_barrier(guard);
            throw;
        }
    }

    std::int64_t apply_merge_changes_unfenced(
        const std::string &source,
        const std::string &target,
        const std::vector<chronos::native::NativeMergeChange> &changes,
        std::int64_t expected_source_segment_id,
        std::int64_t expected_target_segment_id
    ) {
        const bool started_tx = !driver_->in_transaction();
        if (started_tx) driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
        bool metadata_tx_open = started_tx;
        try {
            NativeMergePlan merge_plan = load_merge_plan(source, target, true);
            if (
                (expected_source_segment_id != 0 &&
                 merge_plan.source.segment_id != expected_source_segment_id) ||
                (expected_target_segment_id != 0 &&
                 merge_plan.target.segment_id != expected_target_segment_id)
            ) {
                throw std::runtime_error("chronos_native_merge_preview_stale");
            }
            const NativeDirectMergeSegment original_target_segment = merge_plan.target;
            NativeBranchCommitReservation reservation = reserve_branch_transaction_commit(
                original_target_segment,
                source,
                target
            );
            NativeBranchSegment merge_segment{
                reservation.merge_segment.segment_id,
                reservation.merge_segment.live_lo,
                reservation.merge_segment.live_hi,
                reservation.merge_segment.branch_point,
            };

            std::vector<std::string> table_order;
            std::unordered_map<std::string, std::vector<chronos::native::NativeMergeChange>> by_table;
            for (const auto &change : changes) {
                if (by_table.find(change.table) == by_table.end()) {
                    table_order.push_back(change.table);
                }
                by_table[change.table].push_back(change);
            }

            std::int64_t applied = 0;
            const bool committed_split_reservation = split_store() && started_tx;
            if (committed_split_reservation) {
                driver_->execute("COMMIT");
                metadata_tx_open = false;
            }
            const bool started_data_tx = split_store() && !driver().in_transaction();
            if (started_data_tx) {
                driver().execute(dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
            }
            bool data_committed = !started_data_tx;
            try {
                for (const auto &table : table_order) {
                    NativeTableMeta meta = load_table_meta(table, merge_segment);
                    if (meta.has_schema_binding) {
                        throw std::runtime_error("chronos_native_merge_unsupported: schema-branching resolved merge");
                    }
                    NativeRows upserts;
                    NativeRows deletes;
                    for (const auto &change : by_table[table]) {
                        if (change.change == "deleted") {
                            deletes.push_back(tombstone_values_for_key(meta, change));
                        } else {
                            if (!change.has_after) {
                                throw std::runtime_error("merge change is missing source row");
                            }
                            upserts.push_back(values_for_columns(
                                meta.columns,
                                change.after_columns,
                                change.after_values
                            ));
                        }
                    }
                    if (!deletes.empty()) {
                        driver().interval_upsert(
                            meta.physical_name,
                            meta.columns,
                            meta.pk_columns,
                            deletes,
                            reservation.merge_segment.live_lo,
                            reservation.merge_segment.live_hi,
                            reservation.merge_segment.segment_id,
                            true,
                            false,
                            IntervalWriteMode::Upsert,
                            reservation.merge_segment.branch_point
                        );
                        applied += static_cast<std::int64_t>(deletes.size());
                    }
                    if (!upserts.empty()) {
                        driver().interval_upsert(
                            meta.physical_name,
                            meta.columns,
                            meta.pk_columns,
                            upserts,
                            reservation.merge_segment.live_lo,
                            reservation.merge_segment.live_hi,
                            reservation.merge_segment.segment_id,
                            false,
                            false,
                            IntervalWriteMode::Upsert,
                            reservation.merge_segment.branch_point
                        );
                        applied += static_cast<std::int64_t>(upserts.size());
                    }
                }
                if (started_data_tx) driver().execute("COMMIT");
                data_committed = true;
            } catch (...) {
                if (started_data_tx) {
                    try {
                        driver().execute("ROLLBACK");
                    } catch (...) {
                    }
                }
                if (committed_split_reservation && !data_committed) {
                    cleanup_branch_transaction_commit_reservation(reservation, target);
                }
                throw;
            }

            if (committed_split_reservation) {
                driver_->execute(metadata_dialect() == "sqlite" ? "BEGIN IMMEDIATE" : "BEGIN");
                metadata_tx_open = true;
            }
            publish_branch_transaction_commit(reservation, target);
            if (started_tx && metadata_tx_open) driver_->execute("COMMIT");
            metadata_tx_open = false;
            return applied;
        } catch (...) {
            if (started_tx && metadata_tx_open) {
                try {
                    driver_->execute("ROLLBACK");
                } catch (...) {
                }
            }
            throw;
        }
    }
