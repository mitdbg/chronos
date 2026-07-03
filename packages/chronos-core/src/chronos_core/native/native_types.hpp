#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace chronos::native {

using IntervalBlob = std::vector<unsigned char>;

struct IntervalDecimal {
    std::string text;

    bool operator==(const IntervalDecimal &other) const {
        return text == other.text;
    }
};

struct IntervalDate {
    int year;
    int month;
    int day;

    bool operator==(const IntervalDate &other) const {
        return year == other.year && month == other.month && day == other.day;
    }
};

using IntervalValue = std::variant<
    std::monostate,
    std::int64_t,
    double,
    std::string,
    IntervalBlob,
    IntervalDecimal,
    IntervalDate
>;
using IntervalRows = std::vector<std::vector<IntervalValue>>;

struct IntervalQueryResult {
    std::vector<std::string> columns;
    IntervalRows rows;
};

struct IntervalBulkUpsertStats {
    std::int64_t selected = 0;
    std::int64_t deleted_rows = 0;
    std::int64_t inserted = 0;
};

struct NativeSqlProfile {
    std::int64_t round_trips = 0;
    std::int64_t queries = 0;
    std::int64_t executes = 0;
    std::int64_t prepares = 0;
    std::int64_t direct_exec_params = 0;
    std::int64_t prepared_execs = 0;
    std::int64_t simple_execs = 0;
    std::int64_t pipeline_round_trips = 0;
    std::int64_t pipeline_statements = 0;
    std::int64_t bulk_select_round_trips = 0;
    std::int64_t bulk_write_round_trips = 0;
    std::int64_t bulk_tx_round_trips = 0;
};

struct NativeSqlTraceEntry {
    std::string sql;
    std::string call;
    std::string bucket;
    double elapsed_ms = 0.0;
    std::int64_t rows = -1;
    std::int64_t param_count = 0;
};

struct NativeBranchInfo {
    std::string branch_id;
    std::string current_ref;
    std::string created_at;
    std::string metadata_json;
};

struct NativeCheckpointInfo {
    std::string checkpoint_id;
    std::string branch_id;
    std::string ref;
    std::string created_at;
    std::string metadata_json;
};

struct NativeSegmentInfo {
    std::int64_t segment_id = 0;
    std::string live_lo;
    std::string live_hi;
    std::string branch_point;
};

struct NativeTableInfo {
    std::string logical_name;
    std::string physical_name;
    std::vector<std::string> primary_key;
    std::vector<std::string> columns;
    std::vector<std::string> column_defs;
};

struct NativePreparedRefInfo {
    NativeSegmentInfo segment;
    std::vector<NativeTableInfo> tables;
    std::vector<std::string> known_schema_tables;
};

struct NativeRowDiff {
    std::string table;
    std::vector<std::string> key_columns;
    std::vector<IntervalValue> key_values;
    std::string change;
    std::vector<std::string> columns;
    std::vector<IntervalValue> before;
    std::vector<IntervalValue> after;
    bool has_before = false;
    bool has_after = false;
};

struct NativeMergePreview {
    std::vector<NativeRowDiff> changes;
    std::vector<NativeRowDiff> conflicts;
};

struct NativeMergeChange {
    std::string table;
    std::string change;
    std::vector<std::string> key_columns;
    std::vector<IntervalValue> key_values;
    std::vector<std::string> after_columns;
    std::vector<IntervalValue> after_values;
    bool has_after = false;
};

} // namespace chronos::native
