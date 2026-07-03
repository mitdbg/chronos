namespace chronos::native::detail {

using boost::multiprecision::cpp_int;
using Blob = chronos::native::IntervalBlob;
using DecimalValue = chronos::native::IntervalDecimal;
using DateValue = chronos::native::IntervalDate;
using Value = chronos::native::IntervalValue;
using NativeRows = chronos::native::IntervalRows;

class PgProtobufParseResult;

struct BoundSql {
    std::string sql;
    std::vector<Value> positional_params;
};

struct SqlBindPlan {
    std::string sql;
    std::vector<std::string> param_names;
};

struct BulkUpsertStats {
    std::int64_t selected = 0;
    std::int64_t deleted_rows = 0;
    std::int64_t inserted = 0;
};

enum class IntervalWriteMode {
    Upsert,
    Insert,
    InsertIgnoreConflicts,
};

struct BulkUpsertResult {
    BulkUpsertStats stats;
    std::int64_t logical_rows_written = 0;
};

struct QueryResult {
    std::vector<std::string> columns;
    NativeRows rows;
};

struct NativeInsertPlan {
    std::string table;
    std::vector<std::string> columns;
    std::vector<std::vector<PgQuery__Node *>> value_tuples;
    bool ignore_conflicts = false;
};

struct NativeUpdatePlan {
    std::string table;
    std::vector<std::pair<std::string, PgQuery__Node *>> assignments;
    PgQuery__Node *where = nullptr;
};

struct NativeDeletePlan {
    std::string table;
    PgQuery__Node *where = nullptr;
};

enum class NativeStatementKind {
    Insert,
    Update,
    Delete,
};

struct CachedNativeStatement {
    std::shared_ptr<PgProtobufParseResult> parsed;
    NativeStatementKind kind = NativeStatementKind::Insert;
    NativeInsertPlan insert;
    NativeUpdatePlan update;
    NativeDeletePlan del;
};

struct NativeRowKey {
    std::vector<Value> values;

    bool operator==(const NativeRowKey &other) const {
        return values == other.values;
    }
};

typedef struct {
    PyObject_HEAD
    sqlite3 *db;
} PySqliteConnectionPrefix;

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

std::string quote_table_name(const std::string &table) {
    std::string out;
    std::string part;
    for (char ch : table) {
        if (ch == '.') {
            if (!out.empty()) out += ".";
            out += quote_ident(part);
            part.clear();
        } else {
            part.push_back(ch);
        }
    }
    if (!out.empty()) out += ".";
    out += quote_ident(part);
    return out;
}

std::pair<std::string, std::string> split_postgres_table_name(const std::string &table) {
    const std::size_t dot = table.find('.');
    if (dot == std::string::npos) {
        return {"public", table};
    }
    return {table.substr(0, dot), table.substr(dot + 1)};
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

std::string date_value_to_iso(const DateValue &value) {
    std::ostringstream out;
    out << std::setfill('0')
        << std::setw(4) << value.year << "-"
        << std::setw(2) << value.month << "-"
        << std::setw(2) << value.day;
    return out.str();
}

std::string value_text(const Value &value) {
    if (auto ptr = std::get_if<std::string>(&value)) return *ptr;
    if (auto ptr = std::get_if<DecimalValue>(&value)) return ptr->text;
    if (auto ptr = std::get_if<DateValue>(&value)) return date_value_to_iso(*ptr);
    if (auto ptr = std::get_if<std::int64_t>(&value)) return std::to_string(*ptr);
    if (auto ptr = std::get_if<double>(&value)) {
        std::ostringstream out;
        out.precision(17);
        out << *ptr;
        return out.str();
    }
    return {};
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
    if (py::isinstance<py::dict>(obj) || py::isinstance<py::list>(obj)) {
        py::object dumped = py::module_::import("json").attr("dumps")(obj);
        return dumped.cast<std::string>();
    }
    if (py::isinstance<py::str>(obj)) {
        return obj.cast<std::string>();
    }
    return py::str(obj).cast<std::string>();
}

std::size_t hash_combine(std::size_t seed, std::size_t value) {
    return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

std::size_t value_hash(const Value &value) {
    if (std::holds_alternative<std::monostate>(value)) {
        return 0x6eed0e9da4d94a4fULL;
    }
    if (auto ptr = std::get_if<std::int64_t>(&value)) {
        return hash_combine(0x01ULL, std::hash<std::int64_t>{}(*ptr));
    }
    if (auto ptr = std::get_if<double>(&value)) {
        return hash_combine(0x02ULL, std::hash<double>{}(*ptr));
    }
    if (auto ptr = std::get_if<std::string>(&value)) {
        return hash_combine(0x03ULL, std::hash<std::string>{}(*ptr));
    }
    if (auto ptr = std::get_if<Blob>(&value)) {
        std::size_t hash = 1469598103934665603ULL;
        for (unsigned char byte : *ptr) {
            hash ^= static_cast<std::size_t>(byte);
            hash *= 1099511628211ULL;
        }
        return hash_combine(0x04ULL, hash);
    }
    if (auto ptr = std::get_if<DecimalValue>(&value)) {
        return hash_combine(0x05ULL, std::hash<std::string>{}(ptr->text));
    }
    if (auto ptr = std::get_if<DateValue>(&value)) {
        std::size_t hash = std::hash<int>{}(ptr->year);
        hash = hash_combine(hash, std::hash<int>{}(ptr->month));
        hash = hash_combine(hash, std::hash<int>{}(ptr->day));
        return hash_combine(0x06ULL, hash);
    }
    return 0;
}

struct NativeRowKeyHash {
    std::size_t operator()(const NativeRowKey &key) const {
        std::size_t seed = 0x54d6c1b4a51f2d7bULL;
        for (const auto &value : key.values) {
            seed = hash_combine(seed, value_hash(value));
        }
        return seed;
    }
};

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
        if (!ptr->empty() && ((*ptr)[0] == '{' || (*ptr)[0] == '[')) {
            try {
                return py::module_::import("json").attr("loads")(*ptr);
            } catch (...) {
            }
        }
        return py::str(*ptr);
    }
    if (auto ptr = std::get_if<DecimalValue>(&value)) {
        return py::module_::import("decimal").attr("Decimal")(ptr->text);
    }
    if (auto ptr = std::get_if<DateValue>(&value)) {
        return py::module_::import("datetime").attr("date")(
            ptr->year,
            ptr->month,
            ptr->day
        );
    }
    const auto &blob = std::get<Blob>(value);
    return py::bytes(reinterpret_cast<const char *>(blob.data()), blob.size());
}

NativeRows rows_to_native_values(const py::list &rows, const std::vector<std::string> &columns) {
    NativeRows native_rows;
    native_rows.reserve(static_cast<std::size_t>(py::len(rows)));
    for (const auto item : rows) {
        py::dict row = py::reinterpret_borrow<py::dict>(item);
        std::vector<Value> values;
        values.reserve(columns.size());
        for (const auto &column : columns) {
            py::object key = py::str(column);
            if (row.contains(key)) {
                values.push_back(py_to_value(row[key]));
            } else {
                values.push_back(std::monostate{});
            }
        }
        native_rows.push_back(std::move(values));
    }
    return native_rows;
}

std::vector<Value> list_to_values(const py::list &items) {
    std::vector<Value> values;
    values.reserve(static_cast<std::size_t>(py::len(items)));
    for (const auto item : items) {
        values.push_back(py_to_value(item));
    }
    return values;
}

std::vector<Value> params_to_values(const py::object &params) {
    if (params.is_none()) {
        return {};
    }
    if (py::isinstance<py::dict>(params)) {
        throw std::invalid_argument("native branch APIs accept positional parameters");
    }
    std::vector<Value> values;
    for (const auto item : params) {
        values.push_back(py_to_value(item));
    }
    return values;
}

bool is_identifier_start(char ch) {
    return std::isalpha(static_cast<unsigned char>(ch)) || ch == '_';
}

bool is_identifier_part(char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

std::string pg_parse_placeholder_sql(const std::string &sql);

class PgParseResult {
  public:
    explicit PgParseResult(const std::string &sql) : result_(pg_query_parse(sql.c_str())) {
        if (result_.error) {
            std::string message = result_.error->message ? result_.error->message : "PostgreSQL parser error";
            throw std::runtime_error(message);
        }
        if (!result_.parse_tree) {
            throw std::runtime_error("PostgreSQL parser returned no parse tree");
        }
    }
    ~PgParseResult() { pg_query_free_parse_result(result_); }
    PgParseResult(const PgParseResult &) = delete;
    PgParseResult &operator=(const PgParseResult &) = delete;

    std::string_view tree() const { return result_.parse_tree; }

  private:
    PgQueryParseResult result_{};
};

class PgProtobufParseResult {
  public:
    explicit PgProtobufParseResult(const std::string &sql)
        : result_(pg_query_parse_protobuf(pg_parse_placeholder_sql(sql).c_str())) {
        if (result_.error) {
            std::string message = result_.error->message ? result_.error->message : "PostgreSQL parser error";
            throw std::runtime_error(message);
        }
        root_ = pg_query__parse_result__unpack(
            nullptr,
            result_.parse_tree.len,
            reinterpret_cast<const uint8_t *>(result_.parse_tree.data)
        );
        if (!root_) {
            throw std::runtime_error("could not unpack libpg_query parse tree");
        }
    }
    ~PgProtobufParseResult() {
        if (root_) pg_query__parse_result__free_unpacked(root_, nullptr);
        pg_query_free_protobuf_parse_result(result_);
    }
    PgProtobufParseResult(const PgProtobufParseResult &) = delete;
    PgProtobufParseResult &operator=(const PgProtobufParseResult &) = delete;

    PgQuery__Node *single_statement() const {
        if (!root_ || root_->n_stmts != 1 || !root_->stmts[0] || !root_->stmts[0]->stmt) {
            throw std::runtime_error("native branch SQL requires exactly one statement");
        }
        return root_->stmts[0]->stmt;
    }

  private:
    PgQueryProtobufParseResult result_{};
    PgQuery__ParseResult *root_ = nullptr;
};

std::string pg_parse_placeholder_sql(const std::string &sql) {
    std::string out;
    out.reserve(sql.size());
    bool single_quote = false;
    bool double_quote = false;
    std::size_t param_index = 1;
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
            out += "$" + std::to_string(param_index++);
            continue;
        }
        out.push_back(ch);
    }
    return out;
}

std::string pg_statement_kind(const std::string &sql) {
    PgParseResult parsed(pg_parse_placeholder_sql(sql));
    const std::string_view tree = parsed.tree();
    const std::pair<const char *, const char *> kinds[] = {
        {"InsertStmt", "insert"},
        {"UpdateStmt", "update"},
        {"DeleteStmt", "delete"},
        {"SelectStmt", "select"},
    };
    for (const auto &[needle, kind] : kinds) {
        if (tree.find(needle) != std::string_view::npos) {
            return kind;
        }
    }
    throw std::runtime_error("unsupported SQL statement for native branch session");
}

bool starts_with_sql_keyword(const std::string &sql, const std::string &keyword) {
    std::size_t i = 0;
    while (i < sql.size() && std::isspace(static_cast<unsigned char>(sql[i]))) {
        ++i;
    }
    if (i + keyword.size() > sql.size()) return false;
    for (std::size_t j = 0; j < keyword.size(); ++j) {
        const char actual = static_cast<char>(std::toupper(static_cast<unsigned char>(sql[i + j])));
        if (actual != keyword[j]) return false;
    }
    const std::size_t end = i + keyword.size();
    return end == sql.size() || !is_identifier_part(sql[end]);
}

bool is_select_query_sql(const std::string &sql) {
    // BranchSession.query is a read API.  DML/DDL still goes through the
    // libpg_query planner in execute(); this cheap guard avoids a parser call
    // on every cached SELECT, which is visible in point-read microbenchmarks.
    return starts_with_sql_keyword(sql, "SELECT") || starts_with_sql_keyword(sql, "WITH");
}

SqlBindPlan plan_named_sql(const std::string &sql) {
    SqlBindPlan plan;
    plan.sql.reserve(sql.size());
    bool single_quote = false;
    bool double_quote = false;
    for (std::size_t i = 0; i < sql.size(); ++i) {
        const char ch = sql[i];
        if (ch == '\'' && !double_quote) {
            single_quote = !single_quote;
            plan.sql.push_back(ch);
            continue;
        }
        if (ch == '"' && !single_quote) {
            double_quote = !double_quote;
            plan.sql.push_back(ch);
            continue;
        }
        if (!single_quote && !double_quote && ch == ':' && i + 1 < sql.size() && is_identifier_start(sql[i + 1])) {
            std::size_t j = i + 2;
            while (j < sql.size() && is_identifier_part(sql[j])) {
                ++j;
            }
            std::string name = sql.substr(i + 1, j - i - 1);
            plan.sql.push_back('?');
            plan.param_names.push_back(std::move(name));
            i = j - 1;
            continue;
        }
        plan.sql.push_back(ch);
    }
    return plan;
}

BoundSql bind_named_sql_plan(const SqlBindPlan &plan, const py::dict &params) {
    BoundSql bound;
    bound.sql = plan.sql;
    bound.positional_params.reserve(plan.param_names.size());
    for (const auto &name : plan.param_names) {
        py::str key(name);
        if (!params.contains(key)) {
            throw std::invalid_argument("missing SQL parameter: " + name);
        }
        bound.positional_params.push_back(py_to_value(params[key]));
    }
    return bound;
}

BoundSql bind_sql_params(const std::string &sql, const py::object &params) {
    if (py::isinstance<py::dict>(params)) {
        return bind_named_sql_plan(plan_named_sql(sql), params.cast<py::dict>());
    }
    BoundSql bound;
    bound.sql = sql;
    if (params.is_none()) {
        return bound;
    }
    py::sequence seq = params.cast<py::sequence>();
    bound.positional_params.reserve(static_cast<std::size_t>(py::len(seq)));
    for (const auto item : seq) {
        bound.positional_params.push_back(py_to_value(item));
    }
    return bound;
}

std::optional<std::string> lookup_replacement(const py::dict &replacements, const std::string &name) {
    py::str key(name);
    if (replacements.contains(key)) {
        return replacements[key].cast<std::string>();
    }
    return std::nullopt;
}

std::optional<std::string> lookup_replacement(
    const std::unordered_map<std::string, std::string> &replacements,
    const std::string &name
) {
    auto found = replacements.find(name);
    if (found != replacements.end()) {
        return found->second;
    }
    return std::nullopt;
}

template <typename Replacements>
std::string rewrite_visible_tables(const std::string &sql, const Replacements &replacements) {
    std::string out;
    out.reserve(sql.size() * 2);
    bool single_quote = false;
    bool double_quote = false;
    bool expect_table = false;
    bool in_from_list = false;
    int paren_depth = 0;
    int from_list_depth = 0;
    auto is_from_clause_boundary = [](const std::string &upper) {
        return upper == "WHERE" || upper == "GROUP" || upper == "ORDER" ||
            upper == "HAVING" || upper == "LIMIT" || upper == "OFFSET" ||
            upper == "FETCH" || upper == "UNION" || upper == "EXCEPT" ||
            upper == "INTERSECT" || upper == "QUALIFY" || upper == "WINDOW" ||
            upper == "ON" || upper == "USING";
    };
    for (std::size_t i = 0; i < sql.size();) {
        const char ch = sql[i];
        if (ch == '\'' && !double_quote) {
            single_quote = !single_quote;
            out.push_back(ch);
            ++i;
            continue;
        }
        if (ch == '"' && !single_quote) {
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
                if (auto replacement = lookup_replacement(replacements, ident)) {
                    std::string ltrimmed = *replacement;
                    ltrimmed.erase(0, ltrimmed.find_first_not_of(" \n\r\t"));
                    std::string head = ltrimmed.substr(0, std::min<std::size_t>(6, ltrimmed.size()));
                    std::transform(head.begin(), head.end(), head.begin(), [](unsigned char c) {
                        return static_cast<char>(std::toupper(c));
                    });
                    if (head == "SELECT" || head.rfind("WITH", 0) == 0) {
                        out += "(" + *replacement + ")";
                    } else {
                        out += quote_ident(*replacement);
                    }
                } else {
                    out.append(sql, i, j - i + 1);
                }
                expect_table = false;
                i = j + 1;
            } else {
                out.append(sql, i, j < sql.size() ? j - i + 1 : j - i);
                i = j < sql.size() ? j + 1 : j;
            }
            continue;
        }
        if (single_quote || double_quote || !is_identifier_start(ch)) {
            if (!single_quote && !double_quote) {
                if (ch == '(') {
                    ++paren_depth;
                } else if (ch == ')') {
                    if (paren_depth > 0) --paren_depth;
                    if (in_from_list && paren_depth < from_list_depth) {
                        in_from_list = false;
                        expect_table = false;
                    }
                } else if (ch == ',' && in_from_list && paren_depth == from_list_depth) {
                    expect_table = true;
                }
            }
            out.push_back(ch);
            ++i;
            continue;
        }

        std::size_t j = i + 1;
        while (j < sql.size() && is_identifier_part(sql[j])) {
            ++j;
        }
        std::string ident = sql.substr(i, j - i);
        std::string upper = ident;
        std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) {
            return static_cast<char>(std::toupper(c));
        });

        if (expect_table) {
            std::string lookup_name = ident;
            std::size_t consumed = j;
            if (j < sql.size() && sql[j] == '.' && j + 1 < sql.size() && is_identifier_start(sql[j + 1])) {
                std::size_t k = j + 2;
                while (k < sql.size() && is_identifier_part(sql[k])) {
                    ++k;
                }
                lookup_name = ident + "." + sql.substr(j + 1, k - j - 1);
                consumed = k;
            }
            if (auto replacement = lookup_replacement(replacements, lookup_name)) {
                const std::string trimmed = replacement->substr(0, replacement->find_last_not_of(" \n\r\t") + 1);
                std::string ltrimmed = trimmed;
                ltrimmed.erase(0, ltrimmed.find_first_not_of(" \n\r\t"));
                std::string head = ltrimmed.substr(0, std::min<std::size_t>(6, ltrimmed.size()));
                std::transform(head.begin(), head.end(), head.begin(), [](unsigned char c) {
                    return static_cast<char>(std::toupper(c));
                });
                if (head == "SELECT" || head.rfind("WITH", 0) == 0) {
                    out += "(" + *replacement + ")";
                } else {
                    out += quote_ident(*replacement);
                }
            } else {
                out += lookup_name;
            }
            expect_table = false;
            i = consumed;
            continue;
        }

        out += ident;
        if (is_from_clause_boundary(upper) && paren_depth == from_list_depth) {
            in_from_list = false;
            expect_table = false;
        }
        if (upper == "FROM" || upper == "JOIN") {
            expect_table = true;
            in_from_list = true;
            from_list_depth = paren_depth;
        } else if (upper == "INTO" || upper == "UPDATE") {
            expect_table = true;
            in_from_list = false;
        }
        i = j;
    }
    return out;
}

std::size_t column_index(const std::vector<std::string> &columns, const std::string &column) {
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (columns[i] == column) {
            return i;
        }
    }
    throw std::invalid_argument("primary key column is not present in columns: " + column);
}

std::vector<std::size_t> pk_column_indices(
    const std::vector<std::string> &columns,
    const std::vector<std::string> &pk_columns
) {
    std::vector<std::size_t> indices;
    indices.reserve(pk_columns.size());
    for (const auto &column : pk_columns) {
        indices.push_back(column_index(columns, column));
    }
    return indices;
}

NativeRows dedupe_rows_by_pk(const NativeRows &rows, const std::vector<std::size_t> &pk_indices) {
    NativeRows deduped;
    deduped.reserve(rows.size());
    std::unordered_map<NativeRowKey, std::size_t, NativeRowKeyHash> positions;
    positions.reserve(rows.size() * 2 + 1);
    for (const auto &row : rows) {
        NativeRowKey key;
        key.values.reserve(pk_indices.size());
        for (std::size_t index : pk_indices) {
            key.values.push_back(row[index]);
        }
        auto found = positions.find(key);
        if (found == positions.end()) {
            positions.emplace(std::move(key), deduped.size());
            deduped.push_back(row);
        } else {
            deduped[found->second] = row;
        }
    }
    return deduped;
}
