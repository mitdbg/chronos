#include "chronos/gateway/postgres_server.hpp"

#include <boost/asio.hpp>
#include <libpq-fe.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using chronos::gateway::CreateBranchRequest;
using chronos::gateway::GatewayRuntime;
using chronos::gateway::ListenAddress;
using chronos::gateway::PostgresServer;
using chronos::gateway::StoreConfig;
using chronos::gateway::WorkspaceConfig;
using chronos::native::NativeBranchStore;

namespace {

void check(bool condition, const std::string &message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-postgres.XXXXXX";
        pattern.push_back('\0');
        path = mkdtemp(pattern.data());
        if (path.empty()) throw std::runtime_error("mkdtemp failed");
    }
    ~TempDirectory() { std::filesystem::remove_all(path); }
    std::string path;
};

struct ConnectionDeleter {
    void operator()(PGconn *connection) const { if (connection) PQfinish(connection); }
};
using Connection = std::unique_ptr<PGconn, ConnectionDeleter>;

struct ResultDeleter {
    void operator()(PGresult *result) const { if (result) PQclear(result); }
};
using Result = std::unique_ptr<PGresult, ResultDeleter>;

std::uint16_t unused_port() {
    boost::asio::io_context io;
    boost::asio::ip::tcp::acceptor acceptor(
        io, {boost::asio::ip::make_address("127.0.0.1"), 0});
    return acceptor.local_endpoint().port();
}

Connection connect(
    std::uint16_t port,
    const std::string &grant,
    const std::string &secret,
    const std::string &database) {
    const std::string connection =
        "host=127.0.0.1 port=" + std::to_string(port) +
        " user=" + grant + " password=" + secret +
        " dbname=" + database + " sslmode=prefer connect_timeout=2";
    for (int attempt = 0; attempt < 40; ++attempt) {
        Connection result(PQconnectdb(connection.c_str()));
        if (PQstatus(result.get()) == CONNECTION_OK) return result;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    Connection failed(PQconnectdb(connection.c_str()));
    return failed;
}

Result execute(PGconn *connection, const std::string &sql) {
    return Result(PQexec(connection, sql.c_str()));
}

Result expect_result(
    PGconn *connection,
    const std::string &sql,
    ExecStatusType expected,
    const std::string &message) {
    auto result = execute(connection, sql);
    check(PQresultStatus(result.get()) == expected,
          message + ": " + PQresultErrorMessage(result.get()));
    return result;
}

void expect_cell(
    PGconn *connection,
    const std::string &sql,
    int row,
    int column,
    const std::string &expected,
    const std::string &message) {
    auto result = expect_result(connection, sql, PGRES_TUPLES_OK, message);
    check(PQntuples(result.get()) > row && PQnfields(result.get()) > column,
          message + ": result shape differs");
    check(!PQgetisnull(result.get(), row, column) &&
              std::string(PQgetvalue(result.get(), row, column)) == expected,
          message + ": result value differs");
}

void expect_command(
    PGconn *connection,
    const std::string &sql,
    const std::string &expected_tag,
    const std::string &message) {
    auto result = expect_result(connection, sql, PGRES_COMMAND_OK, message);
    check(std::string(PQcmdStatus(result.get())) == expected_tag,
          message + ": command tag differs: " + PQcmdStatus(result.get()));
}

} // namespace

int main() {
    TempDirectory temp;
    const char *postgres_data = std::getenv("CHRONOS_GATEWAY_TEST_POSTGRES_DATA_URL");
    const char *postgres_metadata = std::getenv("CHRONOS_GATEWAY_TEST_POSTGRES_METADATA_URL");
    check((postgres_data == nullptr) == (postgres_metadata == nullptr),
          "both PostgreSQL test URLs must be set together");
    const bool use_postgres = postgres_data != nullptr;
    const std::string metadata = use_postgres
        ? postgres_metadata
        : "sqlite://" + temp.path + "/metadata.sqlite";
    const std::string data = use_postgres
        ? postgres_data
        : "sqlite://" + temp.path + "/orders.sqlite";
    if (!use_postgres) {
        std::ofstream(temp.path + "/metadata.sqlite").close();
        std::ofstream(temp.path + "/orders.sqlite").close();
    }

    NativeBranchStore setup(data, metadata);
    setup.ensure();
    setup.execute_sql(
        "CREATE TABLE orders ("
        "id INTEGER PRIMARY KEY, customer_id INTEGER, state TEXT, amount INTEGER, note TEXT)");
    setup.execute_sql("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
    setup.execute_sql("INSERT INTO customers VALUES (?, ?)",
                      {std::int64_t(10), std::string("Ada")});
    setup.execute_sql("INSERT INTO customers VALUES (?, ?)",
                      {std::int64_t(20), std::string("Linus")});
    setup.execute_sql("INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                      {std::int64_t(1), std::int64_t(10), std::string("open"),
                       std::int64_t(25), std::monostate{}});
    setup.execute_sql("INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                      {std::int64_t(2), std::int64_t(10), std::string("open"),
                       std::int64_t(75), std::string("fragile")});
    setup.execute_sql("INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                      {std::int64_t(3), std::int64_t(20), std::string("closed"),
                       std::int64_t(40), std::string("delivered")});
    setup.commit();
    setup.register_table("orders", {"id"});
    setup.register_table("customers", {"id"});

    GatewayRuntime runtime;
    WorkspaceConfig workspace;
    workspace.postgres_stores.emplace("orders", StoreConfig{data, metadata});
    runtime.add_workspace("training", workspace);
    CreateBranchRequest request;
    request.workspace = "training";
    request.from_branch = "main";
    request.branch_id = "rollout-wire";
    request.postgres_stores = {"orders"};
    auto issued = runtime.create_branch(request);

    const auto port = unused_port();
    PostgresServer server(runtime, ListenAddress{"127.0.0.1", port});
    std::thread server_thread([&] { server.run(); });

    auto connection = connect(port, issued.access_grant.id, issued.database_credential, "orders");
    check(PQstatus(connection.get()) == CONNECTION_OK, PQerrorMessage(connection.get()));
    auto selected = execute(connection.get(), "SELECT state FROM orders WHERE id = 1");
    check(PQresultStatus(selected.get()) == PGRES_TUPLES_OK, PQerrorMessage(connection.get()));
    check(PQntuples(selected.get()) == 1 && std::string(PQgetvalue(selected.get(), 0, 0)) == "open",
          "simple-query SELECT must return branch data");

    auto physical = execute(connection.get(), "SELECT * FROM _chronos_b_interval_orders");
    check(PQresultStatus(physical.get()) == PGRES_FATAL_ERROR,
          "sandbox SQL must not read physical interval tables");
    auto reserved_ddl = execute(connection.get(),
        "CREATE TABLE _chronos_branch_interval_branches (id INTEGER PRIMARY KEY)");
    check(PQresultStatus(reserved_ddl.get()) == PGRES_FATAL_ERROR,
          "sandbox SQL must not create objects in the reserved namespace");

    const char *parameter_values[] = {"1"};
    Result parameterized(PQexecParams(
        connection.get(),
        "SELECT state FROM orders WHERE id = $1",
        1,
        nullptr,
        parameter_values,
        nullptr,
        nullptr,
        0));
    check(PQresultStatus(parameterized.get()) == PGRES_TUPLES_OK,
          PQerrorMessage(connection.get()));
    check(PQntuples(parameterized.get()) == 1 &&
              std::string(PQgetvalue(parameterized.get(), 0, 0)) == "open",
          "extended-query parameters must be executed on the branch");

    // Read compatibility: these all travel over the gateway's pgwire socket
    // and exercise the interval-query rewriter rather than a direct store API.
    auto joined = expect_result(connection.get(),
        "SELECT o.id, c.name, o.amount FROM orders AS o "
        "JOIN customers AS c ON c.id = o.customer_id "
        "WHERE o.state = 'open' ORDER BY o.amount DESC",
        PGRES_TUPLES_OK, "joins, aliases, predicates, and ordering must work");
    check(PQntuples(joined.get()) == 2 &&
              std::string(PQgetvalue(joined.get(), 0, 0)) == "2" &&
              std::string(PQgetvalue(joined.get(), 0, 1)) == "Ada" &&
              std::string(PQgetvalue(joined.get(), 1, 0)) == "1",
          "join result must retain PostgreSQL row order and values");

    auto grouped = expect_result(connection.get(),
        "SELECT customer_id, COUNT(*) AS count, SUM(amount) AS total "
        "FROM orders GROUP BY customer_id HAVING COUNT(*) >= 1 ORDER BY customer_id",
        PGRES_TUPLES_OK, "aggregates, GROUP BY, and HAVING must work");
    check(PQntuples(grouped.get()) == 2 &&
              std::string(PQgetvalue(grouped.get(), 0, 1)) == "2" &&
              std::string(PQgetvalue(grouped.get(), 0, 2)) == "100",
          "aggregate result must be correct");

    auto cte = expect_result(connection.get(),
        "WITH open_orders AS ("
        "SELECT id, customer_id, amount FROM orders WHERE state = 'open') "
        "SELECT customer_id, MAX(amount) FROM open_orders "
        "GROUP BY customer_id ORDER BY customer_id",
        PGRES_TUPLES_OK, "read-only CTEs must work");
    check(PQntuples(cte.get()) == 1 &&
              std::string(PQgetvalue(cte.get(), 0, 1)) == "75",
          "CTE result must be correct");

    expect_cell(connection.get(),
        "SELECT name FROM customers WHERE EXISTS ("
        "SELECT 1 FROM orders WHERE orders.customer_id = customers.id "
        "AND amount > 50)",
        0, 0, "Ada", "correlated EXISTS subqueries must work");
    expect_cell(connection.get(),
        "SELECT CASE WHEN amount >= 50 THEN 'large' ELSE 'small' END "
        "FROM orders WHERE id = 2",
        0, 0, "large", "CASE expressions must work");
    expect_cell(connection.get(),
        "SELECT COALESCE(note, 'none') FROM orders WHERE id = 1",
        0, 0, "none", "NULL and COALESCE must work");

    auto unioned = expect_result(connection.get(),
        "SELECT id FROM orders WHERE id = 1 UNION ALL "
        "SELECT id FROM orders WHERE id = 3 ORDER BY id",
        PGRES_TUPLES_OK, "set operations must work");
    check(PQntuples(unioned.get()) == 2 &&
              std::string(PQgetvalue(unioned.get(), 1, 0)) == "3",
          "UNION ALL result must be correct");

    const char *range_values[] = {"20", "80"};
    Result ranged(PQexecParams(
        connection.get(),
        "SELECT id FROM orders WHERE amount BETWEEN $1 AND $2 ORDER BY id",
        2, nullptr, range_values, nullptr, nullptr, 0));
    check(PQresultStatus(ranged.get()) == PGRES_TUPLES_OK &&
              PQntuples(ranged.get()) == 3,
          "multiple extended-query parameters must work");

    Result prepared(PQprepare(connection.get(), "order_by_id",
        "SELECT state, amount FROM orders WHERE id = $1", 1, nullptr));
    check(PQresultStatus(prepared.get()) == PGRES_COMMAND_OK,
          "named prepared statements must parse");
    const char *prepared_values[] = {"3"};
    Result prepared_result(PQexecPrepared(
        connection.get(), "order_by_id", 1, prepared_values, nullptr, nullptr, 0));
    check(PQresultStatus(prepared_result.get()) == PGRES_TUPLES_OK &&
              PQntuples(prepared_result.get()) == 1 &&
              std::string(PQgetvalue(prepared_result.get(), 0, 0)) == "closed",
          "named prepared statements must bind and execute");
    const char *update_values[] = {"queued", "2"};
    Result parameterized_update(PQexecParams(
        connection.get(),
        "UPDATE orders SET state = $1 WHERE id = $2",
        2, nullptr, update_values, nullptr, nullptr, 0));
    check(PQresultStatus(parameterized_update.get()) == PGRES_COMMAND_OK &&
              std::string(PQcmdStatus(parameterized_update.get())) == "UPDATE 1",
          "extended-query DML must bind and report affected rows");
    expect_cell(connection.get(), "SELECT state FROM orders WHERE id = 2",
                0, 0, "queued", "extended-query DML must change branch data");

    expect_cell(connection.get(),
        "/* ORM trace: rollout */ -- leading comments are legal\n"
        "SELECT state FROM orders WHERE id = 1",
        0, 0, "open", "leading block and line comments must be accepted");
    auto empty_rows = expect_result(connection.get(),
        "SELECT id FROM orders WHERE id = -1",
        PGRES_TUPLES_OK, "zero-row SELECT must work");
    check(PQntuples(empty_rows.get()) == 0 && PQnfields(empty_rows.get()) == 1,
          "zero-row SELECT must retain its row description");

    const char *null_values[] = {nullptr};
    Result null_parameter(PQexecParams(
        connection.get(), "SELECT $1", 1, nullptr, null_values, nullptr, nullptr, 0));
    check(PQresultStatus(null_parameter.get()) == PGRES_TUPLES_OK &&
              PQntuples(null_parameter.get()) == 1 &&
              PQgetisnull(null_parameter.get(), 0, 0),
          "NULL extended-query parameters must retain SQL NULL");
    expect_command(connection.get(), "SET application_name = 'chronos-sql-test'",
                   "SET", "ordinary session SET commands must be accepted");
    expect_cell(connection.get(), "SHOW standard_conforming_strings",
                0, 0, "on", "SHOW commands required by clients must respond");
    auto empty_query = execute(connection.get(), "");
    check(PQresultStatus(empty_query.get()) == PGRES_EMPTY_QUERY,
          "an empty simple query must return EmptyQueryResponse");

    if (use_postgres) {
        expect_cell(connection.get(), "SELECT DATE '2026-09-15'",
                    0, 0, "2026-09-15", "PostgreSQL date literals must work");
        expect_cell(connection.get(), "SELECT ARRAY[1, 2, 3]::text",
                    0, 0, "{1,2,3}", "PostgreSQL array expressions must work");
        expect_cell(connection.get(), "SELECT '{\"agent\": \"ready\"}'::jsonb::text",
                    0, 0, "{\"agent\": \"ready\"}",
                    "PostgreSQL JSONB expressions must work");
        expect_cell(connection.get(), "SELECT 'branch-世界'::text",
                    0, 0, "branch-世界", "UTF-8 query results must work");
    }

    // DML compatibility and command tags.
    expect_command(connection.get(),
        "INSERT INTO orders (id, customer_id, state, amount, note) VALUES "
        "(4, 20, 'open', 15, NULL), (5, 10, 'open', 60, 'gift')",
        "INSERT 0 2", "multi-row INSERT must work");
    expect_command(connection.get(),
        "INSERT INTO orders (id, customer_id, state, amount, note) "
        "VALUES (5, 10, 'duplicate', 1, NULL) ON CONFLICT DO NOTHING",
        "INSERT 0 0", "ON CONFLICT DO NOTHING must work");
    expect_command(connection.get(),
        "UPDATE orders SET amount = amount + 5, note = COALESCE(note, 'updated') "
        "WHERE customer_id = 20",
        "UPDATE 2", "set-based UPDATE expressions must work");
    expect_cell(connection.get(), "SELECT amount FROM orders WHERE id = 3",
                0, 0, "45", "UPDATE result must be visible");
    expect_command(connection.get(), "DELETE FROM orders WHERE id IN (4, 5)",
                   "DELETE 2", "DELETE predicates must work");
    expect_cell(connection.get(), "SELECT COUNT(*) FROM orders",
                0, 0, "3", "DELETE result must be visible");

    // Branch-local DDL must be usable immediately through the same pgwire
    // session, including the copied schema and its logical indexes.
    expect_command(connection.get(),
        "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT)",
        "CREATE 0", "branch-local CREATE TABLE must work");
    expect_command(connection.get(),
        "INSERT INTO tasks (id, title) VALUES (1, 'ship')",
        "INSERT 0 1", "new branch-local tables must accept writes");
    expect_command(connection.get(),
        "ALTER TABLE tasks ADD COLUMN priority TEXT DEFAULT 'normal'",
        "ALTER 0", "branch-local ALTER TABLE ADD COLUMN must work");
    expect_cell(connection.get(), "SELECT priority FROM tasks WHERE id = 1",
                0, 0, "normal", "ALTER TABLE defaults must be visible");
    expect_command(connection.get(), "CREATE INDEX tasks_title_idx ON tasks (title)",
                   "CREATE 0", "branch-local CREATE INDEX must work");
    expect_command(connection.get(), "DROP INDEX tasks_title_idx",
                   "DROP 0", "branch-local DROP INDEX must work");
    expect_command(connection.get(),
        "ALTER TABLE orders ADD COLUMN branch_label TEXT DEFAULT 'rollout'",
        "ALTER 0", "ALTER TABLE on an inherited table must copy its schema version");
    expect_cell(connection.get(),
        "SELECT branch_label FROM orders WHERE id = 2",
        0, 0, "rollout", "schema-copy ALTER must preserve rows and backfill defaults");
    expect_cell(connection.get(), "SELECT COUNT(*) FROM orders",
                0, 0, "3", "schema copying must preserve every visible row");

    // A simple-query error must not poison an autocommit connection.
    auto syntax_error = execute(connection.get(), "SELECT * FROM missing_table");
    check(PQresultStatus(syntax_error.get()) == PGRES_FATAL_ERROR,
          "invalid SQL must return a PostgreSQL ErrorResponse");
    expect_cell(connection.get(), "SELECT state FROM orders WHERE id = 1",
                0, 0, "open", "connection must recover after an autocommit error");

    auto updated = execute(connection.get(), "UPDATE orders SET state = 'placed' WHERE id = 1");
    check(PQresultStatus(updated.get()) == PGRES_COMMAND_OK, PQerrorMessage(connection.get()));
    selected = execute(connection.get(), "SELECT state FROM orders WHERE id = 1");
    check(std::string(PQgetvalue(selected.get(), 0, 0)) == "placed",
          "wire-protocol DML must update the workspace branch");
    auto main = setup.checkout("main");
    auto base = main.query("SELECT state FROM orders WHERE id = 1");
    check(std::get<std::string>(base.rows.at(0).at(0)) == "open",
          "wire-protocol DML must not update the source branch");
    bool source_rejected_branch_column = false;
    try {
        (void)main.query("SELECT branch_label FROM orders");
    } catch (const std::exception &) {
        source_rejected_branch_column = true;
    }
    check(source_rejected_branch_column,
          "branch-local ALTER TABLE must not change the source branch schema");
    bool source_rejected_branch_table = false;
    try {
        (void)main.query("SELECT * FROM tasks");
    } catch (const std::exception &) {
        source_rejected_branch_table = true;
    }
    check(source_rejected_branch_table,
          "branch-local CREATE TABLE must not expose the table on the source branch");

    check(PQresultStatus(execute(connection.get(), "BEGIN").get()) == PGRES_COMMAND_OK,
          "BEGIN must succeed");
    check(PQresultStatus(execute(connection.get(),
        "UPDATE orders SET state = 'cancelled' WHERE id = 1").get()) == PGRES_COMMAND_OK,
        "transactional update must succeed");
    check(PQresultStatus(execute(connection.get(), "ROLLBACK").get()) == PGRES_COMMAND_OK,
          "ROLLBACK must succeed");
    selected = execute(connection.get(), "SELECT state FROM orders WHERE id = 1");
    check(std::string(PQgetvalue(selected.get(), 0, 0)) == "placed",
          "ROLLBACK must restore branch state");

    expect_command(connection.get(), "BEGIN", "BEGIN",
                   "a transaction used for failure recovery must begin");
    auto transaction_error = execute(connection.get(), "SELECT * FROM missing_table");
    check(PQresultStatus(transaction_error.get()) == PGRES_FATAL_ERROR &&
              PQtransactionStatus(connection.get()) == PQTRANS_INERROR,
          "an error inside a transaction must enter failed-transaction state");
    auto blocked = execute(connection.get(), "SELECT state FROM orders WHERE id = 1");
    const char *blocked_state = PQresultErrorField(blocked.get(), PG_DIAG_SQLSTATE);
    check(PQresultStatus(blocked.get()) == PGRES_FATAL_ERROR && blocked_state &&
              std::string(blocked_state) == "25P02",
          "queries in a failed transaction must be rejected with SQLSTATE 25P02");
    expect_command(connection.get(), "ROLLBACK", "ROLLBACK",
                   "ROLLBACK must recover a failed transaction");
    check(PQtransactionStatus(connection.get()) == PQTRANS_IDLE,
          "ROLLBACK must restore idle transaction state");
    expect_cell(connection.get(), "SELECT state FROM orders WHERE id = 1",
                0, 0, "placed", "connection must work after failed-transaction recovery");
    expect_command(connection.get(), "BEGIN", "BEGIN",
                   "a committing transaction must begin");
    expect_command(connection.get(),
        "INSERT INTO orders "
        "(id, customer_id, state, amount, note, branch_label) "
        "VALUES (6, 10, 'committed', 30, NULL, 'rollout')",
        "INSERT 0 1", "DML inside a committing transaction must work");
    expect_command(connection.get(), "COMMIT", "COMMIT",
                   "COMMIT must publish a transaction");
    expect_cell(connection.get(), "SELECT state FROM orders WHERE id = 6",
                0, 0, "committed", "committed data must remain visible");
    expect_command(connection.get(), "DELETE FROM orders WHERE id = 6",
                   "DELETE 1", "committed test data must be removable");

    auto bad = connect(port, issued.access_grant.id, "wrong", "orders");
    check(PQstatus(bad.get()) == CONNECTION_BAD, "wrong grant secret must fail authentication");

    runtime.close_branch("training", "rollout-wire");
    auto revoked = execute(connection.get(), "SELECT state FROM orders");
    check(PQresultStatus(revoked.get()) == PGRES_FATAL_ERROR,
          "revocation must invalidate an already-open SQL connection");

    connection.reset();
    bad.reset();
    server.stop();
    server_thread.join();
    std::cout << "all PostgreSQL gateway protocol tests passed\n";
    return 0;
}
