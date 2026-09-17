#include "chronos/gateway/postgres_server.hpp"

#include <boost/asio.hpp>
#include <libpq-fe.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using chronos::gateway::CreateBranchRequest;
using chronos::gateway::GatewayRuntime;
using chronos::gateway::ProvisionedBranch;
using chronos::gateway::ListenAddress;
using chronos::gateway::PostgresServer;
using chronos::gateway::StoreConfig;
using chronos::gateway::WorkspaceConfig;
using chronos::native::NativeBranchStore;

namespace {

constexpr int rollout_count = 128;

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-concurrency.XXXXXX";
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

std::uint16_t unused_port() {
    boost::asio::io_context io;
    boost::asio::ip::tcp::acceptor acceptor(
        io, {boost::asio::ip::make_address("127.0.0.1"), 0});
    return acceptor.local_endpoint().port();
}

Connection connect(std::uint16_t port, const ProvisionedBranch &grant) {
    const std::string parameters =
        "host=127.0.0.1 port=" + std::to_string(port) +
        " user=" + grant.access_grant.id + " password=" + grant.database_credential +
        " dbname=orders sslmode=disable connect_timeout=5";
    for (int attempt = 0; attempt < 100; ++attempt) {
        Connection connection(PQconnectdb(parameters.c_str()));
        if (PQstatus(connection.get()) == CONNECTION_OK) return connection;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return Connection(PQconnectdb(parameters.c_str()));
}

} // namespace

int main() {
    TempDirectory temp;
    const auto metadata = "sqlite://" + temp.path + "/metadata.sqlite";
    const auto data = "sqlite://" + temp.path + "/orders.sqlite";
    std::ofstream(temp.path + "/metadata.sqlite").close();
    std::ofstream(temp.path + "/orders.sqlite").close();
    NativeBranchStore setup(data, metadata);
    setup.ensure();
    setup.execute_sql("CREATE TABLE orders (id INTEGER PRIMARY KEY, state TEXT)");
    setup.execute_sql("INSERT INTO orders VALUES (?, ?)",
                      {std::int64_t(1), std::string("open")});
    setup.commit();
    setup.register_table("orders", {"id"});

    GatewayRuntime runtime;
    WorkspaceConfig workspace;
    workspace.postgres_stores.emplace("orders", StoreConfig{data, metadata});
    runtime.add_workspace("training", workspace);
    std::vector<ProvisionedBranch> grants;
    grants.reserve(rollout_count);
    for (int index = 0; index < rollout_count; ++index) {
        CreateBranchRequest request;
        request.workspace = "training";
        request.from_branch = "main";
        request.branch_id = "parallel-rollout-" + std::to_string(index);
        request.postgres_stores = {"orders"};
        grants.push_back(runtime.create_branch(request));
    }

    const auto port = unused_port();
    PostgresServer server(runtime, ListenAddress{"127.0.0.1", port});
    std::thread server_thread([&] { server.run(); });
    std::atomic<int> passed{0};
    std::vector<std::thread> clients;
    clients.reserve(rollout_count);
    for (const auto &grant : grants) {
        clients.emplace_back([&, grant] {
            auto connection = connect(port, grant);
            if (PQstatus(connection.get()) != CONNECTION_OK) return;
            PGresult *raw = PQexec(connection.get(), "SELECT state FROM orders WHERE id = 1");
            if (!raw) return;
            std::unique_ptr<PGresult, decltype(&PQclear)> result(raw, &PQclear);
            if (PQresultStatus(result.get()) == PGRES_TUPLES_OK &&
                PQntuples(result.get()) == 1 &&
                std::string(PQgetvalue(result.get(), 0, 0)) == "open") {
                ++passed;
            }
        });
    }
    for (auto &client : clients) client.join();
    check(passed.load() == rollout_count,
          "all 128 branch-scoped clients must query concurrently");
    server.stop();
    server_thread.join();
    std::cout << "all 128-way gateway concurrency tests passed\n";
    return 0;
}
