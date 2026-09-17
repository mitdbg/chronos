#include "chronos/gateway/control_server.hpp"

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

using chronos::gateway::ControlServer;
using chronos::gateway::FilesystemConfig;
using chronos::gateway::GatewayRuntime;
using chronos::gateway::ListenAddress;
using chronos::gateway::StoreConfig;
using chronos::gateway::WorkspaceConfig;
using chronos::native::NativeBranchStore;

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;
using Json = nlohmann::json;

void check(bool condition, const std::string &message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-control.XXXXXX";
        pattern.push_back('\0');
        path = mkdtemp(pattern.data());
        if (path.empty()) throw std::runtime_error("mkdtemp failed");
    }
    ~TempDirectory() { std::filesystem::remove_all(path); }
    std::string path;
};

std::uint16_t unused_port() {
    asio::io_context io;
    tcp::acceptor acceptor(io, {asio::ip::make_address("127.0.0.1"), 0});
    return acceptor.local_endpoint().port();
}

http::response<http::string_body> request(
    std::uint16_t port,
    http::verb method,
    const std::string &target,
    const Json &body = Json{},
    const std::string &token = "controller-test") {
    for (int attempt = 0; attempt < 40; ++attempt) {
        try {
            asio::io_context io;
            tcp::socket socket(io);
            socket.connect({asio::ip::make_address("127.0.0.1"), port});
            http::request<http::string_body> outgoing(method, target, 11);
            outgoing.set(http::field::host, "127.0.0.1");
            if (!token.empty()) outgoing.set(http::field::authorization, "Bearer " + token);
            if (!body.is_null() && !body.empty()) {
                outgoing.set(http::field::content_type, "application/json");
                outgoing.body() = body.dump();
                outgoing.prepare_payload();
            }
            http::write(socket, outgoing);
            beast::flat_buffer buffer;
            http::response<http::string_body> incoming;
            http::read(socket, buffer, incoming);
            return incoming;
        } catch (const boost::system::system_error &) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
    }
    throw std::runtime_error("control server did not accept connections");
}

} // namespace

int main() {
    TempDirectory temp;
    const std::string metadata = "sqlite://" + temp.path + "/metadata.sqlite";
    const std::string data = "sqlite://" + temp.path + "/orders.sqlite";
    const std::string files = "sqlite://" + temp.path + "/files.sqlite";
    for (const auto &path : {temp.path + "/metadata.sqlite", temp.path + "/orders.sqlite",
                             temp.path + "/files.sqlite"}) {
        std::ofstream(path).close();
    }
    NativeBranchStore setup(data, metadata);
    setup.ensure();
    setup.execute_sql("CREATE TABLE orders (id INTEGER PRIMARY KEY, state TEXT)");
    setup.commit();
    setup.register_table("orders", {"id"});

    GatewayRuntime runtime;
    WorkspaceConfig workspace;
    workspace.postgres_stores.emplace("orders", StoreConfig{data, metadata});
    workspace.filesystem = FilesystemConfig{files, metadata, 4096};
    runtime.add_workspace("training", workspace);

    const auto port = unused_port();
    ControlServer server(
        runtime,
        ListenAddress{"127.0.0.1", port},
        ListenAddress{"127.0.0.1", 15432},
        ListenAddress{"127.0.0.1", 12049},
        "controller-test");
    std::thread server_thread([&] { server.run(); });

    auto health = request(port, http::verb::get, "/healthz", Json{}, "");
    check(health.result() == http::status::ok, "health endpoint must be public");
    const std::string branch_path = "/v1/workspaces/training/branches/rollout%2Fcontrol";
    auto denied = request(port, http::verb::get, branch_path, Json{}, "wrong");
    check(denied.result() == http::status::unauthorized, "control endpoints require bearer auth");

    Json create_body = {
        {"branch_id", "rollout/control"},
        {"databases", Json::array({"orders"})},
        {"filesystem", "read_write"},
        {"ttl_seconds", 120},
    };
    auto created = request(
        port, http::verb::post, "/v1/workspaces/training/branches", create_body);
    check(created.result() == http::status::created, created.body());
    auto branch = Json::parse(created.body());
    check(branch.at("branch_id") == "rollout/control", "control API must preserve branch id");
    check(branch.at("workspace") == "training", "control API must return workspace name");
    check(!branch.contains("secret") && !branch.contains("credential") &&
              !branch.contains("grant_id") && !branch.contains("access_id"),
          "control API must hide internal access-grant details");
    check(branch.at("database_urls").contains("orders"),
          "control API must return allowed database URL");

    auto attached = request(port, http::verb::post,
                            branch_path + "/attach", {
        {"sandbox_id", "e2b-test"}, {"sandbox_ip", "10.0.0.42"}});
    check(attached.result() == http::status::ok, attached.body());
    auto binding = Json::parse(attached.body());
    check(binding.at("nfs_export").get<std::string>().rfind("127.0.0.1:/", 0) == 0,
          "attach must return an opaque NFS export path");
    check(binding.at("nfs_options").get<std::string>().find("port=12049") != std::string::npos,
          "attach must advertise the configured NFS port");

    auto fetched = request(port, http::verb::get, branch_path);
    check(fetched.result() == http::status::ok, fetched.body());
    check(!Json::parse(fetched.body()).contains("database_urls"),
          "branch lookup must not reissue database credentials");
    auto closed = request(port, http::verb::delete_, branch_path);
    check(closed.result() == http::status::ok, closed.body());
    check(Json::parse(closed.body()).at("state") == "closed", "DELETE must close immediately");

    server.stop();
    server_thread.join();
    std::cout << "all gateway control API tests passed\n";
    return 0;
}
