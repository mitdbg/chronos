#include "chronos/gateway/runtime.hpp"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

using chronos::gateway::CreateBranchRequest;
using chronos::gateway::FilesystemAccess;
using chronos::gateway::FilesystemConfig;
using chronos::gateway::GatewayRuntime;
using chronos::gateway::GatewayError;
using chronos::gateway::StoreConfig;
using chronos::gateway::WorkspaceConfig;
using chronos::native::IntervalBlob;
using chronos::native::IntervalValue;
using chronos::native::NativeBranchStore;

namespace {

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

template <typename Function>
void rejects(Function &&function, const char *message) {
    try {
        function();
    } catch (const GatewayError &) {
        return;
    }
    check(false, message);
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-runtime.XXXXXX";
        pattern.push_back('\0');
        path = mkdtemp(pattern.data());
        if (path.empty()) throw std::runtime_error("mkdtemp failed");
    }
    ~TempDirectory() { std::filesystem::remove_all(path); }
    std::string path;
};

std::string sqlite_url(const std::string &path) {
    return "sqlite://" + path;
}

} // namespace

int main() {
    TempDirectory temp;
    const auto metadata = sqlite_url(temp.path + "/metadata.sqlite");
    const auto orders = sqlite_url(temp.path + "/orders.sqlite");
    const auto events = sqlite_url(temp.path + "/events.sqlite");
    const auto files = sqlite_url(temp.path + "/files.sqlite");

    for (const auto &path : {
             temp.path + "/metadata.sqlite",
             temp.path + "/orders.sqlite",
             temp.path + "/events.sqlite",
             temp.path + "/files.sqlite"}) {
        std::ofstream(path).close();
    }

    GatewayRuntime invalid_runtime;
    WorkspaceConfig missing_metadata;
    missing_metadata.postgres_stores.emplace("orders", StoreConfig{orders, {}});
    rejects(
        [&] { invalid_runtime.add_workspace("missing-metadata", missing_metadata); },
        "gateway data stores must use separate metadata");
    WorkspaceConfig colocated_metadata;
    colocated_metadata.postgres_stores.emplace("orders", StoreConfig{orders, orders});
    rejects(
        [&] { invalid_runtime.add_workspace("colocated-metadata", colocated_metadata); },
        "gateway metadata must not be exposed through its data connection");

    NativeBranchStore orders_setup(orders, metadata);
    orders_setup.ensure();
    orders_setup.execute_sql("CREATE TABLE orders (id INTEGER PRIMARY KEY, state TEXT)");
    orders_setup.execute_sql("INSERT INTO orders VALUES (?, ?)", {std::int64_t(1), std::string("open")});
    orders_setup.commit();
    orders_setup.register_table("orders", {"id"});

    NativeBranchStore events_setup(events, metadata);
    events_setup.ensure();
    events_setup.execute_sql("CREATE TABLE events (id INTEGER PRIMARY KEY, note TEXT)");
    events_setup.execute_sql("INSERT INTO events VALUES (?, ?)", {std::int64_t(1), std::string("base")});
    events_setup.commit();
    events_setup.register_table("events", {"id"});

    auto now = std::chrono::system_clock::time_point{std::chrono::seconds(5000)};
    GatewayRuntime runtime(std::chrono::minutes(10), [&] { return now; });
    WorkspaceConfig workspace;
    workspace.postgres_stores.emplace("orders", StoreConfig{orders, metadata});
    workspace.postgres_stores.emplace("events", StoreConfig{events, metadata});
    workspace.filesystem = FilesystemConfig{files, metadata, 4096};
    runtime.add_workspace("training", workspace);

    CreateBranchRequest request;
    request.workspace = "training";
    request.from_branch = "main";
    request.branch_id = "rollout-1";
    request.postgres_stores = {"orders"};
    request.filesystem_access = FilesystemAccess::ReadWrite;
    request.ttl = std::chrono::minutes(30);
    auto issued = runtime.create_branch(request);

    rejects(
        [&] { runtime.open_postgres(issued.access_grant.id, issued.database_credential, "events"); },
        "grant must not open an unlisted store");
    auto sql = runtime.open_postgres(issued.access_grant.id, issued.database_credential, "orders");
    sql->session().execute(
        "UPDATE orders SET state = ? WHERE id = ?",
        {std::string("placed"), std::int64_t(1)});
    auto branch_rows = sql->session().query("SELECT state FROM orders WHERE id = ?", {std::int64_t(1)});
    check(std::get<std::string>(branch_rows.rows.at(0).at(0)) == "placed", "grant SQL must write its branch");
    auto main = orders_setup.checkout("main");
    auto main_rows = main.query("SELECT state FROM orders WHERE id = ?", {std::int64_t(1)});
    check(std::get<std::string>(main_rows.rows.at(0).at(0)) == "open", "grant SQL must not change main");

    auto bound = runtime.attach_branch("training", "rollout-1", "e2b-1", "10.0.0.21");
    chronos::gateway::AccessGrant fs_workspace;
    auto &filesystem = runtime.authorize_filesystem(
        bound.export_id, "10.0.0.21", true, &fs_workspace);
    auto result = filesystem.create_file(fs_workspace.branch, 1, "result.txt", 0644);
    const std::string payload = "reward=1";
    filesystem.write(
        fs_workspace.branch,
        result,
        0,
        IntervalBlob(payload.begin(), payload.end()));
    check(
        filesystem.lookup_child(fs_workspace.branch, 1, "result.txt").id == result,
        "NFS authorization must resolve the workspace branch");

    runtime.close_branch("training", "rollout-1");
    rejects(
        [&] { runtime.open_postgres(issued.access_grant.id, issued.database_credential, "orders"); },
        "revoked SQL grant must fail");
    rejects(
        [&] { runtime.authorize_filesystem(bound.export_id, "10.0.0.21", false); },
        "revoked filesystem grant must fail");
    check(runtime.reclaim_due().empty(), "recovery window must delay deletion");
    now += std::chrono::minutes(10);
    check(runtime.reclaim_due().size() == 1, "reaper must delete expired branch");

    std::cout << "all gateway runtime integration tests passed\n";
    return 0;
}
