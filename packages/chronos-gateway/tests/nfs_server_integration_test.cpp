#include "chronos/gateway/nfs_server.hpp"

#include <boost/asio.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <exception>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>
#include <unistd.h>

using chronos::gateway::CreateBranchRequest;
using chronos::gateway::FilesystemAccess;
using chronos::gateway::FilesystemConfig;
using chronos::gateway::GatewayConfig;
using chronos::gateway::GatewayRuntime;
using chronos::gateway::ListenAddress;
using chronos::gateway::NfsServer;
using chronos::gateway::WorkspaceConfig;

namespace {

void check(bool condition, const std::string &message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-nfs.XXXXXX";
        pattern.push_back('\0');
        path = mkdtemp(pattern.data());
        if (path.empty()) throw std::runtime_error("mkdtemp failed");
    }
    ~TempDirectory() { std::filesystem::remove_all(path); }
    std::string path;
};

int command(const std::vector<std::string> &arguments) {
    const pid_t child = fork();
    if (child == 0) {
        std::vector<char *> argv;
        for (const auto &argument : arguments) argv.push_back(const_cast<char *>(argument.c_str()));
        argv.push_back(nullptr);
        execv(argv.front(), argv.data());
        _exit(127);
    }
    if (child < 0) return -1;
    int status = 0;
    if (waitpid(child, &status, 0) < 0) return -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

std::uint16_t unused_port() {
    boost::asio::io_context io;
    boost::asio::ip::tcp::acceptor acceptor(
        io, {boost::asio::ip::make_address("127.0.0.1"), 0});
    return acceptor.local_endpoint().port();
}

bool port_open(std::uint16_t port) {
    try {
        boost::asio::io_context io;
        boost::asio::ip::tcp::socket socket(io);
        socket.connect({boost::asio::ip::make_address("127.0.0.1"), port});
        return true;
    } catch (...) { return false; }
}

} // namespace

int main() {
    const char *library = std::getenv("CHRONOS_GATEWAY_NFS_LIBRARY");
    const char *plugins = std::getenv("CHRONOS_GATEWAY_NFS_PLUGINS");
    if (!library || !plugins || geteuid() != 0) {
        std::cerr << "SKIP: set Ganesha paths and run as root for NFS integration\n";
        return 77;
    }

    TempDirectory temp;
    const auto metadata = "sqlite://" + temp.path + "/metadata.sqlite";
    const auto files = "sqlite://" + temp.path + "/files.sqlite";
    std::ofstream(temp.path + "/metadata.sqlite").close();
    std::ofstream(temp.path + "/files.sqlite").close();

    GatewayRuntime runtime;
    WorkspaceConfig workspace;
    workspace.filesystem = FilesystemConfig{files, metadata, 4096};
    runtime.add_workspace("training", workspace);
    CreateBranchRequest request;
    request.workspace = "training";
    request.from_branch = "main";
    request.branch_id = "rollout-nfs";
    request.filesystem_access = FilesystemAccess::ReadWrite;
    auto issued = runtime.create_branch(request);
    issued.access_grant = runtime.attach_branch(
        "training", "rollout-nfs", "e2b-nfs", "127.0.0.1");
    request.branch_id = "rollout-nfs-sibling";
    auto sibling = runtime.create_branch(request);
    sibling.access_grant = runtime.attach_branch(
        "training", "rollout-nfs-sibling", "e2b-nfs-sibling", "127.0.0.2");

    const auto port = unused_port();
    GatewayConfig::NfsRuntime nfs;
    nfs.ganesha_library = library;
    nfs.plugins_directory = plugins;
    nfs.state_directory = temp.path + "/state";
    nfs.mountpoint = temp.path + "/backing";
    const auto client_mount = temp.path + "/client";
    const auto sibling_mount = temp.path + "/sibling";
    std::filesystem::create_directory(client_mount);
    std::filesystem::create_directory(sibling_mount);
    NfsServer server(runtime, ListenAddress{"127.0.0.1", port}, nfs);
    std::exception_ptr failure;
    std::atomic<bool> ended{false};
    std::thread thread([&] {
        try { server.run(); } catch (...) { failure = std::current_exception(); }
        ended.store(true);
    });
    bool ready = false;
    for (int attempt = 0; attempt < 500 && !ended.load(); ++attempt) {
        if (port_open(port)) { ready = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (!ready) {
        server.stop();
        thread.join();
        if (failure) std::rethrow_exception(failure);
        check(false, "embedded NFS server did not listen");
    }

    const auto options = "vers=4.2,port=" + std::to_string(port) +
        ",proto=tcp,timeo=10,retrans=1";
    const auto remote = "127.0.0.1:/" + issued.access_grant.export_id;
    const int mounted = command({"/usr/bin/mount", "-t", "nfs4", "-o", options,
                                 remote, client_mount});
    if (mounted != 0) {
        server.stop();
        thread.join();
        check(false, "NFSv4 client could not mount the grant capability");
    }
    const auto sibling_remote = "127.0.0.1:/" + sibling.access_grant.export_id;
    const int sibling_mounted = command({"/usr/bin/mount", "-t", "nfs4", "-o", options,
                                         sibling_remote, sibling_mount});
    if (sibling_mounted != 0) {
        command({"/usr/bin/umount", client_mount});
        server.stop();
        thread.join();
        check(false, "NFSv4 client could not mount a sibling capability");
    }
    {
        std::ofstream output(client_mount + "/result.txt");
        output << "reward=1";
    }
    {
        std::ifstream input(client_mount + "/result.txt");
        std::string contents;
        std::getline(input, contents);
        check(contents == "reward=1", "NFSv4 bytes must round trip through ChronosFS");
    }
    check(!std::filesystem::exists(sibling_mount + "/result.txt"),
          "an NFS write must remain isolated from a sibling branch");

    runtime.close_branch("training", "rollout-nfs");
    {
        std::ofstream forbidden(client_mount + "/after-revoke.txt");
        check(!forbidden, "revocation must reject new operations on an existing NFS mount");
    }
    check(command({"/usr/bin/umount", client_mount}) == 0, "NFS client must unmount cleanly");
    check(command({"/usr/bin/umount", sibling_mount}) == 0,
          "sibling NFS client must unmount cleanly");

    server.stop();
    thread.join();
    if (failure) std::rethrow_exception(failure);
    std::cout << "all embedded NFSv4 integration tests passed\n";
    return 0;
}
