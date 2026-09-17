#include "chronos/gateway/fuse_server.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <thread>
#include <unistd.h>

using chronos::gateway::CreateBranchRequest;
using chronos::gateway::FilesystemAccess;
using chronos::gateway::FilesystemConfig;
using chronos::gateway::FuseServer;
using chronos::gateway::GatewayRuntime;
using chronos::gateway::WorkspaceConfig;

namespace {

constexpr long fuse_super_magic = 0x65735546;

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-fuse.XXXXXX";
        pattern.push_back('\0');
        path = mkdtemp(pattern.data());
        if (path.empty()) throw std::runtime_error("mkdtemp failed");
    }
    ~TempDirectory() { std::filesystem::remove_all(path); }
    std::string path;
};

} // namespace

int main() {
    TempDirectory temp;
    const auto metadata = "sqlite://" + temp.path + "/metadata.sqlite";
    const auto files = "sqlite://" + temp.path + "/files.sqlite";
    const auto mountpoint = temp.path + "/mount";
    std::ofstream(temp.path + "/metadata.sqlite").close();
    std::ofstream(temp.path + "/files.sqlite").close();
    std::filesystem::create_directory(mountpoint);

    GatewayRuntime runtime;
    WorkspaceConfig workspace;
    workspace.filesystem = FilesystemConfig{files, metadata, 4096};
    runtime.add_workspace("training", workspace);
    CreateBranchRequest request;
    request.workspace = "training";
    request.from_branch = "main";
    request.branch_id = "rollout-fuse";
    request.filesystem_access = FilesystemAccess::ReadWrite;
    auto issued = runtime.create_branch(request);
    issued.access_grant = runtime.attach_branch(
        "training", "rollout-fuse", "e2b-fuse", "10.0.0.50");

    FuseServer server(runtime);
    std::atomic<int> result{-1};
    std::thread thread([&] {
        result.store(server.run(mountpoint,
            {"attr_timeout=0", "entry_timeout=0", "negative_timeout=0"}));
    });
    bool mounted = false;
    for (int attempt = 0; attempt < 100; ++attempt) {
        struct statfs status{};
        if (statfs(mountpoint.c_str(), &status) == 0 && status.f_type == fuse_super_magic) {
            mounted = true;
            break;
        }
        if (result.load() != -1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (!mounted) {
        server.stop();
        thread.join();
        std::cerr << "SKIP: FUSE mounts are unavailable in this environment\n";
        return 77;
    }

    const auto export_root = mountpoint + "/" + issued.access_grant.export_id;
    check(std::filesystem::is_directory(export_root), "capability root must be mountable");
    std::size_t visible_exports = 0;
    for (const auto &ignored : std::filesystem::directory_iterator(mountpoint)) {
        (void)ignored;
        ++visible_exports;
    }
    check(visible_exports == 0, "mount root must not enumerate grant capabilities");
    std::filesystem::create_directory(export_root + "/work");
    {
        std::ofstream output(export_root + "/work/result.txt");
        output << "reward=1";
    }
    std::ifstream input(export_root + "/work/result.txt");
    std::string contents;
    std::getline(input, contents);
    check(contents == "reward=1", "POSIX writes and reads must traverse the capability view");
    std::filesystem::rename(
        export_root + "/work/result.txt", export_root + "/work/final.txt");
    check(std::filesystem::file_size(export_root + "/work/final.txt") == 8,
          "renamed file must preserve contents");
    const struct timespec timestamps[2] = {{123456789, 0}, {123456790, 0}};
    check(::utimensat(
              AT_FDCWD,
              (export_root + "/work/final.txt").c_str(),
              timestamps,
              0) == 0,
          "POSIX timestamp updates must traverse the capability view");
    struct stat timestamped{};
    check(::stat((export_root + "/work/final.txt").c_str(), &timestamped) == 0 &&
              timestamped.st_atim.tv_sec == timestamps[0].tv_sec &&
              timestamped.st_mtim.tv_sec == timestamps[1].tv_sec,
          "FUSE attributes must expose persisted timestamps");

    runtime.close_branch("training", "rollout-fuse");
    errno = 0;
    struct stat revoked{};
    check(::stat((export_root + "/new-lookup").c_str(), &revoked) != 0,
          "revoked capability must reject new filesystem operations");

    server.stop();
    thread.join();
    check(result.load() == 0, "FUSE event loop must shut down cleanly");
    std::cout << "all gateway FUSE integration tests passed\n";
    return 0;
}
