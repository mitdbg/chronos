#include "chronos/gateway/filesystem_view.hpp"

#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

using chronos::gateway::CreateBranchRequest;
using chronos::gateway::FilesystemAccess;
using chronos::gateway::FilesystemConfig;
using chronos::gateway::FilesystemView;
using chronos::gateway::GatewayRuntime;
using chronos::gateway::GatewayError;
using chronos::gateway::WorkspaceConfig;
using chronos::native::IntervalBlob;

namespace {

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

template <typename Function>
void rejects(Function &&function, const char *message) {
    try { function(); } catch (const std::exception &) { return; }
    check(false, message);
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-view.XXXXXX";
        pattern.push_back('\0');
        path = mkdtemp(pattern.data());
        if (path.empty()) throw std::runtime_error("mkdtemp failed");
    }
    ~TempDirectory() { std::filesystem::remove_all(path); }
    std::string path;
};

chronos::gateway::ProvisionedBranch make_branch(
    GatewayRuntime &runtime,
    const std::string &branch,
    FilesystemAccess access,
    const std::string &sandbox,
    const std::string &ip) {
    CreateBranchRequest request;
    request.workspace = "training";
    request.from_branch = "main";
    request.branch_id = branch;
    request.filesystem_access = access;
    auto issued = runtime.create_branch(request);
    issued.access_grant = runtime.attach_branch("training", branch, sandbox, ip);
    return issued;
}

} // namespace

int main() {
    TempDirectory temp;
    const auto metadata = "sqlite://" + temp.path + "/metadata.sqlite";
    const auto files = "sqlite://" + temp.path + "/files.sqlite";
    std::ofstream(temp.path + "/metadata.sqlite").close();
    std::ofstream(temp.path + "/files.sqlite").close();

    GatewayRuntime runtime;
    WorkspaceConfig workspace;
    workspace.filesystem = FilesystemConfig{files, metadata, 4096};
    runtime.add_workspace("training", workspace);
    auto writable = make_branch(
        runtime, "rollout-view-rw", FilesystemAccess::ReadWrite, "e2b-rw", "10.0.0.30");
    auto read_only = make_branch(
        runtime, "rollout-view-ro", FilesystemAccess::ReadOnly, "e2b-ro", "10.0.0.31");
    FilesystemView view(runtime);

    auto root = view.list("/");
    check(root.size() == 2, "capability root must not enumerate export ids");
    const auto rw = "/" + writable.access_grant.export_id;
    const auto ro = "/" + read_only.access_grant.export_id;
    check(view.stat(rw).kind == "directory", "export capability must resolve branch root");
    view.create_directory(rw + "/work", 0755);
    view.create_file(rw + "/work/result.txt", 0644, 1234, 2345);
    const auto owned = view.stat(rw + "/work/result.txt");
    check(owned.uid == 1234 && owned.gid == 2345,
          "capability view must propagate request ownership");
    view.chown(rw + "/work/result.txt", static_cast<std::uint32_t>(-1), 5432);
    const auto changed_owner = view.stat(rw + "/work/result.txt");
    check(changed_owner.uid == 1234 && changed_owner.gid == 5432,
          "capability view must apply partial ownership changes");
    const struct timespec timestamps[2] = {{123456789, 0}, {123456790, 0}};
    view.utimens(rw + "/work/result.txt", timestamps);
    const auto changed_times = view.stat(rw + "/work/result.txt");
    check(changed_times.atime != owned.atime && changed_times.mtime != owned.mtime,
          "capability view must apply timestamp changes");
    const std::string contents = "reward=1";
    view.write(rw + "/work/result.txt", 0, IntervalBlob(contents.begin(), contents.end()));
    const auto read = view.read(rw + "/work/result.txt", 0, 64);
    check(std::string(read.begin(), read.end()) == contents, "capability path must read written bytes");
    view.rename(rw + "/work/result.txt", rw + "/work/final.txt");
    view.truncate(rw + "/work/final.txt", 6);
    check(view.stat(rw + "/work/final.txt").size == 6, "truncate must update workspace inode");
    view.create_symlink(rw + "/latest", "work/final.txt");
    check(view.read_symlink(rw + "/latest") == "work/final.txt", "symlink target must round trip");

    rejects([&] { view.create_file(ro + "/forbidden", 0644); },
            "read-only capability must reject mutations");
    rejects([&] { view.rename(rw + "/work/final.txt", ro + "/stolen"); },
            "rename must not cross capabilities");
    rejects([&] { view.stat("/unknown/file"); }, "unknown capabilities must not resolve");
    rejects([&] { view.stat(rw + "/../" + read_only.access_grant.export_id); },
            "path traversal must not escape a capability");

    runtime.close_branch("training", "rollout-view-rw");
    rejects([&] { view.read(rw + "/work/final.txt", 0, 1); },
            "revocation must invalidate existing filesystem paths");

    std::cout << "all gateway filesystem view tests passed\n";
    return 0;
}
