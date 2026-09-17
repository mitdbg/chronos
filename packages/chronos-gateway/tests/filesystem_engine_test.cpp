#include "chronosfs_fuse.hpp"

#include <cerrno>
#include <ctime>
#include <cstdlib>
#include <iostream>
#include <string>

using chronos::native::ChronosFSError;
using chronos::native::IntervalBlob;
using chronos::native::NativeChronosFilesystem;

namespace {

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

template <typename Function>
void rejects_errno(Function &&function, int expected, const char *message) {
    try {
        function();
    } catch (const ChronosFSError &error) {
        check(error.error_code() == expected, message);
        return;
    }
    check(false, message);
}

} // namespace

int main() {
    NativeChronosFilesystem filesystem(":memory:", "", 4096);
    filesystem.ensure();
    filesystem.create_branch("rollout-a", "main");
    filesystem.create_branch("rollout-b", "main");

    const auto file_a = filesystem.create_file(
        "rollout-a", 1, "result.txt", 0644, 1234, 2345);
    const auto owned_file = filesystem.stat_inode("rollout-a", file_a);
    check(owned_file.uid == 1234 && owned_file.gid == 2345,
          "file creation must preserve the caller's identity");
    filesystem.chown("rollout-a", file_a, 4321, static_cast<std::uint32_t>(-1));
    const auto changed_owner = filesystem.stat_inode("rollout-a", file_a);
    check(changed_owner.uid == 4321 && changed_owner.gid == 2345,
          "chown must update only requested identity fields");
    const struct timespec timestamps[2] = {{123456789, 0}, {123456790, 0}};
    filesystem.utimens("rollout-a", file_a, timestamps);
    const auto changed_times = filesystem.stat_inode("rollout-a", file_a);
    check(changed_times.atime != owned_file.atime && changed_times.mtime != owned_file.mtime,
          "utimens must update file timestamps");
    const std::string payload_a = "accepted trajectory";
    filesystem.write(
        "rollout-a",
        file_a,
        0,
        IntervalBlob(payload_a.begin(), payload_a.end()));
    auto read_a = filesystem.read("rollout-a", file_a, 0, 1024);
    check(std::string(read_a.begin(), read_a.end()) == payload_a, "branch must read its write");

    rejects_errno(
        [&] { filesystem.lookup_child("main", 1, "result.txt"); },
        ENOENT,
        "parent must not see branch file");
    rejects_errno(
        [&] { filesystem.lookup_child("rollout-b", 1, "result.txt"); },
        ENOENT,
        "sibling must not see branch file");

    const auto dir = filesystem.create_directory(
        "rollout-a", 1, "artifacts", 0755, 3456, 4567);
    const auto owned_directory = filesystem.stat_inode("rollout-a", dir);
    check(owned_directory.uid == 3456 && owned_directory.gid == 4567,
          "directory creation must preserve the caller's identity");
    const auto link = filesystem.create_symlink(
        "rollout-a", dir, "latest", "../result.txt", 5678, 6789);
    const auto owned_link = filesystem.stat_inode("rollout-a", link);
    check(owned_link.uid == 5678 && owned_link.gid == 6789,
          "symlink creation must preserve the caller's identity");
    check(owned_link.symlink_target == "../result.txt", "symlink target must persist");
    auto names = filesystem.list_directory("rollout-a", 1);
    check(names.size() == 2, "branch root must contain file and directory");

    filesystem.rename("rollout-a", 1, "result.txt", dir, "final.txt");
    const auto moved = filesystem.lookup_child("rollout-a", dir, "final.txt");
    filesystem.truncate("rollout-a", moved.id, 8);
    auto truncated = filesystem.read("rollout-a", moved.id, 0, 1024);
    check(std::string(truncated.begin(), truncated.end()) == "accepted", "truncate must be branch local");

    filesystem.unlink("rollout-a", dir, "latest", false);
    filesystem.unlink("rollout-a", dir, "final.txt", false);
    filesystem.unlink("rollout-a", 1, "artifacts", true);
    check(filesystem.list_directory("rollout-a", 1).empty(), "unlink sequence must remove tree");

    std::cout << "all native filesystem engine tests passed\n";
    return 0;
}
