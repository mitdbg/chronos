#pragma once

#include "chronos/gateway/runtime.hpp"

#include <cstdint>
#include <ctime>
#include <string>
#include <vector>

namespace chronos::gateway {

// Filesystem namespace presented to NFS-Ganesha.  The first path component is
// an opaque grant capability.  It is deliberately absent from root readdir,
// so clients can use only a capability delivered by the control plane.
class FilesystemView {
  public:
    explicit FilesystemView(GatewayRuntime &runtime) : runtime_(runtime) {}

    chronos::native::ChronosFSInode stat(const std::string &path);
    std::vector<std::string> list(const std::string &path);
    chronos::native::IntervalBlob read(
        const std::string &path, std::int64_t offset, std::int64_t size);
    void write(
        const std::string &path,
        std::int64_t offset,
        const chronos::native::IntervalBlob &data);
    void truncate(const std::string &path, std::int64_t size);
    void chmod(const std::string &path, std::uint32_t mode);
    void chown(
        const std::string &path, std::uint32_t uid, std::uint32_t gid);
    void utimens(const std::string &path, const struct timespec times[2]);
    void create_file(const std::string &path, std::uint32_t mode);
    void create_file(
        const std::string &path,
        std::uint32_t mode,
        std::uint32_t uid,
        std::uint32_t gid);
    void create_directory(const std::string &path, std::uint32_t mode);
    void create_directory(
        const std::string &path,
        std::uint32_t mode,
        std::uint32_t uid,
        std::uint32_t gid);
    void create_symlink(const std::string &path, const std::string &target);
    void create_symlink(
        const std::string &path,
        const std::string &target,
        std::uint32_t uid,
        std::uint32_t gid);
    std::string read_symlink(const std::string &path);
    void unlink(const std::string &path, bool directory);
    void rename(const std::string &old_path, const std::string &new_path);

  private:
    struct Resolved {
        chronos::native::NativeChronosFilesystem *filesystem = nullptr;
        AccessGrant grant;
        std::int64_t inode = 0;
        std::string export_id;
    };
    struct Parent {
        Resolved directory;
        std::string name;
    };

    Resolved resolve(const std::string &path, bool write);
    Parent resolve_parent(const std::string &path, bool write);
    static std::vector<std::string> components(const std::string &path);

    GatewayRuntime &runtime_;
};

} // namespace chronos::gateway
