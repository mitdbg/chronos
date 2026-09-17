#pragma once

#include "native_types.hpp"

#include <cstdint>
#include <ctime>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <pybind11/pybind11.h>

namespace chronos::native {

struct ChronosFSInode {
    std::int64_t id = 0;
    std::string kind;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::int64_t size = 0;
    std::uint32_t nlink = 0;
    std::string symlink_target;
    std::string atime;
    std::string mtime;
    std::string ctime;
};

class ChronosFSError : public std::runtime_error {
  public:
    ChronosFSError(int error_code, const std::string &message)
        : std::runtime_error(message), error_code_(error_code) {}

    int error_code() const noexcept { return error_code_; }

  private:
    int error_code_;
};

// Plain C++ access to the branch-aware filesystem engine. FUSE, Python, and
// NFS are protocol adapters around this API; none of them owns filesystem
// versioning semantics.
class NativeChronosFilesystem {
  public:
    NativeChronosFilesystem(
        std::string database_url,
        std::string metadata_url,
        std::int64_t block_size);
    ~NativeChronosFilesystem();
    NativeChronosFilesystem(NativeChronosFilesystem &&) noexcept;
    NativeChronosFilesystem &operator=(NativeChronosFilesystem &&) noexcept;
    NativeChronosFilesystem(const NativeChronosFilesystem &) = delete;
    NativeChronosFilesystem &operator=(const NativeChronosFilesystem &) = delete;

    void ensure();
    void create_branch(const std::string &branch, const std::string &from_branch);
    void delete_branch(const std::string &branch);
    ChronosFSInode stat_inode(const std::string &branch, std::int64_t inode_id);
    ChronosFSInode lookup_child(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name);
    std::vector<std::string> list_directory(
        const std::string &branch,
        std::int64_t inode_id);
    IntervalBlob read(
        const std::string &branch,
        std::int64_t inode_id,
        std::int64_t offset,
        std::int64_t size);
    void write(
        const std::string &branch,
        std::int64_t inode_id,
        std::int64_t offset,
        const IntervalBlob &data);
    void truncate(
        const std::string &branch,
        std::int64_t inode_id,
        std::int64_t size);
    void chmod(
        const std::string &branch,
        std::int64_t inode_id,
        std::uint32_t mode);
    void chown(
        const std::string &branch,
        std::int64_t inode_id,
        std::uint32_t uid,
        std::uint32_t gid);
    void utimens(
        const std::string &branch,
        std::int64_t inode_id,
        const struct timespec times[2]);
    std::int64_t create_file(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name,
        std::uint32_t mode);
    std::int64_t create_file(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name,
        std::uint32_t mode,
        std::uint32_t uid,
        std::uint32_t gid);
    std::int64_t create_directory(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name,
        std::uint32_t mode);
    std::int64_t create_directory(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name,
        std::uint32_t mode,
        std::uint32_t uid,
        std::uint32_t gid);
    std::int64_t create_symlink(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name,
        const std::string &target);
    std::int64_t create_symlink(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name,
        const std::string &target,
        std::uint32_t uid,
        std::uint32_t gid);
    void unlink(
        const std::string &branch,
        std::int64_t parent_inode_id,
        const std::string &name,
        bool directory);
    void rename(
        const std::string &branch,
        std::int64_t old_parent_inode_id,
        const std::string &old_name,
        std::int64_t new_parent_inode_id,
        const std::string &new_name);
    void begin_write_batch(const std::string &branch);
    void commit_write_batch(const std::string &branch);
    void rollback_write_batch(const std::string &branch);
    void refresh(const std::string &branch);

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

void bind_chronosfs_fuse(pybind11::module_ &m);

} // namespace chronos::native
