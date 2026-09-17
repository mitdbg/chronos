#include "chronos/gateway/filesystem_view.hpp"

#include <algorithm>
#include <unistd.h>

namespace chronos::gateway {

std::vector<std::string> FilesystemView::components(const std::string &path) {
    if (path.empty() || path.front() != '/') throw GatewayError("filesystem path must be absolute");
    std::vector<std::string> result;
    std::size_t start = 1;
    while (start < path.size()) {
        const auto end = path.find('/', start);
        auto component = path.substr(start, end == std::string::npos
            ? std::string::npos : end - start);
        if (!component.empty()) {
            if (component == "." || component == "..") {
                throw GatewayError("filesystem path traversal is not allowed");
            }
            if (component.find('\0') != std::string::npos) {
                throw GatewayError("filesystem path contains a null byte");
            }
            result.push_back(std::move(component));
        }
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return result;
}

FilesystemView::Resolved FilesystemView::resolve(const std::string &path, bool write) {
    const auto parts = components(path);
    if (parts.empty()) throw GatewayError("the capability root has no backing inode");
    AccessGrant grant;
    auto &filesystem = runtime_.authorize_filesystem_capability(parts.front(), write, &grant);
    std::int64_t inode = 1;
    for (std::size_t index = 1; index < parts.size(); ++index) {
        inode = filesystem.lookup_child(grant.branch, inode, parts[index]).id;
    }
    return {&filesystem, std::move(grant), inode, parts.front()};
}

FilesystemView::Parent FilesystemView::resolve_parent(
    const std::string &path, bool write) {
    if (path.size() > 1 && path.back() == '/') {
        throw GatewayError("mutation path cannot end with a slash");
    }
    auto parts = components(path);
    if (parts.size() < 2) throw GatewayError("cannot mutate the capability root");
    const auto name = parts.back();
    const auto separator = path.find_last_of('/');
    const auto parent_path = separator == 0 ? "/" : path.substr(0, separator);
    return {resolve(parent_path, write), name};
}

chronos::native::ChronosFSInode FilesystemView::stat(const std::string &path) {
    if (components(path).empty()) {
        chronos::native::ChronosFSInode root;
        root.id = 0;
        root.kind = "directory";
        root.mode = 0555;
        root.nlink = 2;
        return root;
    }
    auto resolved = resolve(path, false);
    return resolved.filesystem->stat_inode(resolved.grant.branch, resolved.inode);
}

std::vector<std::string> FilesystemView::list(const std::string &path) {
    if (components(path).empty()) return {".", ".."};
    auto resolved = resolve(path, false);
    auto names = resolved.filesystem->list_directory(resolved.grant.branch, resolved.inode);
    names.insert(names.begin(), {".", ".."});
    return names;
}

chronos::native::IntervalBlob FilesystemView::read(
    const std::string &path, std::int64_t offset, std::int64_t size) {
    auto resolved = resolve(path, false);
    return resolved.filesystem->read(resolved.grant.branch, resolved.inode, offset, size);
}

void FilesystemView::write(
    const std::string &path,
    std::int64_t offset,
    const chronos::native::IntervalBlob &data) {
    auto resolved = resolve(path, true);
    resolved.filesystem->write(resolved.grant.branch, resolved.inode, offset, data);
}

void FilesystemView::truncate(const std::string &path, std::int64_t size) {
    auto resolved = resolve(path, true);
    resolved.filesystem->truncate(resolved.grant.branch, resolved.inode, size);
}

void FilesystemView::chmod(const std::string &path, std::uint32_t mode) {
    auto resolved = resolve(path, true);
    resolved.filesystem->chmod(resolved.grant.branch, resolved.inode, mode);
}

void FilesystemView::chown(
    const std::string &path, std::uint32_t uid, std::uint32_t gid) {
    auto resolved = resolve(path, true);
    resolved.filesystem->chown(resolved.grant.branch, resolved.inode, uid, gid);
}

void FilesystemView::utimens(
    const std::string &path, const struct timespec times[2]) {
    auto resolved = resolve(path, true);
    resolved.filesystem->utimens(resolved.grant.branch, resolved.inode, times);
}

void FilesystemView::create_file(const std::string &path, std::uint32_t mode) {
    create_file(path, mode, ::getuid(), ::getgid());
}

void FilesystemView::create_file(
    const std::string &path,
    std::uint32_t mode,
    std::uint32_t uid,
    std::uint32_t gid) {
    auto parent = resolve_parent(path, true);
    parent.directory.filesystem->create_file(
        parent.directory.grant.branch,
        parent.directory.inode,
        parent.name,
        mode,
        uid,
        gid);
}

void FilesystemView::create_directory(const std::string &path, std::uint32_t mode) {
    create_directory(path, mode, ::getuid(), ::getgid());
}

void FilesystemView::create_directory(
    const std::string &path,
    std::uint32_t mode,
    std::uint32_t uid,
    std::uint32_t gid) {
    auto parent = resolve_parent(path, true);
    parent.directory.filesystem->create_directory(
        parent.directory.grant.branch,
        parent.directory.inode,
        parent.name,
        mode,
        uid,
        gid);
}

void FilesystemView::create_symlink(
    const std::string &path, const std::string &target) {
    create_symlink(path, target, ::getuid(), ::getgid());
}

void FilesystemView::create_symlink(
    const std::string &path,
    const std::string &target,
    std::uint32_t uid,
    std::uint32_t gid) {
    auto parent = resolve_parent(path, true);
    parent.directory.filesystem->create_symlink(
        parent.directory.grant.branch,
        parent.directory.inode,
        parent.name,
        target,
        uid,
        gid);
}

std::string FilesystemView::read_symlink(const std::string &path) {
    auto inode = stat(path);
    if (inode.kind != "symlink") throw GatewayError("path is not a symbolic link");
    return inode.symlink_target;
}

void FilesystemView::unlink(const std::string &path, bool directory) {
    auto parent = resolve_parent(path, true);
    parent.directory.filesystem->unlink(
        parent.directory.grant.branch, parent.directory.inode, parent.name, directory);
}

void FilesystemView::rename(
    const std::string &old_path, const std::string &new_path) {
    auto old_parent = resolve_parent(old_path, true);
    auto new_parent = resolve_parent(new_path, true);
    if (old_parent.directory.export_id != new_parent.directory.export_id) {
        throw GatewayError("cannot rename across filesystem capabilities");
    }
    old_parent.directory.filesystem->rename(
        old_parent.directory.grant.branch,
        old_parent.directory.inode,
        old_parent.name,
        new_parent.directory.inode,
        new_parent.name);
}

} // namespace chronos::gateway
