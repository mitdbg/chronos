#define FUSE_USE_VERSION 35
#include "chronos/gateway/fuse_server.hpp"

#include <fuse3/fuse.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <mutex>
#include <pthread.h>
#include <signal.h>
#include <sys/stat.h>

namespace chronos::gateway {
namespace {

struct FuseState {
    explicit FuseState(GatewayRuntime &runtime) : view(runtime) {}
    FilesystemView view;
};

void wake_signal(int) {}

FuseState &state() {
    return *static_cast<FuseState *>(fuse_get_context()->private_data);
}

int translated_error() {
    try { throw; }
    catch (const chronos::native::ChronosFSError &error) { return -error.error_code(); }
    catch (const GatewayError &) { return -EACCES; }
    catch (...) { return -EIO; }
}

struct timespec parsed_time(const std::string &text) {
    struct timespec result{};
    std::tm broken_down{};
    if (text.empty() ||
        strptime(text.c_str(), "%Y-%m-%dT%H:%M:%SZ", &broken_down) == nullptr) {
        return result;
    }
    result.tv_sec = timegm(&broken_down);
    return result;
}

std::uint64_t stable_inode(const char *path, std::int64_t inode) {
    // FNV-1a over the unguessable export component plus the Chronos inode.
    // The export component prevents two branch roots from sharing st_ino.
    std::uint64_t value = 1469598103934665603ULL;
    const std::string full_path(path);
    const auto separator = full_path.find('/', 1);
    const std::string capability = full_path.substr(
        1, separator == std::string::npos ? std::string::npos : separator - 1);
    const std::string material = capability + ":" + std::to_string(inode);
    for (unsigned char byte : material) {
        value ^= byte;
        value *= 1099511628211ULL;
    }
    return value == 0 ? 1 : value;
}

void fill_stat(
    const char *path,
    const chronos::native::ChronosFSInode &inode,
    struct stat *result) {
    std::memset(result, 0, sizeof(*result));
    result->st_ino = static_cast<ino_t>(stable_inode(path, inode.id));
    result->st_uid = inode.uid;
    result->st_gid = inode.gid;
    result->st_nlink = inode.nlink ? inode.nlink : 1;
    result->st_size = inode.size;
    result->st_blksize = 4096;
    result->st_blocks = (inode.size + 511) / 512;
    mode_t type = S_IFREG;
    if (inode.kind == "directory") type = S_IFDIR;
    else if (inode.kind == "symlink") type = S_IFLNK;
    result->st_mode = type | (inode.mode & 07777);
    result->st_atim = parsed_time(inode.atime);
    result->st_mtim = parsed_time(inode.mtime);
    result->st_ctim = parsed_time(inode.ctime);
}

int op_getattr(const char *path, struct stat *result, struct fuse_file_info *) {
    try { fill_stat(path, state().view.stat(path), result); return 0; }
    catch (...) { return translated_error(); }
}

int op_readdir(const char *path, void *buffer, fuse_fill_dir_t filler,
               off_t, struct fuse_file_info *, enum fuse_readdir_flags) {
    try {
        for (const auto &name : state().view.list(path)) {
            if (filler(buffer, name.c_str(), nullptr, 0, FUSE_FILL_DIR_PLUS) != 0) break;
        }
        return 0;
    } catch (...) { return translated_error(); }
}

int op_open(const char *path, struct fuse_file_info *) {
    try {
        if (state().view.stat(path).kind != "file") return -EISDIR;
        return 0;
    } catch (...) { return translated_error(); }
}

int op_read(const char *path, char *buffer, size_t size, off_t offset,
            struct fuse_file_info *) {
    try {
        auto bytes = state().view.read(path, offset, size);
        if (!bytes.empty()) std::memcpy(buffer, bytes.data(), bytes.size());
        return static_cast<int>(bytes.size());
    } catch (...) { return translated_error(); }
}

int op_write(const char *path, const char *buffer, size_t size, off_t offset,
             struct fuse_file_info *) {
    try {
        state().view.write(path, offset,
            chronos::native::IntervalBlob(buffer, buffer + size));
        return static_cast<int>(size);
    } catch (...) { return translated_error(); }
}

int op_create(const char *path, mode_t mode, struct fuse_file_info *) {
    const auto *context = fuse_get_context();
    try { state().view.create_file(path, mode, context->uid, context->gid); return 0; }
    catch (...) { return translated_error(); }
}

int op_mkdir(const char *path, mode_t mode) {
    const auto *context = fuse_get_context();
    try { state().view.create_directory(path, mode, context->uid, context->gid); return 0; }
    catch (...) { return translated_error(); }
}

int op_unlink(const char *path) {
    try { state().view.unlink(path, false); return 0; }
    catch (...) { return translated_error(); }
}

int op_rmdir(const char *path) {
    try { state().view.unlink(path, true); return 0; }
    catch (...) { return translated_error(); }
}

int op_rename(const char *old_path, const char *new_path, unsigned int flags) {
    if (flags != 0) return -EINVAL;
    try { state().view.rename(old_path, new_path); return 0; }
    catch (...) { return translated_error(); }
}

int op_truncate(const char *path, off_t size, struct fuse_file_info *) {
    try { state().view.truncate(path, size); return 0; }
    catch (...) { return translated_error(); }
}

int op_chmod(const char *path, mode_t mode, struct fuse_file_info *) {
    try { state().view.chmod(path, mode); return 0; }
    catch (...) { return translated_error(); }
}

int op_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *) {
    try { state().view.chown(path, uid, gid); return 0; }
    catch (...) { return translated_error(); }
}

int op_utimens(
    const char *path, const struct timespec times[2], struct fuse_file_info *) {
    try { state().view.utimens(path, times); return 0; }
    catch (...) { return translated_error(); }
}

int op_symlink(const char *target, const char *path) {
    const auto *context = fuse_get_context();
    try { state().view.create_symlink(path, target, context->uid, context->gid); return 0; }
    catch (...) { return translated_error(); }
}

int op_readlink(const char *path, char *buffer, size_t size) {
    try {
        auto target = state().view.read_symlink(path);
        if (size > 0) {
            const auto count = std::min(size - 1, target.size());
            std::memcpy(buffer, target.data(), count);
            buffer[count] = '\0';
        }
        return 0;
    } catch (...) { return translated_error(); }
}

int op_access(const char *path, int) {
    try { state().view.stat(path); return 0; }
    catch (...) { return translated_error(); }
}

void *op_init(struct fuse_conn_info *connection, struct fuse_config *config) {
    if (connection->capable & FUSE_CAP_EXPORT_SUPPORT) {
        connection->want |= FUSE_CAP_EXPORT_SUPPORT;
    }
    config->use_ino = 1;
    return fuse_get_context()->private_data;
}

} // namespace

class FuseServer::Impl {
  public:
    explicit Impl(GatewayRuntime &runtime) : state(runtime) {}

    int run(const std::string &mountpoint, const std::vector<std::string> &options) {
        fuse_operations operations{};
        operations.getattr = op_getattr;
        operations.readdir = op_readdir;
        operations.open = op_open;
        operations.read = op_read;
        operations.write = op_write;
        operations.create = op_create;
        operations.mkdir = op_mkdir;
        operations.unlink = op_unlink;
        operations.rmdir = op_rmdir;
        operations.rename = op_rename;
        operations.truncate = op_truncate;
        operations.chmod = op_chmod;
        operations.chown = op_chown;
        operations.utimens = op_utimens;
        operations.symlink = op_symlink;
        operations.readlink = op_readlink;
        operations.access = op_access;
        operations.init = op_init;

        std::vector<std::string> arguments{"chronos-gateway-fuse", "-o"};
        std::string option_text = "fsname=chronos-gateway,default_permissions";
        for (const auto &option : options) option_text += "," + option;
        arguments.push_back(std::move(option_text));
        std::vector<char *> argv;
        for (auto &argument : arguments) argv.push_back(argument.data());
        fuse_args args = FUSE_ARGS_INIT(static_cast<int>(argv.size()), argv.data());
        auto *instance = fuse_new(&args, &operations, sizeof(operations), &state);
        if (!instance) { fuse_opt_free_args(&args); return 3; }
        fuse.store(instance);
        if (fuse_mount(instance, mountpoint.c_str()) != 0) {
            fuse.store(nullptr);
            fuse_destroy(instance);
            fuse_opt_free_args(&args);
            return 4;
        }
        mounted.store(true);
        struct sigaction action{};
        struct sigaction previous{};
        action.sa_handler = wake_signal;
        sigemptyset(&action.sa_mask);
        sigaction(SIGUSR1, &action, &previous);
        {
            std::lock_guard lock(loop_mutex);
            loop_thread = pthread_self();
            loop_thread_active = true;
        }
        // Ganesha supplies its own request concurrency. Keeping the local
        // backing mount single-threaded avoids libfuse worker shutdown races;
        // each Chronos operation remains short and independently committed.
        const int result = fuse_loop(instance);
        if (mounted.exchange(false)) fuse_unmount(instance);
        fuse.store(nullptr);
        {
            std::lock_guard lock(loop_mutex);
            loop_thread_active = false;
        }
        sigaction(SIGUSR1, &previous, nullptr);
        fuse_destroy(instance);
        fuse_opt_free_args(&args);
        return result == 0 ? 0 : 7;
    }

    void stop() {
        if (auto *instance = fuse.load()) {
            fuse_exit(instance);
            // libfuse before 3.18 only marks the session as exited; it does
            // not wake a loop blocked in read(/dev/fuse). A targeted no-op
            // signal produces EINTR and lets the loop observe the exit flag.
            std::lock_guard lock(loop_mutex);
            if (loop_thread_active) pthread_kill(loop_thread, SIGUSR1);
        }
    }

    FuseState state;
    std::atomic<struct fuse *> fuse{nullptr};
    std::atomic<bool> mounted{false};
    std::mutex loop_mutex;
    pthread_t loop_thread{};
    bool loop_thread_active = false;
};

FuseServer::FuseServer(GatewayRuntime &runtime)
    : impl_(std::make_unique<Impl>(runtime)) {}
FuseServer::~FuseServer() { stop(); }
int FuseServer::run(const std::string &mountpoint, const std::vector<std::string> &options) {
    return impl_->run(mountpoint, options);
}
void FuseServer::stop() { impl_->stop(); }

} // namespace chronos::gateway
