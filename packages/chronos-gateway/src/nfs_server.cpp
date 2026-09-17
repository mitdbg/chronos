#include "chronos/gateway/nfs_server.hpp"

#include <dlfcn.h>
#include <sys/vfs.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <utility>

namespace chronos::gateway {
namespace {

constexpr long fuse_super_magic = 0x65735546;

std::string quoted(const std::string &value) {
    std::string result = "\"";
    for (char character : value) {
        if (character == '\\' || character == '\"') result.push_back('\\');
        result.push_back(character);
    }
    result.push_back('\"');
    return result;
}

} // namespace

class NfsServer::Impl {
  public:
    using Main = int (*)(const char *, const char *, int);
    using Halt = void (*)();

    Impl(GatewayRuntime &runtime, ListenAddress listen, GatewayConfig::NfsRuntime config)
        : fuse(runtime), listen(std::move(listen)), config(std::move(config)) {}

    ~Impl() {
        stop();
        if (fuse_thread.joinable()) fuse_thread.join();
        // Ganesha owns process-global registries and worker teardown. Keep the
        // module loaded until process exit rather than invalidating late
        // callbacks with dlclose().
    }

    void run() {
        std::filesystem::create_directories(config.state_directory);
        std::filesystem::create_directories(config.mountpoint);
        const auto config_path = config.state_directory + "/ganesha.conf";
        const auto log_path = config.state_directory + "/ganesha.log";
        {
            std::ofstream output(config_path, std::ios::trunc);
            if (!output) throw GatewayError("cannot write NFS-Ganesha configuration");
            output << NfsServer::render_ganesha_config(listen, config);
            if (!output) throw GatewayError("cannot finish NFS-Ganesha configuration");
        }

        fuse_result.store(-1);
        fuse_thread = std::thread([this] {
            fuse_result.store(fuse.run(config.mountpoint,
                {"attr_timeout=0", "entry_timeout=0", "negative_timeout=0",
                 "noforget", "allow_other"}));
        });
        bool mounted = false;
        for (int attempt = 0; attempt < 250 && !stopping.load(); ++attempt) {
            struct statfs status{};
            if (statfs(config.mountpoint.c_str(), &status) == 0 &&
                status.f_type == fuse_super_magic) {
                mounted = true;
                break;
            }
            if (fuse_result.load() != -1) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        if (!mounted) {
            fuse.stop();
            if (fuse_thread.joinable()) fuse_thread.join();
            throw GatewayError("ChronosFS backing mount did not start");
        }

        library = dlopen(config.ganesha_library.c_str(), RTLD_NOW | RTLD_GLOBAL);
        if (!library) {
            const char *loader_error = dlerror();
            const std::string error = loader_error ? loader_error : "unknown loader error";
            fuse.stop();
            fuse_thread.join();
            throw GatewayError("cannot load NFS-Ganesha: " + error);
        }
        main = reinterpret_cast<Main>(dlsym(library, "nfs_libmain"));
        halt = reinterpret_cast<Halt>(dlsym(library, "admin_halt"));
        if (!main || !halt) {
            fuse.stop();
            fuse_thread.join();
            throw GatewayError("NFS-Ganesha library lacks nfs_libmain or admin_halt");
        }
        running_ganesha.store(true);
        const int result = main(config_path.c_str(), log_path.c_str(), 5);
        running_ganesha.store(false);
        fuse.stop();
        fuse_thread.join();
        if (!stopping.load() && result != 0) {
            throw GatewayError("NFS-Ganesha stopped with status " + std::to_string(result));
        }
    }

    void stop() {
        if (stopping.exchange(true)) return;
        // Let Ganesha close its VFS handles before tearing down the backing
        // mount. run() stops FUSE after nfs_libmain has completed shutdown.
        if (running_ganesha.load() && halt) halt();
        else fuse.stop();
    }

    FuseServer fuse;
    ListenAddress listen;
    GatewayConfig::NfsRuntime config;
    std::thread fuse_thread;
    std::atomic<int> fuse_result{-1};
    std::atomic<bool> stopping{false};
    std::atomic<bool> running_ganesha{false};
    void *library = nullptr;
    Main main = nullptr;
    Halt halt = nullptr;
};

NfsServer::NfsServer(
    GatewayRuntime &runtime,
    ListenAddress listen,
    GatewayConfig::NfsRuntime config)
    : impl_(std::make_unique<Impl>(runtime, std::move(listen), std::move(config))) {}
NfsServer::~NfsServer() = default;
void NfsServer::run() { impl_->run(); }
void NfsServer::stop() { impl_->stop(); }

std::string NfsServer::render_ganesha_config(
    const ListenAddress &listen,
    const GatewayConfig::NfsRuntime &config) {
    std::ostringstream out;
    out << "NFS_CORE_PARAM {\n"
        << "  NFS_Port = " << listen.port << ";\n"
        << "  Bind_addr = " << listen.host << ";\n"
        << "  Plugins_Dir = " << quoted(config.plugins_directory) << ";\n"
        << "  Protocols = 4;\n"
        << "  Enable_NLM = false;\n"
        << "  Enable_RQUOTA = false;\n"
        << "  Enable_UDP = false;\n"
        << "  mount_path_pseudo = true;\n"
        << "}\n"
        << "NFSV4 {\n"
        << "  Graceless = true;\n"
        << "  Only_Numeric_Owners = true;\n"
        << "}\n"
        << "EXPORT {\n"
        << "  Export_Id = 1;\n"
        << "  Path = " << quoted(config.mountpoint) << ";\n"
        << "  Pseudo = \"/\";\n"
        << "  Access_Type = RW;\n"
        << "  Squash = No_Root_Squash;\n"
        << "  SecType = sys;\n"
        << "  Protocols = 4;\n"
        << "  Transports = TCP;\n"
        << "  FSAL { Name = VFS; }\n"
        << "}\n";
    return out.str();
}

} // namespace chronos::gateway
