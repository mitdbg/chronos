#include "chronos/gateway/nfs_server.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

using chronos::gateway::GatewayConfig;
using chronos::gateway::ListenAddress;
using chronos::gateway::NfsServer;

namespace {

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

} // namespace

int main() {
    GatewayConfig::NfsRuntime runtime;
    runtime.ganesha_library = "/usr/lib/ganesha/libganesha_nfsd.so";
    runtime.plugins_directory = "/usr/lib/ganesha";
    runtime.state_directory = "/var/lib/chronos-gateway/nfs";
    runtime.mountpoint = "/var/lib/chronos-gateway/nfs/mount";
    const auto config = NfsServer::render_ganesha_config(
        ListenAddress{"10.40.1.10", 2049}, runtime);
    check(config.find("NFS_Port = 2049;") != std::string::npos,
          "rendered configuration must select the requested port");
    check(config.find("Bind_addr = 10.40.1.10;") != std::string::npos,
          "rendered configuration must bind the private address");
    check(config.find("Protocols = 4;") != std::string::npos,
          "rendered configuration must disable legacy NFS protocols");
    check(config.find("Only_Numeric_Owners = true;") != std::string::npos,
          "NFSv4 must expose numeric owners without host-local id mapping");
    check(config.find("Path = \"/var/lib/chronos-gateway/nfs/mount\";") != std::string::npos,
          "VFS export must point at the in-process ChronosFS mount");
    check(config.find("Name = VFS;") != std::string::npos,
          "rendered export must use Ganesha's VFS adapter");
    std::cout << "all NFS-Ganesha configuration tests passed\n";
    return 0;
}
