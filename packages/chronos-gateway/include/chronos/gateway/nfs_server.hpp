#pragma once

#include "chronos/gateway/config.hpp"
#include "chronos/gateway/fuse_server.hpp"

#include <memory>
#include <string>

namespace chronos::gateway {

// Runs NFS-Ganesha as a library in the gateway process. FSAL_VFS reads the
// local FuseServer mount; both adapters therefore share GatewayRuntime and no
// filesystem RPC is introduced between Ganesha and ChronosFS.
class NfsServer {
  public:
    NfsServer(
        GatewayRuntime &runtime,
        ListenAddress listen,
        GatewayConfig::NfsRuntime config);
    ~NfsServer();
    NfsServer(const NfsServer &) = delete;
    NfsServer &operator=(const NfsServer &) = delete;

    void run();
    void stop();
    static std::string render_ganesha_config(
        const ListenAddress &listen,
        const GatewayConfig::NfsRuntime &config);

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chronos::gateway
