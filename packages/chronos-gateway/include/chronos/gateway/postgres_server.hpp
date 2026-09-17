#pragma once

#include "chronos/gateway/config.hpp"
#include "chronos/gateway/runtime.hpp"

#include <memory>

namespace chronos::gateway {

// PostgreSQL wire-protocol frontend for a branch grant.  The startup database
// is a configured store alias, the startup user is the grant id, and the
// cleartext password is the per-grant secret.  Each accepted connection owns
// one branch-bound NativeBranchSession.
class PostgresServer {
  public:
    PostgresServer(GatewayRuntime &runtime, ListenAddress listen);
    ~PostgresServer();
    PostgresServer(const PostgresServer &) = delete;
    PostgresServer &operator=(const PostgresServer &) = delete;

    void run();
    void stop();

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chronos::gateway
