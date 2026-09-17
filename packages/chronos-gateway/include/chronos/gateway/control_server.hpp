#pragma once

#include "chronos/gateway/config.hpp"
#include "chronos/gateway/runtime.hpp"

#include <atomic>
#include <memory>
#include <string>

namespace chronos::gateway {

class ControlServer {
  public:
    ControlServer(
        GatewayRuntime &runtime,
        ListenAddress listen,
        ListenAddress postgres,
        ListenAddress nfs,
        std::string controller_token);
    ~ControlServer();
    ControlServer(const ControlServer &) = delete;
    ControlServer &operator=(const ControlServer &) = delete;

    void run();
    void stop();

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chronos::gateway
