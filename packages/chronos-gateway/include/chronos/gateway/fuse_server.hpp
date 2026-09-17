#pragma once

#include "chronos/gateway/filesystem_view.hpp"

#include <memory>
#include <string>
#include <vector>

namespace chronos::gateway {

// Local filesystem frontend used as the backing filesystem for the embedded
// NFS-Ganesha VFS adapter. It exposes one non-enumerable directory per grant
// capability, all from the same mount and process.
class FuseServer {
  public:
    explicit FuseServer(GatewayRuntime &runtime);
    ~FuseServer();
    FuseServer(const FuseServer &) = delete;
    FuseServer &operator=(const FuseServer &) = delete;

    int run(const std::string &mountpoint, const std::vector<std::string> &options = {});
    void stop();

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chronos::gateway
