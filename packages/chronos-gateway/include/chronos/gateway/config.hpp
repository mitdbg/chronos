#pragma once

#include "chronos/gateway/runtime.hpp"

#include <cstdint>
#include <string>
#include <unordered_map>

namespace chronos::gateway {

struct ListenAddress {
    std::string host;
    std::uint16_t port = 0;

    std::string endpoint() const;
};

struct GatewayConfig {
    ListenAddress control;
    ListenAddress postgres;
    ListenAddress nfs;
    struct NfsRuntime {
        std::string ganesha_library;
        std::string plugins_directory;
        std::string state_directory;
        std::string mountpoint;
    } nfs_runtime;
    std::string controller_token;
    std::chrono::seconds recovery_window{600};
    std::unordered_map<std::string, WorkspaceConfig> workspaces;
};

GatewayConfig load_config(const std::string &path);

} // namespace chronos::gateway
