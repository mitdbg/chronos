#include "chronos/gateway/config.hpp"

#include <arpa/inet.h>
#include <nlohmann/json.hpp>

#include <cstdlib>
#include <fstream>
#include <limits>
#include <sstream>
#include <utility>
#include <vector>

namespace chronos::gateway {
namespace {

using Json = nlohmann::json;

std::string required_env(const std::string &name) {
    if (name.empty()) throw GatewayError("environment variable name is empty");
    const char *value = std::getenv(name.c_str());
    if (!value || !*value) throw GatewayError("required environment variable is not set: " + name);
    return value;
}

ListenAddress parse_address(const std::string &text, const std::string &label) {
    const auto separator = text.rfind(':');
    if (separator == std::string::npos || separator == 0 || separator + 1 == text.size()) {
        throw GatewayError(label + " must have the form IP:port");
    }
    ListenAddress result;
    result.host = text.substr(0, separator);
    if (result.host.front() == '[' && result.host.back() == ']') {
        result.host = result.host.substr(1, result.host.size() - 2);
    }
    in_addr ipv4{};
    in6_addr ipv6{};
    if (inet_pton(AF_INET, result.host.c_str(), &ipv4) != 1 &&
        inet_pton(AF_INET6, result.host.c_str(), &ipv6) != 1) {
        throw GatewayError(label + " host must be an IP literal");
    }
    if (result.host == "0.0.0.0" || result.host == "::") {
        throw GatewayError(label + " cannot use a wildcard address without TLS");
    }
    unsigned long port = 0;
    try {
        std::size_t consumed = 0;
        port = std::stoul(text.substr(separator + 1), &consumed);
        if (consumed != text.size() - separator - 1) throw GatewayError(label + " has an invalid port");
    } catch (const std::exception &) {
        throw GatewayError(label + " has an invalid port");
    }
    if (port == 0 || port > std::numeric_limits<std::uint16_t>::max()) {
        throw GatewayError(label + " port is out of range");
    }
    result.port = static_cast<std::uint16_t>(port);
    return result;
}

StoreConfig store_config(const Json &value, const std::string &context) {
    if (!value.is_object()) throw GatewayError(context + " must be an object");
    StoreConfig result;
    result.data_url = required_env(value.at("data_url_env").get<std::string>());
    if (value.contains("metadata_url_env")) {
        result.metadata_url = required_env(value.at("metadata_url_env").get<std::string>());
    }
    return result;
}

} // namespace

std::string ListenAddress::endpoint() const {
    if (host.find(':') != std::string::npos) return "[" + host + "]:" + std::to_string(port);
    return host + ":" + std::to_string(port);
}

GatewayConfig load_config(const std::string &path) {
    std::ifstream input(path);
    if (!input) throw GatewayError("could not open gateway configuration: " + path);
    Json root;
    try {
        input >> root;
    } catch (const std::exception &error) {
        throw GatewayError("invalid gateway JSON: " + std::string(error.what()));
    }

    GatewayConfig config;
    const auto &listeners = root.at("listeners");
    config.control = parse_address(listeners.at("control").get<std::string>(), "control listener");
    config.postgres = parse_address(listeners.at("postgres").get<std::string>(), "postgres listener");
    config.nfs = parse_address(listeners.at("nfs").get<std::string>(), "nfs listener");
    const auto &nfs = root.at("nfs");
    config.nfs_runtime.ganesha_library = nfs.at("ganesha_library").get<std::string>();
    config.nfs_runtime.plugins_directory = nfs.at("plugins_directory").get<std::string>();
    config.nfs_runtime.state_directory = nfs.at("state_directory").get<std::string>();
    config.nfs_runtime.mountpoint = nfs.value(
        "mountpoint", config.nfs_runtime.state_directory + "/mount");
    for (const auto &[label, value] : std::vector<std::pair<std::string, std::string>>{
             {"ganesha_library", config.nfs_runtime.ganesha_library},
             {"plugins_directory", config.nfs_runtime.plugins_directory},
             {"state_directory", config.nfs_runtime.state_directory},
             {"mountpoint", config.nfs_runtime.mountpoint}}) {
        if (value.empty() || value.find_first_of("\r\n{};") != std::string::npos) {
            throw GatewayError("nfs " + label + " is empty or contains unsafe characters");
        }
    }
    config.controller_token = required_env(
        root.value("controller_token_env", std::string("CHRONOS_CONTROLLER_TOKEN")));
    const auto recovery = root.value("recovery_seconds", 600LL);
    if (recovery < 0) throw GatewayError("recovery_seconds cannot be negative");
    config.recovery_window = std::chrono::seconds(recovery);

    const auto &workspaces = root.at("workspaces");
    if (!workspaces.is_object() || workspaces.empty()) {
        throw GatewayError("at least one workspace is required");
    }
    for (auto workspace = workspaces.begin(); workspace != workspaces.end(); ++workspace) {
        WorkspaceConfig parsed;
        const auto shared_metadata_env = workspace.value().value(
            "metadata_url_env", std::string{});
        const std::string shared_metadata = shared_metadata_env.empty()
            ? std::string{}
            : required_env(shared_metadata_env);
        if (workspace.value().contains("postgres")) {
            for (auto store = workspace.value().at("postgres").begin();
                 store != workspace.value().at("postgres").end();
                 ++store) {
                auto parsed_store = store_config(store.value(), "postgres store " + store.key());
                if (parsed_store.metadata_url.empty()) parsed_store.metadata_url = shared_metadata;
                parsed.postgres_stores.emplace(store.key(), std::move(parsed_store));
            }
        }
        if (workspace.value().contains("filesystem")) {
            const auto &filesystem = workspace.value().at("filesystem");
            auto parsed_store = store_config(filesystem, "filesystem");
            FilesystemConfig fs;
            fs.data_url = std::move(parsed_store.data_url);
            fs.metadata_url = parsed_store.metadata_url.empty()
                ? shared_metadata
                : std::move(parsed_store.metadata_url);
            fs.block_size = filesystem.value("block_size", 1024 * 1024);
            parsed.filesystem = std::move(fs);
        }
        config.workspaces.emplace(workspace.key(), std::move(parsed));
    }
    return config;
}

} // namespace chronos::gateway
