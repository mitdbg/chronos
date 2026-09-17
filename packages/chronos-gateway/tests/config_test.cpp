#include "chronos/gateway/config.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

using chronos::gateway::GatewayError;
using chronos::gateway::load_config;

namespace {

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

template <typename Function>
void rejects(Function &&function, const char *message) {
    try { function(); } catch (const std::exception &) { return; }
    check(false, message);
}

struct TempDirectory {
    TempDirectory() {
        std::string pattern = "/tmp/chronos-gateway-config.XXXXXX";
        pattern.push_back('\0');
        path = mkdtemp(pattern.data());
        if (path.empty()) throw std::runtime_error("mkdtemp failed");
    }
    ~TempDirectory() { std::filesystem::remove_all(path); }
    std::string path;
};

void write_config(const std::string &path, const std::string &control) {
    std::ofstream output(path);
    output << R"JSON({
  "listeners": {
    "control": ")JSON" << control << R"JSON(",
    "postgres": "127.0.0.1:15432",
    "nfs": "127.0.0.1:12049"
  },
  "controller_token_env": "TEST_GATEWAY_TOKEN",
  "recovery_seconds": 45,
  "nfs": {
    "ganesha_library": "/usr/lib/ganesha/libganesha_nfsd.so",
    "plugins_directory": "/usr/lib/x86_64-linux-gnu/ganesha",
    "state_directory": "/tmp/chronos-test-nfs"
  },
  "workspaces": {
    "training": {
      "metadata_url_env": "TEST_METADATA_URL",
      "filesystem": {"data_url_env": "TEST_FILES_URL", "block_size": 4096},
      "postgres": {"orders": {"data_url_env": "TEST_ORDERS_URL"}}
    }
  }
})JSON";
}

} // namespace

int main() {
    TempDirectory temp;
    setenv("TEST_GATEWAY_TOKEN", "controller-secret", 1);
    setenv("TEST_METADATA_URL", "sqlite:///metadata.sqlite", 1);
    setenv("TEST_FILES_URL", "sqlite:///files.sqlite", 1);
    setenv("TEST_ORDERS_URL", "postgresql://orders", 1);
    const auto path = temp.path + "/gateway.json";
    write_config(path, "127.0.0.1:17400");
    auto config = load_config(path);
    check(config.control.port == 17400, "control listener must parse");
    check(config.recovery_window == std::chrono::seconds(45), "recovery window must parse");
    check(config.nfs_runtime.mountpoint == "/tmp/chronos-test-nfs/mount",
          "default NFS mountpoint must derive from its state directory");
    check(config.workspaces.at("training").postgres_stores.at("orders").metadata_url ==
              "sqlite:///metadata.sqlite",
          "workspace metadata URL must apply to database stores");

    write_config(path, "0.0.0.0:17400");
    rejects([&] { load_config(path); }, "plaintext wildcard listeners must fail closed");
    unsetenv("TEST_GATEWAY_TOKEN");
    write_config(path, "127.0.0.1:17400");
    rejects([&] { load_config(path); }, "missing controller token must fail startup");
    std::cout << "all gateway configuration tests passed\n";
    return 0;
}
