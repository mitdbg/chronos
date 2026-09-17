#include "chronos/gateway/config.hpp"
#include "chronos/gateway/control_server.hpp"
#include "chronos/gateway/postgres_server.hpp"
#include "chronos/gateway/nfs_server.hpp"
#include "chronos/gateway/runtime.hpp"

#include <csignal>
#include <iostream>
#include <memory>
#include <atomic>
#include <chrono>
#include <exception>
#include <mutex>
#include <string>
#include <thread>

namespace {

void usage(const char *program) {
    std::cerr << "usage: " << program << " --config PATH\n"
              << "       " << program << " --check\n";
}

volatile std::sig_atomic_t stop_requested = 0;

void stop_server(int) {
    stop_requested = 1;
}

} // namespace

int main(int argc, char **argv) {
    if (argc == 2 && std::string(argv[1]) == "--check") {
        try {
            chronos::gateway::AccessGrantRegistry registry;
            chronos::gateway::AccessGrantSpec spec;
            spec.workspace = "healthcheck";
            spec.branch = "healthcheck";
            spec.postgres_stores.insert("postgres");
            auto issued = registry.create(spec);
            registry.activate(issued.grant.id);
            registry.authorize_postgres(issued.grant.id, issued.credential, "postgres");
            std::cout << "chronos-gateway: configuration and crypto checks passed\n";
            return 0;
        } catch (const std::exception &error) {
            std::cerr << "chronos-gateway: check failed: " << error.what() << '\n';
            return 1;
        }
    }
    if (argc == 3 && std::string(argv[1]) == "--config") {
        try {
            auto config = chronos::gateway::load_config(argv[2]);
            chronos::gateway::GatewayRuntime runtime(config.recovery_window);
            for (const auto &[name, workspace] : config.workspaces) {
                runtime.add_workspace(name, workspace);
            }
            chronos::gateway::ControlServer server(
                runtime,
                config.control,
                config.postgres,
                config.nfs,
                config.controller_token);
            chronos::gateway::PostgresServer postgres_server(runtime, config.postgres);
            chronos::gateway::NfsServer nfs_server(
                runtime, config.nfs, config.nfs_runtime);
            std::signal(SIGINT, stop_server);
            std::signal(SIGTERM, stop_server);
            std::mutex failure_mutex;
            std::atomic<bool> service_failed{false};
            std::exception_ptr failure;
            auto run = [&](auto &service) {
                try {
                    service.run();
                } catch (...) {
                    {
                        std::lock_guard lock(failure_mutex);
                        if (!failure) failure = std::current_exception();
                    }
                    service_failed.store(true);
                }
            };
            std::thread control_thread([&] { run(server); });
            std::thread postgres_thread([&] { run(postgres_server); });
            std::thread nfs_thread([&] { run(nfs_server); });
            while (!stop_requested && !service_failed.load()) {
                runtime.expire_due();
                runtime.reclaim_due();
                for (int tick = 0; tick < 20 && !stop_requested &&
                     !service_failed.load(); ++tick) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
            server.stop();
            postgres_server.stop();
            nfs_server.stop();
            control_thread.join();
            postgres_thread.join();
            nfs_thread.join();
            if (failure) std::rethrow_exception(failure);
            return 0;
        } catch (const std::exception &error) {
            std::cerr << "chronos-gateway: " << error.what() << '\n';
            return 1;
        }
    }
    usage(argv[0]);
    return 2;
}
