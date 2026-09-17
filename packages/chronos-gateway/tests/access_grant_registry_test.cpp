#include "chronos/gateway/access_grant_registry.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using chronos::gateway::FilesystemAccess;
using chronos::gateway::GatewayError;
using chronos::gateway::AccessGrantRegistry;
using chronos::gateway::AccessGrantSpec;
using chronos::gateway::AccessGrantState;

namespace {

void check(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

template <typename Function>
void rejects(Function &&function, const char *message) {
    try {
        function();
    } catch (const GatewayError &) {
        return;
    }
    check(false, message);
}

AccessGrantSpec full_spec() {
    AccessGrantSpec spec;
    spec.workspace = "agent-training";
    spec.branch = "rollout-1";
    spec.postgres_stores = {"orders", "events"};
    spec.filesystem_access = FilesystemAccess::ReadWrite;
    spec.ttl = std::chrono::minutes(30);
    return spec;
}

} // namespace

int main() {
    auto now = std::chrono::system_clock::time_point{std::chrono::seconds(1000)};
    AccessGrantRegistry registry(std::chrono::minutes(10), [&] { return now; });

    rejects([&] { registry.create(AccessGrantSpec{}); }, "empty grant must fail");
    auto issued = registry.create(full_spec());
    check(issued.credential.size() == 64, "secret must contain 256 bits");
    check(issued.grant.id.size() == 32, "grant id must contain 128 bits");
    check(issued.grant.state == AccessGrantState::Provisioning, "new grant must provision");
    auto by_branch = registry.get_for_branch("agent-training", "rollout-1");
    check(by_branch && by_branch->id == issued.grant.id,
          "workspace branch must resolve its internal access grant");
    rejects([&] { registry.create(full_spec()); },
            "one workspace branch must not receive two active access grants");

    rejects(
        [&] { registry.authorize_postgres(issued.grant.id, issued.credential, "orders"); },
        "provisioning grant must not authorize");
    registry.activate(issued.grant.id);
    registry.authorize_postgres(issued.grant.id, issued.credential, "orders");
    rejects(
        [&] { registry.authorize_postgres(issued.grant.id, "wrong", "orders"); },
        "wrong secret must fail");
    rejects(
        [&] { registry.authorize_postgres(issued.grant.id, issued.credential, "admin"); },
        "unlisted database must fail");

    auto bound = registry.attach_sandbox(issued.grant.id, "e2b-123", "10.0.0.8");
    check(!bound.export_id.empty(), "filesystem binding must allocate export id");
    registry.attach_sandbox(issued.grant.id, "e2b-123", "10.0.0.8");
    registry.authorize_filesystem(bound.export_id, "10.0.0.8", true);
    registry.authorize_filesystem_capability(bound.export_id, true);
    rejects(
        [&] { registry.authorize_filesystem(bound.export_id, "10.0.0.9", false); },
        "wrong source ip must fail");
    rejects(
        [&] { registry.attach_sandbox(issued.grant.id, "e2b-456", "10.0.0.9"); },
        "grant rebinding must fail");

    auto read_only_spec = full_spec();
    read_only_spec.branch = "read-only";
    read_only_spec.filesystem_access = FilesystemAccess::ReadOnly;
    auto read_only = registry.create(read_only_spec);
    registry.activate(read_only.grant.id);
    auto read_only_bound = registry.attach_sandbox(read_only.grant.id, "e2b-ro", "10.0.0.8");
    registry.authorize_filesystem(read_only_bound.export_id, "10.0.0.8", false);
    registry.authorize_filesystem_capability(read_only_bound.export_id, false);
    rejects(
        [&] { registry.authorize_filesystem(read_only_bound.export_id, "10.0.0.8", true); },
        "read-only export must reject writes");
    rejects(
        [&] { registry.authorize_filesystem_capability(read_only_bound.export_id, true); },
        "read-only capability must reject writes");

    constexpr int thread_count = 16;
    constexpr int iterations = 500;
    std::atomic<int> authorized{0};
    std::vector<std::thread> threads;
    for (int index = 0; index < thread_count; ++index) {
        threads.emplace_back([&] {
            for (int iteration = 0; iteration < iterations; ++iteration) {
                registry.authorize_postgres(issued.grant.id, issued.credential, "events");
                ++authorized;
            }
        });
    }
    for (auto &thread : threads) thread.join();
    check(authorized == thread_count * iterations, "concurrent authorization lost calls");

    now += std::chrono::minutes(31);
    auto expired = registry.expire_due();
    check(expired.size() == 2, "both grants must expire");
    check(expired.front().generation == 2, "revocation must advance generation");
    rejects(
        [&] { registry.authorize_postgres(issued.grant.id, issued.credential, "orders"); },
        "expired SQL grant must fail");
    rejects(
        [&] { registry.authorize_filesystem(bound.export_id, "10.0.0.8", false); },
        "expired filesystem grant must fail");
    check(registry.due_for_reclamation().empty(), "reclamation grace must be honored");

    now += std::chrono::minutes(10);
    check(registry.due_for_reclamation().size() == 2, "expired grants must become reclaimable");
    registry.mark_deleted(issued.grant.id);
    auto deleted = registry.get(issued.grant.id);
    check(deleted && deleted->state == AccessGrantState::Deleted, "deleted state must persist");
    check(!registry.get_for_branch("agent-training", "rollout-1"),
          "deleted branch must release its public branch lookup");
    rejects(
        [&] { registry.authorize_filesystem(bound.export_id, "10.0.0.8", false); },
        "deleted export must not resolve");

    std::cout << "all grant registry tests passed\n";
    return 0;
}
