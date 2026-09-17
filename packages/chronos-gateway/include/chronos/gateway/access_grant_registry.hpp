#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace chronos::gateway {

enum class FilesystemAccess {
    None,
    ReadOnly,
    ReadWrite,
};

enum class AccessGrantState {
    Provisioning,
    Active,
    Revoked,
    Deleted,
};

struct AccessGrantSpec {
    std::string workspace;
    std::string branch;
    std::unordered_set<std::string> postgres_stores;
    FilesystemAccess filesystem_access = FilesystemAccess::None;
    std::chrono::seconds ttl{3600};
};

struct AccessGrant {
    std::string id;
    std::string workspace;
    std::string branch;
    std::unordered_set<std::string> postgres_stores;
    FilesystemAccess filesystem_access = FilesystemAccess::None;
    AccessGrantState state = AccessGrantState::Provisioning;
    std::string sandbox_id;
    std::string sandbox_ip;
    std::string export_id;
    std::uint64_t generation = 1;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    std::optional<std::chrono::system_clock::time_point> reclaim_at;
};

struct IssuedAccessGrant {
    AccessGrant grant;
    std::string credential;
};

class GatewayError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

class AccessGrantRegistry {
  public:
    using Clock = std::function<std::chrono::system_clock::time_point()>;

    explicit AccessGrantRegistry(
        std::chrono::seconds recovery_window = std::chrono::minutes(10),
        Clock clock = [] { return std::chrono::system_clock::now(); });

    IssuedAccessGrant create(const AccessGrantSpec &spec);
    AccessGrant activate(const std::string &grant_id);
    AccessGrant attach_sandbox(
        const std::string &grant_id,
        const std::string &sandbox_id,
        const std::string &sandbox_ip);
    AccessGrant renew(const std::string &grant_id, std::chrono::seconds ttl);
    AccessGrant revoke(const std::string &grant_id);
    AccessGrant mark_deleted(const std::string &grant_id);
    std::optional<AccessGrant> get(const std::string &grant_id) const;
    std::optional<AccessGrant> get_for_branch(
        const std::string &workspace,
        const std::string &branch) const;

    AccessGrant authorize_postgres(
        const std::string &grant_id,
        const std::string &secret,
        const std::string &store_alias);
    AccessGrant authorize_filesystem(
        const std::string &export_id,
        const std::string &source_ip,
        bool write);
    // For an in-process protocol adapter whose upstream peer authentication
    // is already complete. The unguessable export id acts as the capability;
    // unlike authorize_filesystem(), no network identity is consulted.
    AccessGrant authorize_filesystem_capability(
        const std::string &export_id,
        bool write);

    std::vector<AccessGrant> expire_due();
    std::vector<AccessGrant> due_for_reclamation() const;

  private:
    struct StoredAccessGrant {
        AccessGrant record;
        std::string secret_digest;
    };

    static void validate_spec(const AccessGrantSpec &spec);
    static std::string random_token(std::size_t bytes);
    static std::string digest_secret(const std::string &secret);
    static bool constant_time_equal(const std::string &left, const std::string &right);
    static bool valid_ip_literal(const std::string &value);
    static std::string branch_key(
        const std::string &workspace,
        const std::string &branch);

    StoredAccessGrant &required_locked(const std::string &grant_id);
    const StoredAccessGrant &required_locked(const std::string &grant_id) const;
    void expire_locked(StoredAccessGrant &grant, std::chrono::system_clock::time_point now) const;
    void require_active_locked(StoredAccessGrant &grant, std::chrono::system_clock::time_point now) const;

    const std::chrono::seconds recovery_window_;
    const Clock clock_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, StoredAccessGrant> grants_;
    std::unordered_map<std::string, std::string> branch_to_grant_;
    std::unordered_map<std::string, std::string> export_to_grant_;
};

} // namespace chronos::gateway
