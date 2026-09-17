#include "chronos/gateway/access_grant_registry.hpp"

#include <arpa/inet.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <array>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <utility>

namespace chronos::gateway {
namespace {

std::string hex(const unsigned char *data, std::size_t size) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (std::size_t index = 0; index < size; ++index) {
        out << std::setw(2) << static_cast<unsigned int>(data[index]);
    }
    return out.str();
}

} // namespace

AccessGrantRegistry::AccessGrantRegistry(std::chrono::seconds recovery_window, Clock clock)
    : recovery_window_(recovery_window), clock_(std::move(clock)) {
    if (recovery_window_ < std::chrono::seconds::zero()) {
        throw GatewayError("recovery window cannot be negative");
    }
    if (!clock_) throw GatewayError("grant clock is required");
}

IssuedAccessGrant AccessGrantRegistry::create(const AccessGrantSpec &spec) {
    validate_spec(spec);
    const auto now = clock_();
    const std::string id = random_token(16);
    const std::string credential = random_token(32);
    AccessGrant record;
    record.id = id;
    record.workspace = spec.workspace;
    record.branch = spec.branch;
    record.postgres_stores = spec.postgres_stores;
    record.filesystem_access = spec.filesystem_access;
    record.created_at = now;
    record.expires_at = now + spec.ttl;

    std::unique_lock lock(mutex_);
    auto [position, inserted] = grants_.emplace(
        id,
        StoredAccessGrant{record, digest_secret(credential)});
    if (!inserted) throw GatewayError("secure grant identifier collision");
    const auto key = branch_key(spec.workspace, spec.branch);
    if (!branch_to_grant_.emplace(key, id).second) {
        grants_.erase(position);
        throw GatewayError("workspace branch already has active sandbox access");
    }
    return IssuedAccessGrant{position->second.record, credential};
}

AccessGrant AccessGrantRegistry::activate(const std::string &grant_id) {
    std::unique_lock lock(mutex_);
    auto &grant = required_locked(grant_id);
    if (grant.record.state == AccessGrantState::Active) return grant.record;
    if (grant.record.state != AccessGrantState::Provisioning) {
        throw GatewayError("only a provisioning grant can be activated");
    }
    if (clock_() >= grant.record.expires_at) {
        expire_locked(grant, clock_());
        throw GatewayError("grant expired before activation");
    }
    grant.record.state = AccessGrantState::Active;
    return grant.record;
}

AccessGrant AccessGrantRegistry::attach_sandbox(
    const std::string &grant_id,
    const std::string &sandbox_id,
    const std::string &sandbox_ip) {
    if (sandbox_id.empty()) throw GatewayError("sandbox id is required");
    if (!valid_ip_literal(sandbox_ip)) throw GatewayError("sandbox ip must be an IP literal");
    std::unique_lock lock(mutex_);
    auto &grant = required_locked(grant_id);
    require_active_locked(grant, clock_());
    if (grant.record.filesystem_access == FilesystemAccess::None) {
        throw GatewayError("grant has no filesystem capability");
    }
    if (!grant.record.sandbox_id.empty()) {
        if (grant.record.sandbox_id == sandbox_id && grant.record.sandbox_ip == sandbox_ip) {
            return grant.record;
        }
        throw GatewayError("grant is already bound to another sandbox");
    }
    grant.record.sandbox_id = sandbox_id;
    grant.record.sandbox_ip = sandbox_ip;
    grant.record.export_id = random_token(24);
    export_to_grant_.emplace(grant.record.export_id, grant_id);
    return grant.record;
}

AccessGrant AccessGrantRegistry::renew(const std::string &grant_id, std::chrono::seconds ttl) {
    if (ttl <= std::chrono::seconds::zero()) throw GatewayError("grant ttl must be positive");
    std::unique_lock lock(mutex_);
    auto &grant = required_locked(grant_id);
    require_active_locked(grant, clock_());
    grant.record.expires_at = clock_() + ttl;
    return grant.record;
}

AccessGrant AccessGrantRegistry::revoke(const std::string &grant_id) {
    std::unique_lock lock(mutex_);
    auto &grant = required_locked(grant_id);
    if (grant.record.state == AccessGrantState::Deleted || grant.record.state == AccessGrantState::Revoked) {
        return grant.record;
    }
    expire_locked(grant, clock_());
    return grant.record;
}

AccessGrant AccessGrantRegistry::mark_deleted(const std::string &grant_id) {
    std::unique_lock lock(mutex_);
    auto &grant = required_locked(grant_id);
    if (grant.record.state != AccessGrantState::Revoked) {
        throw GatewayError("an active grant cannot be deleted");
    }
    if (!grant.record.export_id.empty()) export_to_grant_.erase(grant.record.export_id);
    branch_to_grant_.erase(branch_key(grant.record.workspace, grant.record.branch));
    grant.record.state = AccessGrantState::Deleted;
    return grant.record;
}

std::optional<AccessGrant> AccessGrantRegistry::get(const std::string &grant_id) const {
    std::shared_lock lock(mutex_);
    auto found = grants_.find(grant_id);
    if (found == grants_.end()) return std::nullopt;
    return found->second.record;
}

std::optional<AccessGrant> AccessGrantRegistry::get_for_branch(
    const std::string &workspace,
    const std::string &branch) const {
    std::shared_lock lock(mutex_);
    auto mapped = branch_to_grant_.find(branch_key(workspace, branch));
    if (mapped == branch_to_grant_.end()) return std::nullopt;
    auto found = grants_.find(mapped->second);
    if (found == grants_.end()) return std::nullopt;
    return found->second.record;
}

AccessGrant AccessGrantRegistry::authorize_postgres(
    const std::string &grant_id,
    const std::string &secret,
    const std::string &store_alias) {
    const std::string supplied_digest = digest_secret(secret);
    std::unique_lock lock(mutex_);
    auto &grant = required_locked(grant_id);
    require_active_locked(grant, clock_());
    if (!constant_time_equal(grant.secret_digest, supplied_digest)) {
        throw GatewayError("invalid grant credential");
    }
    if (grant.record.postgres_stores.find(store_alias) == grant.record.postgres_stores.end()) {
        throw GatewayError("database is not allowed by this grant");
    }
    return grant.record;
}

AccessGrant AccessGrantRegistry::authorize_filesystem(
    const std::string &export_id,
    const std::string &source_ip,
    bool write) {
    std::unique_lock lock(mutex_);
    auto mapped = export_to_grant_.find(export_id);
    if (mapped == export_to_grant_.end()) throw GatewayError("unknown filesystem export");
    auto &grant = required_locked(mapped->second);
    require_active_locked(grant, clock_());
    if (grant.record.sandbox_ip != source_ip) throw GatewayError("filesystem client is not allowed");
    if (grant.record.filesystem_access == FilesystemAccess::None ||
        (write && grant.record.filesystem_access != FilesystemAccess::ReadWrite)) {
        throw GatewayError("filesystem operation is not allowed");
    }
    return grant.record;
}

AccessGrant AccessGrantRegistry::authorize_filesystem_capability(
    const std::string &export_id,
    bool write) {
    std::unique_lock lock(mutex_);
    auto mapped = export_to_grant_.find(export_id);
    if (mapped == export_to_grant_.end()) throw GatewayError("unknown filesystem export");
    auto &grant = required_locked(mapped->second);
    require_active_locked(grant, clock_());
    if (grant.record.sandbox_id.empty() || grant.record.sandbox_ip.empty()) {
        throw GatewayError("filesystem grant is not bound to a sandbox");
    }
    if (grant.record.filesystem_access == FilesystemAccess::None ||
        (write && grant.record.filesystem_access != FilesystemAccess::ReadWrite)) {
        throw GatewayError("filesystem operation is not allowed");
    }
    return grant.record;
}

std::vector<AccessGrant> AccessGrantRegistry::expire_due() {
    const auto now = clock_();
    std::vector<AccessGrant> expired;
    std::unique_lock lock(mutex_);
    for (auto &[_, grant] : grants_) {
        if ((grant.record.state == AccessGrantState::Provisioning || grant.record.state == AccessGrantState::Active) &&
            now >= grant.record.expires_at) {
            expire_locked(grant, now);
            expired.push_back(grant.record);
        }
    }
    return expired;
}

std::vector<AccessGrant> AccessGrantRegistry::due_for_reclamation() const {
    const auto now = clock_();
    std::vector<AccessGrant> due;
    std::shared_lock lock(mutex_);
    for (const auto &[_, grant] : grants_) {
        if (grant.record.state == AccessGrantState::Revoked && grant.record.reclaim_at &&
            now >= *grant.record.reclaim_at) {
            due.push_back(grant.record);
        }
    }
    return due;
}

void AccessGrantRegistry::validate_spec(const AccessGrantSpec &spec) {
    if (spec.workspace.empty()) throw GatewayError("workspace is required");
    if (spec.branch.empty()) throw GatewayError("branch is required");
    if (spec.ttl <= std::chrono::seconds::zero()) throw GatewayError("grant ttl must be positive");
    if (spec.postgres_stores.empty() && spec.filesystem_access == FilesystemAccess::None) {
        throw GatewayError("grant must grant at least one data capability");
    }
    for (const auto &store : spec.postgres_stores) {
        if (store.empty()) throw GatewayError("database aliases cannot be empty");
    }
}

std::string AccessGrantRegistry::random_token(std::size_t bytes) {
    if (bytes == 0 || bytes > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        throw GatewayError("invalid random token size");
    }
    std::vector<unsigned char> data(bytes);
    if (RAND_bytes(data.data(), static_cast<int>(data.size())) != 1) {
        throw GatewayError("secure random generation failed");
    }
    return hex(data.data(), data.size());
}

std::string AccessGrantRegistry::digest_secret(const std::string &secret) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digest_size = 0;
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    if (!context) throw GatewayError("could not allocate digest context");
    const bool okay = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1 &&
        EVP_DigestUpdate(context, secret.data(), secret.size()) == 1 &&
        EVP_DigestFinal_ex(context, digest.data(), &digest_size) == 1;
    EVP_MD_CTX_free(context);
    if (!okay) throw GatewayError("could not hash grant secret");
    return std::string(reinterpret_cast<const char *>(digest.data()), digest_size);
}

bool AccessGrantRegistry::constant_time_equal(const std::string &left, const std::string &right) {
    return left.size() == right.size() &&
        CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

bool AccessGrantRegistry::valid_ip_literal(const std::string &value) {
    in_addr ipv4{};
    in6_addr ipv6{};
    return inet_pton(AF_INET, value.c_str(), &ipv4) == 1 ||
        inet_pton(AF_INET6, value.c_str(), &ipv6) == 1;
}

std::string AccessGrantRegistry::branch_key(
    const std::string &workspace,
    const std::string &branch) {
    return workspace + '\0' + branch;
}

AccessGrantRegistry::StoredAccessGrant &AccessGrantRegistry::required_locked(const std::string &grant_id) {
    auto found = grants_.find(grant_id);
    if (found == grants_.end()) throw GatewayError("grant not found");
    return found->second;
}

const AccessGrantRegistry::StoredAccessGrant &AccessGrantRegistry::required_locked(const std::string &grant_id) const {
    auto found = grants_.find(grant_id);
    if (found == grants_.end()) throw GatewayError("grant not found");
    return found->second;
}

void AccessGrantRegistry::expire_locked(
    StoredAccessGrant &grant,
    std::chrono::system_clock::time_point now) const {
    if (grant.record.state == AccessGrantState::Deleted || grant.record.state == AccessGrantState::Revoked) return;
    grant.record.state = AccessGrantState::Revoked;
    ++grant.record.generation;
    grant.record.reclaim_at = now + recovery_window_;
}

void AccessGrantRegistry::require_active_locked(
    StoredAccessGrant &grant,
    std::chrono::system_clock::time_point now) const {
    if ((grant.record.state == AccessGrantState::Provisioning || grant.record.state == AccessGrantState::Active) &&
        now >= grant.record.expires_at) {
        expire_locked(grant, now);
    }
    if (grant.record.state != AccessGrantState::Active) throw GatewayError("grant is not active");
}

} // namespace chronos::gateway
