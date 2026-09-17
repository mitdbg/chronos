#include "chronos/gateway/runtime.hpp"

#include <algorithm>
#include <utility>

namespace chronos::gateway {

SqlBranchSession::SqlBranchSession(
    std::unique_ptr<chronos::native::NativeBranchStore> store,
    chronos::native::NativeBranchSession session,
    AccessGrant grant)
    : store_(std::move(store)),
      session_(std::move(session)),
      access_grant_(std::move(grant)) {}

SqlBranchSession::~SqlBranchSession() = default;
SqlBranchSession::SqlBranchSession(SqlBranchSession &&) noexcept = default;
SqlBranchSession &SqlBranchSession::operator=(SqlBranchSession &&) noexcept = default;

GatewayRuntime::GatewayRuntime(
    std::chrono::seconds recovery_window,
    AccessGrantRegistry::Clock clock)
    : access_grants_(recovery_window, std::move(clock)) {}

void GatewayRuntime::add_workspace(const std::string &name, const WorkspaceConfig &config) {
    validate_workspace(name, config);
    auto entry = std::make_shared<Workspace>();
    entry->config = config;

    std::string control_data_url;
    std::string control_metadata_url;
    if (!config.postgres_stores.empty()) {
        control_data_url = config.postgres_stores.begin()->second.data_url;
        control_metadata_url = config.postgres_stores.begin()->second.metadata_url;
    } else if (config.filesystem) {
        control_data_url = config.filesystem->data_url;
        control_metadata_url = config.filesystem->metadata_url;
    }
    if (control_data_url.empty()) throw GatewayError("workspace has no control store");
    entry->control_store = control_metadata_url.empty()
        ? std::make_unique<chronos::native::NativeBranchStore>(control_data_url)
        : std::make_unique<chronos::native::NativeBranchStore>(
              control_data_url, control_metadata_url);
    // Gateway clients may issue branch-local DDL. Migrate tables registered by
    // older callers into the schema-version catalog before serving branches.
    entry->control_store->ensure(true);

    if (config.filesystem) {
        entry->filesystem = std::make_unique<chronos::native::NativeChronosFilesystem>(
            config.filesystem->data_url,
            config.filesystem->metadata_url,
            config.filesystem->block_size);
        entry->filesystem->ensure();
    }

    std::lock_guard lock(workspaces_mutex_);
    if (!workspaces_.emplace(name, std::move(entry)).second) {
        throw GatewayError("workspace is already configured");
    }
}

ProvisionedBranch GatewayRuntime::create_branch(const CreateBranchRequest &request) {
    if (request.from_branch.empty()) throw GatewayError("source branch is required");
    if (request.branch_id.empty()) throw GatewayError("branch id is required");
    auto target = workspace(request.workspace);
    for (const auto &store : request.postgres_stores) {
        if (target->config.postgres_stores.find(store) == target->config.postgres_stores.end()) {
            throw GatewayError("branch requests an unknown database");
        }
    }
    if (request.filesystem_access != FilesystemAccess::None && !target->filesystem) {
        throw GatewayError("workspace has no filesystem");
    }

    {
        std::lock_guard lock(target->control_mutex);
        target->control_store->create_branch(
            request.branch_id,
            request.from_branch,
            true,
            "{\"owner\":\"chronos-gateway\"}");
        if (target->filesystem) target->filesystem->refresh(request.branch_id);
    }

    AccessGrantSpec spec;
    spec.workspace = request.workspace;
    spec.branch = request.branch_id;
    spec.postgres_stores = request.postgres_stores;
    spec.filesystem_access = request.filesystem_access;
    spec.ttl = request.ttl;
    try {
        auto issued = access_grants_.create(spec);
        issued.grant = access_grants_.activate(issued.grant.id);
        return ProvisionedBranch{std::move(issued.grant), std::move(issued.credential)};
    } catch (...) {
        std::lock_guard lock(target->control_mutex);
        target->control_store->delete_branch(request.branch_id);
        throw;
    }
}

AccessGrant GatewayRuntime::attach_branch(
    const std::string &workspace_name,
    const std::string &branch_id,
    const std::string &sandbox_id,
    const std::string &sandbox_ip) {
    auto grant = access_grants_.get_for_branch(workspace_name, branch_id);
    if (!grant) throw GatewayError("workspace branch not found");
    return access_grants_.attach_sandbox(grant->id, sandbox_id, sandbox_ip);
}

AccessGrant GatewayRuntime::close_branch(
    const std::string &workspace_name,
    const std::string &branch_id) {
    auto grant = access_grants_.get_for_branch(workspace_name, branch_id);
    if (!grant) throw GatewayError("workspace branch not found");
    return access_grants_.revoke(grant->id);
}

std::unique_ptr<SqlBranchSession> GatewayRuntime::open_postgres(
    const std::string &access_id,
    const std::string &credential,
    const std::string &store_alias) {
    auto grant = access_grants_.authorize_postgres(access_id, credential, store_alias);
    auto target = workspace(grant.workspace);
    auto found = target->config.postgres_stores.find(store_alias);
    if (found == target->config.postgres_stores.end()) {
        throw GatewayError("authorized database is not configured");
    }
    auto store = found->second.metadata_url.empty()
        ? std::make_unique<chronos::native::NativeBranchStore>(found->second.data_url)
        : std::make_unique<chronos::native::NativeBranchStore>(
              found->second.data_url, found->second.metadata_url);
    auto session = store->checkout(grant.branch);
    return std::make_unique<SqlBranchSession>(
        std::move(store), std::move(session), std::move(grant));
}

chronos::native::NativeChronosFilesystem &GatewayRuntime::authorize_filesystem(
    const std::string &export_id,
    const std::string &source_ip,
    bool write,
    AccessGrant *authorized_grant) {
    auto grant = access_grants_.authorize_filesystem(export_id, source_ip, write);
    auto target = workspace(grant.workspace);
    if (!target->filesystem) throw GatewayError("authorized filesystem is not configured");
    if (authorized_grant) *authorized_grant = grant;
    return *target->filesystem;
}

chronos::native::NativeChronosFilesystem &GatewayRuntime::authorize_filesystem_capability(
    const std::string &export_id,
    bool write,
    AccessGrant *authorized_grant) {
    auto grant = access_grants_.authorize_filesystem_capability(export_id, write);
    auto target = workspace(grant.workspace);
    if (!target->filesystem) throw GatewayError("authorized filesystem is not configured");
    if (authorized_grant) *authorized_grant = grant;
    return *target->filesystem;
}

std::vector<AccessGrant> GatewayRuntime::expire_due() {
    return access_grants_.expire_due();
}

std::vector<AccessGrant> GatewayRuntime::reclaim_due() {
    auto due = access_grants_.due_for_reclamation();
    std::vector<AccessGrant> reclaimed;
    reclaimed.reserve(due.size());
    for (const auto &grant : due) {
        try {
            delete_branch(grant);
            reclaimed.push_back(access_grants_.mark_deleted(grant.id));
        } catch (...) {
            // Leave the grant revoked so the next reaper pass retries safely.
        }
    }
    return reclaimed;
}

void GatewayRuntime::validate_workspace(
    const std::string &name,
    const WorkspaceConfig &config) {
    if (name.empty()) throw GatewayError("workspace name is required");
    if (config.postgres_stores.empty() && !config.filesystem) {
        throw GatewayError("workspace must configure at least one store");
    }
    std::string metadata_url;
    auto check_store = [&](const std::string &data_url, const std::string &candidate) {
        if (data_url.empty()) throw GatewayError("store data url is required");
        if (candidate.empty()) {
            throw GatewayError("sandbox gateway stores require a separate metadata url");
        }
        if (candidate == data_url) {
            throw GatewayError("sandbox gateway data and metadata urls must be separate");
        }
        if (metadata_url.empty()) metadata_url = candidate;
        else if (metadata_url != candidate) {
            throw GatewayError("all workspace stores must share one metadata url");
        }
    };
    for (const auto &[alias, store] : config.postgres_stores) {
        if (alias.empty()) throw GatewayError("database alias is required");
        check_store(store.data_url, store.metadata_url);
    }
    if (config.filesystem) {
        if (config.filesystem->block_size <= 0) throw GatewayError("filesystem block size must be positive");
        check_store(config.filesystem->data_url, config.filesystem->metadata_url);
    }
}

std::shared_ptr<GatewayRuntime::Workspace> GatewayRuntime::workspace(
    const std::string &name) const {
    std::lock_guard lock(workspaces_mutex_);
    auto found = workspaces_.find(name);
    if (found == workspaces_.end()) throw GatewayError("workspace is not configured");
    return found->second;
}

void GatewayRuntime::delete_branch(const AccessGrant &grant) {
    auto target = workspace(grant.workspace);
    std::lock_guard lock(target->control_mutex);
    if (target->filesystem) target->filesystem->refresh(grant.branch);
    target->control_store->delete_branch(grant.branch);
}

} // namespace chronos::gateway
