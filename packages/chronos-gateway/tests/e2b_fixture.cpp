#include "chronos/gateway/runtime.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

using chronos::native::NativeBranchStore;
using chronos::native::NativeChronosFilesystem;

namespace {

std::string sqlite_url(const std::filesystem::path &path) {
    return "sqlite://" + path.string();
}

void create_empty_file(const std::filesystem::path &path) {
    std::ofstream output(path);
    if (!output) throw std::runtime_error("cannot create " + path.string());
}

} // namespace

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " STATE_DIRECTORY\n";
        return 2;
    }

    try {
        const std::filesystem::path state(argv[1]);
        std::filesystem::create_directories(state);
        const auto metadata_path = state / "metadata.sqlite";
        const auto database_path = state / "database.sqlite";
        const auto filesystem_path = state / "filesystem.sqlite";
        create_empty_file(metadata_path);
        create_empty_file(database_path);
        create_empty_file(filesystem_path);

        const auto metadata = sqlite_url(metadata_path);
        const auto database = sqlite_url(database_path);
        const auto filesystem = sqlite_url(filesystem_path);

        NativeBranchStore store(database, metadata);
        store.ensure();
        store.execute_sql(
            "CREATE TABLE chronos_gateway_e2e "
            "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)");
        store.execute_sql(
            "INSERT INTO chronos_gateway_e2e (id, value) VALUES (?, ?)",
            {std::int64_t(1), std::string("base")});
        store.execute_sql(
            "CREATE TABLE tickets "
            "(id INTEGER PRIMARY KEY, subject TEXT NOT NULL, priority TEXT NOT NULL)");
        store.execute_sql(
            "INSERT INTO tickets (id, subject, priority) VALUES (?, ?, ?)",
            {std::int64_t(101), std::string("Payment outage"), std::string("normal")});
        store.execute_sql(
            "INSERT INTO tickets (id, subject, priority) VALUES (?, ?, ?)",
            {std::int64_t(102), std::string("Documentation typo"), std::string("normal")});
        store.commit();
        store.register_table("chronos_gateway_e2e", {"id"});
        store.register_table("tickets", {"id"});

        NativeChronosFilesystem files(filesystem, metadata, 4096);
        files.ensure();

        std::cout << "Chronos E2B fixture initialized in " << state << '\n';
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "Chronos E2B fixture failed: " << error.what() << '\n';
        return 1;
    }
}
