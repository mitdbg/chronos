#include "chronos/gateway/postgres_server.hpp"

#include <boost/asio.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cstring>
#include <iomanip>
#include <iterator>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::gateway {
namespace {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using chronos::native::IntervalQueryResult;
using chronos::native::IntervalValue;

std::uint16_t read_u16(const unsigned char *data) {
    return static_cast<std::uint16_t>((data[0] << 8) | data[1]);
}

std::uint32_t read_u32(const unsigned char *data) {
    return (static_cast<std::uint32_t>(data[0]) << 24) |
        (static_cast<std::uint32_t>(data[1]) << 16) |
        (static_cast<std::uint32_t>(data[2]) << 8) |
        static_cast<std::uint32_t>(data[3]);
}

void append_u16(std::string &out, std::uint16_t value) {
    out.push_back(static_cast<char>(value >> 8));
    out.push_back(static_cast<char>(value));
}

void append_u32(std::string &out, std::uint32_t value) {
    out.push_back(static_cast<char>(value >> 24));
    out.push_back(static_cast<char>(value >> 16));
    out.push_back(static_cast<char>(value >> 8));
    out.push_back(static_cast<char>(value));
}

void append_cstring(std::string &out, const std::string &value) {
    out.append(value);
    out.push_back('\0');
}

std::string take_cstring(const std::string &payload, std::size_t &offset) {
    if (offset >= payload.size()) throw GatewayError("malformed PostgreSQL message");
    const auto end = payload.find('\0', offset);
    if (end == std::string::npos) throw GatewayError("unterminated PostgreSQL string");
    std::string result = payload.substr(offset, end - offset);
    offset = end + 1;
    return result;
}

void read_exact(tcp::socket &socket, void *data, std::size_t size) {
    asio::read(socket, asio::buffer(data, size));
}

std::string read_sized_payload(tcp::socket &socket) {
    std::array<unsigned char, 4> length{};
    read_exact(socket, length.data(), length.size());
    const std::uint32_t total = read_u32(length.data());
    if (total < 4 || total > 64U * 1024U * 1024U) {
        throw GatewayError("invalid PostgreSQL message length");
    }
    std::string payload(total - 4, '\0');
    if (!payload.empty()) read_exact(socket, payload.data(), payload.size());
    return payload;
}

void send_message(tcp::socket &socket, char type, const std::string &payload) {
    std::string frame;
    frame.reserve(payload.size() + 5);
    frame.push_back(type);
    append_u32(frame, static_cast<std::uint32_t>(payload.size() + 4));
    frame.append(payload);
    asio::write(socket, asio::buffer(frame));
}

void send_auth(tcp::socket &socket, std::uint32_t method) {
    std::string payload;
    append_u32(payload, method);
    send_message(socket, 'R', payload);
}

void send_parameter(tcp::socket &socket, const std::string &name, const std::string &value) {
    std::string payload;
    append_cstring(payload, name);
    append_cstring(payload, value);
    send_message(socket, 'S', payload);
}

void send_ready(tcp::socket &socket, char state) {
    send_message(socket, 'Z', std::string(1, state));
}

void send_error(tcp::socket &socket, const std::string &message, const std::string &code = "XX000") {
    std::string payload;
    payload.push_back('S'); append_cstring(payload, "ERROR");
    payload.push_back('V'); append_cstring(payload, "ERROR");
    payload.push_back('C'); append_cstring(payload, code);
    payload.push_back('M'); append_cstring(payload, message);
    payload.push_back('\0');
    send_message(socket, 'E', payload);
}

std::string lower_keyword(const std::string &sql) {
    std::size_t offset = 0;
    while (offset < sql.size()) {
        while (offset < sql.size() &&
               (std::isspace(static_cast<unsigned char>(sql[offset])) || sql[offset] == ';')) {
            ++offset;
        }
        if (offset + 1 < sql.size() && sql[offset] == '-' && sql[offset + 1] == '-') {
            const auto newline = sql.find('\n', offset + 2);
            offset = newline == std::string::npos ? sql.size() : newline + 1;
            continue;
        }
        if (offset + 1 < sql.size() && sql[offset] == '/' && sql[offset + 1] == '*') {
            const auto end = sql.find("*/", offset + 2);
            if (end == std::string::npos) return {};
            offset = end + 2;
            continue;
        }
        break;
    }
    auto begin = sql.begin() + static_cast<std::ptrdiff_t>(offset);
    std::string word;
    while (begin != sql.end() && (std::isalpha(static_cast<unsigned char>(*begin)) || *begin == '_')) {
        word.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(*begin++))));
    }
    return word;
}

void enforce_branch_sql_boundary(const std::string &sql) {
    std::string lowered;
    lowered.reserve(sql.size());
    std::transform(sql.begin(), sql.end(), std::back_inserter(lowered), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    // The core engine exposes its physical interval and metadata tables to
    // trusted administrative callers. A sandbox connection is not trusted:
    // naming that reserved namespace would bypass logical-table visibility
    // rewriting, so fail closed before parsing or execution.
    if (lowered.find("_chronos_") != std::string::npos) {
        throw GatewayError("the _chronos_ namespace is not accessible through the sandbox gateway");
    }
}

std::string value_text(const IntervalValue &value) {
    if (std::holds_alternative<std::monostate>(value)) return {};
    if (const auto *integer = std::get_if<std::int64_t>(&value)) return std::to_string(*integer);
    if (const auto *number = std::get_if<double>(&value)) {
        std::ostringstream out;
        out << std::setprecision(17) << *number;
        return out.str();
    }
    if (const auto *text = std::get_if<std::string>(&value)) return *text;
    if (const auto *blob = std::get_if<chronos::native::IntervalBlob>(&value)) {
        std::ostringstream out;
        out << "\\x" << std::hex << std::setfill('0');
        for (auto byte : *blob) out << std::setw(2) << static_cast<unsigned int>(byte);
        return out.str();
    }
    if (const auto *decimal = std::get_if<chronos::native::IntervalDecimal>(&value)) {
        return decimal->text;
    }
    const auto &date = std::get<chronos::native::IntervalDate>(value);
    std::ostringstream out;
    out << std::setfill('0') << std::setw(4) << date.year << '-'
        << std::setw(2) << date.month << '-' << std::setw(2) << date.day;
    return out.str();
}

void send_row_description(tcp::socket &socket, const std::vector<std::string> &columns) {
    std::string payload;
    append_u16(payload, static_cast<std::uint16_t>(columns.size()));
    for (const auto &column : columns) {
        append_cstring(payload, column);
        append_u32(payload, 0);              // table oid
        append_u16(payload, 0);              // attribute number
        append_u32(payload, 25);             // text oid
        append_u16(payload, 0xffff);         // variable length
        append_u32(payload, 0xffffffff);     // no type modifier
        append_u16(payload, 0);              // text format
    }
    send_message(socket, 'T', payload);
}

void send_result(tcp::socket &socket, const IntervalQueryResult &result) {
    send_row_description(socket, result.columns);
    for (const auto &row : result.rows) {
        std::string payload;
        append_u16(payload, static_cast<std::uint16_t>(row.size()));
        for (const auto &value : row) {
            if (std::holds_alternative<std::monostate>(value)) {
                append_u32(payload, 0xffffffff);
            } else {
                const auto text = value_text(value);
                append_u32(payload, static_cast<std::uint32_t>(text.size()));
                payload.append(text);
            }
        }
        send_message(socket, 'D', payload);
    }
    send_message(socket, 'C', "SELECT " + std::to_string(result.rows.size()) + '\0');
}

struct Portal {
    std::string sql;
    std::vector<IntervalValue> params;
    std::optional<IntervalQueryResult> described_result;
};

struct Execution {
    bool returns_rows = false;
    IntervalQueryResult rows;
    std::string tag;
};

Execution execute_sql(SqlBranchSession &branch_session, const std::string &sql,
                      const std::vector<IntervalValue> &params) {
    enforce_branch_sql_boundary(sql);
    auto &session = branch_session.session();
    const std::string keyword = lower_keyword(sql);
    if (keyword.empty()) return {false, {}, ""};
    if (keyword == "begin" || keyword == "start") {
        session.begin();
        return {false, {}, "BEGIN"};
    }
    if (keyword == "commit" || keyword == "end") {
        session.commit();
        return {false, {}, "COMMIT"};
    }
    if (keyword == "rollback") {
        session.rollback();
        return {false, {}, "ROLLBACK"};
    }
    if (keyword == "set" || keyword == "reset" || keyword == "discard") {
        return {false, {}, "SET"};
    }
    if (keyword == "show") {
        IntervalQueryResult result;
        result.columns = {"setting"};
        result.rows = {{std::string("on")}};
        return {true, std::move(result), {}};
    }
    if (keyword == "select" || keyword == "with") {
        return {true, session.query(sql, params), {}};
    }
    const auto affected = session.execute(sql, params);
    std::string command = keyword;
    std::transform(command.begin(), command.end(), command.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    if (command == "INSERT") return {false, {}, "INSERT 0 " + std::to_string(affected)};
    return {false, {}, command + " " + std::to_string(affected)};
}

void send_execution(tcp::socket &socket, const Execution &execution, bool include_description) {
    if (execution.returns_rows) {
        if (include_description) send_result(socket, execution.rows);
        else {
            for (const auto &row : execution.rows.rows) {
                std::string payload;
                append_u16(payload, static_cast<std::uint16_t>(row.size()));
                for (const auto &value : row) {
                    if (std::holds_alternative<std::monostate>(value)) append_u32(payload, 0xffffffff);
                    else {
                        const auto text = value_text(value);
                        append_u32(payload, static_cast<std::uint32_t>(text.size()));
                        payload.append(text);
                    }
                }
                send_message(socket, 'D', payload);
            }
            send_message(socket, 'C', "SELECT " + std::to_string(execution.rows.rows.size()) + '\0');
        }
    } else if (execution.tag.empty()) {
        send_message(socket, 'I', {});
    } else {
        send_message(socket, 'C', execution.tag + '\0');
    }
}

} // namespace

class PostgresServer::Impl {
  public:
    Impl(GatewayRuntime &runtime, ListenAddress listen)
        : runtime(runtime), listen(std::move(listen)), acceptor(io) {}

    ~Impl() { stop(); join_clients(); }

    void run() {
        const auto address = asio::ip::make_address(listen.host);
        tcp::endpoint endpoint(address, listen.port);
        acceptor.open(endpoint.protocol());
        acceptor.set_option(asio::socket_base::reuse_address(true));
        acceptor.bind(endpoint);
        acceptor.listen(asio::socket_base::max_listen_connections);
        acceptor.non_blocking(true);
        while (!stopping.load()) {
            boost::system::error_code error;
            auto socket = std::make_shared<tcp::socket>(io);
            acceptor.accept(*socket, error);
            if (error) {
                if (error == asio::error::would_block || error == asio::error::try_again) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    continue;
                }
                if (stopping.load() || error == asio::error::operation_aborted ||
                    error == asio::error::bad_descriptor) break;
                throw boost::system::system_error(error);
            }
            std::lock_guard lock(clients_mutex);
            sockets.push_back(socket);
            clients.emplace_back([this, socket] { serve(socket); });
        }
        join_clients();
    }

    void stop() {
        if (stopping.exchange(true)) return;
        boost::system::error_code ignored;
        acceptor.cancel(ignored);
        acceptor.close(ignored);
        std::lock_guard lock(clients_mutex);
        for (const auto &socket : sockets) {
            socket->cancel(ignored);
            socket->close(ignored);
        }
    }

  private:
    void join_clients() {
        std::vector<std::thread> pending;
        {
            std::lock_guard lock(clients_mutex);
            pending.swap(clients);
            sockets.clear();
        }
        for (auto &thread : pending) if (thread.joinable()) thread.join();
    }

    void serve(const std::shared_ptr<tcp::socket> &socket_ptr) noexcept {
        try {
            auto &socket = *socket_ptr;
            std::unordered_map<std::string, std::string> startup;
            while (true) {
                const auto payload = read_sized_payload(socket);
                if (payload.size() < 4) throw GatewayError("invalid PostgreSQL startup packet");
                const auto protocol = read_u32(reinterpret_cast<const unsigned char *>(payload.data()));
                if (protocol == 80877103U) {
                    asio::write(socket, asio::buffer("N", 1));
                    continue;
                }
                if (protocol != 196608U) throw GatewayError("unsupported PostgreSQL protocol version");
                std::size_t offset = 4;
                while (offset < payload.size() && payload[offset] != '\0') {
                    auto name = take_cstring(payload, offset);
                    auto value = take_cstring(payload, offset);
                    startup.emplace(std::move(name), std::move(value));
                }
                break;
            }
            const auto user = startup.find("user");
            const auto database = startup.find("database");
            if (user == startup.end() || database == startup.end()) {
                throw GatewayError("startup packet requires user and database");
            }
            send_auth(socket, 3);
            char type = 0;
            read_exact(socket, &type, 1);
            if (type != 'p') throw GatewayError("expected PostgreSQL password message");
            auto password_payload = read_sized_payload(socket);
            std::size_t password_offset = 0;
            const auto password = take_cstring(password_payload, password_offset);
            auto branch_session = runtime.open_postgres(user->second, password, database->second);

            send_auth(socket, 0);
            send_parameter(socket, "server_version", "17.0-chronos");
            send_parameter(socket, "server_encoding", "UTF8");
            send_parameter(socket, "client_encoding", "UTF8");
            send_parameter(socket, "DateStyle", "ISO, MDY");
            send_parameter(socket, "integer_datetimes", "on");
            send_parameter(socket, "standard_conforming_strings", "on");
            send_parameter(socket, "TimeZone", "UTC");
            std::string key;
            append_u32(key, 0); append_u32(key, 0);
            send_message(socket, 'K', key);
            send_ready(socket, 'I');

            std::unordered_map<std::string, std::string> statements;
            std::unordered_map<std::string, Portal> portals;
            bool failed_transaction = false;
            bool ignore_until_sync = false;
            while (!stopping.load()) {
                read_exact(socket, &type, 1);
                auto payload = read_sized_payload(socket);
                if (type == 'X') break;
                try {
                    // A live connection must be invalidated immediately when
                    // its workspace expires or is revoked.
                    runtime.access_grants().authorize_postgres(user->second, password, database->second);
                    if (ignore_until_sync && type != 'S') continue;
                    if (type == 'Q') {
                        std::size_t offset = 0;
                        const auto sql = take_cstring(payload, offset);
                        const auto keyword = lower_keyword(sql);
                        if (failed_transaction && keyword != "rollback" &&
                            keyword != "commit" && keyword != "end") {
                            send_error(socket,
                                "current transaction is aborted, commands ignored until end of transaction block",
                                "25P02");
                            send_ready(socket, 'E');
                            continue;
                        }
                        if (failed_transaction && (keyword == "commit" || keyword == "end")) {
                            branch_session->session().rollback();
                            send_message(socket, 'C', std::string("ROLLBACK") + '\0');
                            failed_transaction = false;
                            send_ready(socket, 'I');
                            continue;
                        }
                        auto execution = execute_sql(*branch_session, sql, {});
                        send_execution(socket, execution, true);
                        failed_transaction = false;
                        send_ready(socket, branch_session->session().in_transaction() ? 'T' : 'I');
                    } else if (type == 'P') {
                        std::size_t offset = 0;
                        auto name = take_cstring(payload, offset);
                        auto sql = take_cstring(payload, offset);
                        statements[std::move(name)] = std::move(sql);
                        send_message(socket, '1', {});
                    } else if (type == 'B') {
                        std::size_t offset = 0;
                        auto portal_name = take_cstring(payload, offset);
                        auto statement_name = take_cstring(payload, offset);
                        auto found = statements.find(statement_name);
                        if (found == statements.end()) throw GatewayError("unknown prepared statement");
                        if (offset + 2 > payload.size()) throw GatewayError("malformed bind message");
                        const auto format_count = read_u16(reinterpret_cast<const unsigned char *>(payload.data() + offset));
                        offset += 2 + static_cast<std::size_t>(format_count) * 2;
                        if (offset + 2 > payload.size()) throw GatewayError("malformed bind formats");
                        const auto count = read_u16(reinterpret_cast<const unsigned char *>(payload.data() + offset));
                        offset += 2;
                        Portal portal;
                        portal.sql = found->second;
                        for (std::uint16_t index = 0; index < count; ++index) {
                            if (offset + 4 > payload.size()) throw GatewayError("malformed bind parameter");
                            const auto length = read_u32(reinterpret_cast<const unsigned char *>(payload.data() + offset));
                            offset += 4;
                            if (length == 0xffffffffU) portal.params.emplace_back(std::monostate{});
                            else {
                                if (offset + length > payload.size()) throw GatewayError("truncated bind parameter");
                                portal.params.emplace_back(payload.substr(offset, length));
                                offset += length;
                            }
                        }
                        portals[std::move(portal_name)] = std::move(portal);
                        send_message(socket, '2', {});
                    } else if (type == 'D') {
                        if (payload.empty()) throw GatewayError("malformed describe message");
                        std::size_t offset = 1;
                        const auto name = take_cstring(payload, offset);
                        if (payload[0] == 'S') {
                            auto found = statements.find(name);
                            if (found == statements.end()) throw GatewayError("unknown prepared statement");
                            std::string parameter_description;
                            append_u16(parameter_description, 0);
                            send_message(socket, 't', parameter_description);
                            send_message(socket, 'n', {});
                        } else if (payload[0] == 'P') {
                            auto found = portals.find(name);
                            if (found == portals.end()) throw GatewayError("unknown portal");
                            const auto keyword = lower_keyword(found->second.sql);
                            if (keyword == "select" || keyword == "with" || keyword == "show") {
                                auto execution = execute_sql(*branch_session, found->second.sql, found->second.params);
                                found->second.described_result = std::move(execution.rows);
                                send_row_description(socket, found->second.described_result->columns);
                            } else send_message(socket, 'n', {});
                        } else throw GatewayError("invalid describe target");
                    } else if (type == 'E') {
                        std::size_t offset = 0;
                        const auto portal_name = take_cstring(payload, offset);
                        auto found = portals.find(portal_name);
                        if (found == portals.end()) throw GatewayError("unknown portal");
                        if (found->second.described_result) {
                            Execution execution{true, std::move(*found->second.described_result), {}};
                            found->second.described_result.reset();
                            send_execution(socket, execution, false);
                        } else {
                            send_execution(socket, execute_sql(
                                *branch_session, found->second.sql, found->second.params), false);
                        }
                    } else if (type == 'C') {
                        if (payload.empty()) throw GatewayError("malformed close message");
                        std::size_t offset = 1;
                        const auto name = take_cstring(payload, offset);
                        if (payload[0] == 'S') statements.erase(name);
                        else if (payload[0] == 'P') portals.erase(name);
                        send_message(socket, '3', {});
                    } else if (type == 'S') {
                        ignore_until_sync = false;
                        send_ready(socket, failed_transaction ? 'E' :
                            (branch_session->session().in_transaction() ? 'T' : 'I'));
                    } else if (type == 'H') {
                        // Flush requires no response; writes are synchronous.
                    } else {
                        throw GatewayError("unsupported PostgreSQL frontend message");
                    }
                } catch (const std::exception &error) {
                    failed_transaction = branch_session->session().in_transaction();
                    send_error(socket, error.what(), "0A000");
                    if (type == 'Q') send_ready(socket, failed_transaction ? 'E' : 'I');
                    else ignore_until_sync = true;
                }
            }
        } catch (const boost::system::system_error &) {
            // Disconnects and stop() are ordinary session termination.
        } catch (const std::exception &error) {
            try { send_error(*socket_ptr, error.what(), "28000"); } catch (...) {}
        }
    }

    GatewayRuntime &runtime;
    ListenAddress listen;
    asio::io_context io;
    tcp::acceptor acceptor;
    std::atomic<bool> stopping{false};
    std::mutex clients_mutex;
    std::vector<std::shared_ptr<tcp::socket>> sockets;
    std::vector<std::thread> clients;
};

PostgresServer::PostgresServer(GatewayRuntime &runtime, ListenAddress listen)
    : impl_(std::make_unique<Impl>(runtime, std::move(listen))) {}
PostgresServer::~PostgresServer() = default;
void PostgresServer::run() { impl_->run(); }
void PostgresServer::stop() { impl_->stop(); }

} // namespace chronos::gateway
