#include "chronos_s3.hpp"

#include "interval_data_plane.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <pybind11/stl.h>
#include <pugixml.hpp>
#include <sys/socket.h>

namespace chronos::native {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;
using Json = nlohmann::json;

constexpr const char *kObjectTable = "chronos_s3_objects";
const std::vector<std::string> kObjectColumns = {
    "bucket",
    "object_key",
    "physical_bucket",
    "physical_key",
    "etag",
    "size_bytes",
    "last_modified_ms",
    "content_type",
    "content_encoding",
    "cache_control",
    "user_metadata",
    "ownership",
    "generation",
};
const std::vector<std::string> kObjectPrimaryKey = {"bucket", "object_key"};

std::string sqlite_url(const std::string &path) {
    std::string url = path;
    std::string file_path = path;
    const std::string prefix = "sqlite:///";
    if (path.rfind(prefix, 0) == 0) {
        file_path = "/" + path.substr(prefix.size());
    } else if (path.rfind("sqlite://", 0) == 0) {
        file_path = path.substr(std::strlen("sqlite://"));
    } else {
        url = "sqlite:///" + path;
    }
    sqlite3 *database = nullptr;
    if (sqlite3_open_v2(
            file_path.c_str(),
            &database,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
            nullptr
        ) != SQLITE_OK) {
        std::string message = database ? sqlite3_errmsg(database) : "unknown sqlite error";
        if (database) sqlite3_close(database);
        throw std::runtime_error("unable to create ChronosS3 metadata database: " + message);
    }
    sqlite3_close(database);
    return url;
}

std::string sqlite_file_path(const std::string &path) {
    const std::string prefix = "sqlite:///";
    if (path.rfind(prefix, 0) == 0) return "/" + path.substr(prefix.size());
    if (path.rfind("sqlite://", 0) == 0) return path.substr(std::strlen("sqlite://"));
    return path;
}

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string trim(std::string value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.erase(value.begin());
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.pop_back();
    }
    return value;
}

std::string url_encode(std::string_view input, bool preserve_slash = false) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(input.size() * 3);
    for (unsigned char c : input) {
        const bool unreserved =
            std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' ||
            (preserve_slash && c == '/');
        if (unreserved) {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0f]);
        }
    }
    return out;
}

std::string url_decode(std::string_view input) {
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::string out;
    out.reserve(input.size());
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '%' && i + 2 < input.size()) {
            int high = nibble(input[i + 1]);
            int low = nibble(input[i + 2]);
            if (high >= 0 && low >= 0) {
                out.push_back(static_cast<char>((high << 4) | low));
                i += 2;
                continue;
            }
        }
        out.push_back(input[i] == '+' ? ' ' : input[i]);
    }
    return out;
}

std::map<std::string, std::string> parse_query(std::string_view query) {
    std::map<std::string, std::string> result;
    std::size_t start = 0;
    while (start <= query.size()) {
        std::size_t end = query.find('&', start);
        if (end == std::string_view::npos) end = query.size();
        std::string_view pair = query.substr(start, end - start);
        std::size_t equals = pair.find('=');
        std::string key = url_decode(pair.substr(0, equals));
        std::string value = equals == std::string_view::npos
            ? "" : url_decode(pair.substr(equals + 1));
        result[key] = value;
        if (end == query.size()) break;
        start = end + 1;
    }
    return result;
}

std::string random_id() {
    static thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<std::uint64_t> distribution;
    std::ostringstream out;
    out << std::hex << std::setfill('0')
        << std::setw(16) << distribution(generator)
        << std::setw(16) << distribution(generator);
    return out.str();
}

std::int64_t unix_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

std::string http_date(std::int64_t millis) {
    const std::time_t seconds = static_cast<std::time_t>(millis / 1000);
    std::tm tm{};
    gmtime_r(&seconds, &tm);
    std::ostringstream out;
    out << std::put_time(&tm, "%a, %d %b %Y %H:%M:%S GMT");
    return out.str();
}

std::string iso8601(std::int64_t millis) {
    const std::time_t seconds = static_cast<std::time_t>(millis / 1000);
    std::tm tm{};
    gmtime_r(&seconds, &tm);
    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S.000Z");
    return out.str();
}

std::string md5_hex(const std::string &bytes) {
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    if (!context) throw std::runtime_error("failed to allocate digest context");
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int length = 0;
    if (EVP_DigestInit_ex(context, EVP_md5(), nullptr) != 1 ||
        EVP_DigestUpdate(context, bytes.data(), bytes.size()) != 1 ||
        EVP_DigestFinal_ex(context, digest, &length) != 1) {
        EVP_MD_CTX_free(context);
        throw std::runtime_error("failed to calculate object etag");
    }
    EVP_MD_CTX_free(context);
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < length; ++i) {
        out << std::setw(2) << static_cast<unsigned int>(digest[i]);
    }
    return out.str();
}

std::vector<unsigned char> sha256(const std::string &bytes) {
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    if (!context) throw std::runtime_error("failed to allocate digest context");
    std::vector<unsigned char> digest(EVP_MAX_MD_SIZE);
    unsigned int length = 0;
    if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(context, bytes.data(), bytes.size()) != 1 ||
        EVP_DigestFinal_ex(context, digest.data(), &length) != 1) {
        EVP_MD_CTX_free(context);
        throw std::runtime_error("failed to calculate sha256");
    }
    EVP_MD_CTX_free(context);
    digest.resize(length);
    return digest;
}

std::string hex(const std::vector<unsigned char> &bytes) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (unsigned char byte : bytes) {
        out << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return out.str();
}

std::vector<unsigned char> hmac_sha256(
    const std::vector<unsigned char> &key,
    const std::string &message
) {
    unsigned int length = 0;
    std::vector<unsigned char> digest(EVP_MAX_MD_SIZE);
    if (!HMAC(
            EVP_sha256(),
            key.data(),
            static_cast<int>(key.size()),
            reinterpret_cast<const unsigned char *>(message.data()),
            message.size(),
            digest.data(),
            &length
        )) {
        throw std::runtime_error("failed to calculate request signature");
    }
    digest.resize(length);
    return digest;
}

std::vector<unsigned char> hmac_sha256(const std::string &key, const std::string &message) {
    return hmac_sha256(
        std::vector<unsigned char>(key.begin(), key.end()),
        message
    );
}

std::string normalize_header_value(const std::string &input) {
    std::ostringstream out;
    bool pending_space = false;
    for (unsigned char c : trim(input)) {
        if (std::isspace(c)) {
            pending_space = true;
        } else {
            if (pending_space && out.tellp() > 0) out << ' ';
            out << static_cast<char>(c);
            pending_space = false;
        }
    }
    return out.str();
}

std::string as_string(const IntervalValue &value) {
    if (const auto *text = std::get_if<std::string>(&value)) return *text;
    if (const auto *number = std::get_if<std::int64_t>(&value)) return std::to_string(*number);
    if (std::holds_alternative<std::monostate>(value)) return "";
    throw std::runtime_error("unexpected ChronosS3 mapping value type");
}

std::int64_t as_int(const IntervalValue &value) {
    if (const auto *number = std::get_if<std::int64_t>(&value)) return *number;
    if (const auto *text = std::get_if<std::string>(&value)) return std::stoll(*text);
    throw std::runtime_error("unexpected ChronosS3 mapping integer type");
}

struct ObjectMapping {
    std::string bucket;
    std::string object_key;
    std::string physical_bucket;
    std::string physical_key;
    std::string etag;
    std::int64_t size_bytes = 0;
    std::int64_t last_modified_ms = 0;
    std::string content_type;
    std::string content_encoding;
    std::string cache_control;
    std::string user_metadata = "{}";
    std::string ownership = "managed";
    std::int64_t generation = 1;
};

IntervalRows mapping_rows(const ObjectMapping &mapping) {
    return {{
        mapping.bucket,
        mapping.object_key,
        mapping.physical_bucket,
        mapping.physical_key,
        mapping.etag,
        mapping.size_bytes,
        mapping.last_modified_ms,
        mapping.content_type,
        mapping.content_encoding,
        mapping.cache_control,
        mapping.user_metadata,
        mapping.ownership,
        mapping.generation,
    }};
}

ObjectMapping mapping_from_row(const std::vector<IntervalValue> &row) {
    if (row.size() != kObjectColumns.size()) {
        throw std::runtime_error("invalid ChronosS3 mapping row");
    }
    return {
        as_string(row[0]),
        as_string(row[1]),
        as_string(row[2]),
        as_string(row[3]),
        as_string(row[4]),
        as_int(row[5]),
        as_int(row[6]),
        as_string(row[7]),
        as_string(row[8]),
        as_string(row[9]),
        as_string(row[10]),
        as_string(row[11]),
        as_int(row[12]),
    };
}

struct UpstreamResponse {
    long status = 0;
    std::string body;
    std::unordered_map<std::string, std::string> headers;
};

std::size_t append_body(char *data, std::size_t size, std::size_t count, void *target) {
    auto *body = static_cast<std::string *>(target);
    body->append(data, size * count);
    return size * count;
}

std::size_t append_header(char *data, std::size_t size, std::size_t count, void *target) {
    auto *headers = static_cast<std::unordered_map<std::string, std::string> *>(target);
    std::string line(data, size * count);
    std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
        (*headers)[lower(trim(line.substr(0, colon)))] = trim(line.substr(colon + 1));
    }
    return size * count;
}

class CurlS3Backend {
  public:
    CurlS3Backend(
        std::string endpoint,
        std::string access_key,
        std::string secret_key,
        std::string region
    ) : endpoint_(std::move(endpoint)),
        access_key_(std::move(access_key)),
        secret_key_(std::move(secret_key)),
        region_(std::move(region)) {
        while (!endpoint_.empty() && endpoint_.back() == '/') endpoint_.pop_back();
        static const int initialized = []() {
            return curl_global_init(CURL_GLOBAL_DEFAULT);
        }();
        if (initialized != CURLE_OK) {
            throw std::runtime_error("failed to initialize libcurl");
        }
    }

    UpstreamResponse request(
        const std::string &method,
        const std::string &bucket,
        const std::string &key = "",
        const std::string &query = "",
        const std::string &body = "",
        const std::vector<std::string> &headers = {}
    ) const {
        CURL *curl = curl_easy_init();
        if (!curl) throw std::runtime_error("failed to initialize S3 request");
        UpstreamResponse response;
        std::string url = endpoint_ + "/" + url_encode(bucket);
        if (!key.empty()) url += "/" + url_encode(key, true);
        if (!query.empty()) url += "?" + query;
        std::string credentials = access_key_ + ":" + secret_key_;
        std::string sigv4 = "aws:amz:" + region_ + ":s3";
        curl_slist *header_list = nullptr;
        for (const auto &header : headers) {
            header_list = curl_slist_append(header_list, header.c_str());
        }
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
        curl_easy_setopt(curl, CURLOPT_USERPWD, credentials.c_str());
        curl_easy_setopt(curl, CURLOPT_AWS_SIGV4, sigv4.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, append_body);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.body);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, append_header);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response.headers);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 120000L);
        if (header_list) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);
        if (method == "HEAD") {
            curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        } else if (method == "PUT" || method == "POST") {
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body.size()));
        }
        CURLcode code = curl_easy_perform(curl);
        if (code != CURLE_OK) {
            std::string message = curl_easy_strerror(code);
            curl_slist_free_all(header_list);
            curl_easy_cleanup(curl);
            throw std::runtime_error("upstream S3 request failed: " + message);
        }
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.status);
        curl_slist_free_all(header_list);
        curl_easy_cleanup(curl);
        return response;
    }

  private:
    std::string endpoint_;
    std::string access_key_;
    std::string secret_key_;
    std::string region_;
};

class BranchableObjectStore {
  public:
    BranchableObjectStore(
        std::string database_path,
        std::shared_ptr<CurlS3Backend> upstream
    ) : database_url_(sqlite_url(database_path)),
        database_path_(sqlite_file_path(database_path)),
        upstream_(std::move(upstream)) {
        NativeBranchStore store(database_url_);
        store.ensure();
        store.execute_sql(
            "CREATE TABLE IF NOT EXISTS chronos_s3_objects ("
            "bucket TEXT NOT NULL, object_key TEXT NOT NULL, "
            "physical_bucket TEXT NOT NULL, physical_key TEXT NOT NULL, "
            "etag TEXT NOT NULL, size_bytes BIGINT NOT NULL, "
            "last_modified_ms BIGINT NOT NULL, content_type TEXT NOT NULL, "
            "content_encoding TEXT NOT NULL, cache_control TEXT NOT NULL, "
            "user_metadata TEXT NOT NULL, ownership TEXT NOT NULL, "
            "generation BIGINT NOT NULL, PRIMARY KEY (bucket, object_key))"
        );
        store.register_table(kObjectTable, kObjectPrimaryKey);
        with_operational_db([&](sqlite3 *database) {
            char *error = nullptr;
            const char *sql =
                "CREATE TABLE IF NOT EXISTS chronos_s3_managed_objects ("
                "physical_bucket TEXT NOT NULL, physical_key TEXT NOT NULL, "
                "created_ms BIGINT NOT NULL, "
                "PRIMARY KEY (physical_bucket, physical_key))";
            if (sqlite3_exec(database, sql, nullptr, nullptr, &error) != SQLITE_OK) {
                std::string message = error ? error : sqlite3_errmsg(database);
                sqlite3_free(error);
                throw std::runtime_error(message);
            }
        });
    }

    ObjectMapping put(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key,
        const std::string &body,
        const std::string &content_type = "application/octet-stream",
        const std::string &content_encoding = "",
        const std::string &cache_control = "",
        const std::string &user_metadata = "{}",
        const std::optional<std::string> &if_match = std::nullopt,
        bool if_none_match = false
    ) {
        const std::string physical_key = "_chronos/objects/" + random_id();
        std::vector<std::string> headers = {"Content-Type: " + content_type};
        if (!content_encoding.empty()) headers.push_back("Content-Encoding: " + content_encoding);
        if (!cache_control.empty()) headers.push_back("Cache-Control: " + cache_control);
        UpstreamResponse uploaded = upstream_->request("PUT", bucket, physical_key, "", body, headers);
        if (uploaded.status < 200 || uploaded.status >= 300) {
            throw std::runtime_error("upstream object PUT failed with status " + std::to_string(uploaded.status));
        }

        try {
            std::lock_guard<std::mutex> guard(write_mutex_);
            NativeBranchStore store(database_url_);
            auto session = store.checkout(branch);
            session.begin();
            auto existing = lookup(session, bucket, key);
            if (if_none_match && existing) {
                session.rollback();
                upstream_->request("DELETE", bucket, physical_key);
                throw std::runtime_error("precondition failed: object already exists");
            }
            if (if_match && (!existing || existing->etag != strip_quotes(*if_match))) {
                session.rollback();
                upstream_->request("DELETE", bucket, physical_key);
                throw std::runtime_error("precondition failed: etag mismatch");
            }
            ObjectMapping mapping{
                bucket,
                key,
                bucket,
                physical_key,
                md5_hex(body),
                static_cast<std::int64_t>(body.size()),
                unix_millis(),
                content_type,
                content_encoding,
                cache_control,
                user_metadata,
                "managed",
                existing ? existing->generation + 1 : 1,
            };
            session.upsert_rows(kObjectTable, kObjectColumns, kObjectPrimaryKey, mapping_rows(mapping));
            session.commit();
            try {
                remember_managed(mapping);
            } catch (...) {
                // A missing GC inventory entry leaks storage but cannot break a published mapping.
            }
            return mapping;
        } catch (...) {
            upstream_->request("DELETE", bucket, physical_key);
            throw;
        }
    }

    std::optional<ObjectMapping> head(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key
    ) const {
        NativeBranchStore store(database_url_);
        auto session = store.checkout(branch);
        return lookup(session, bucket, key);
    }

    std::optional<std::pair<ObjectMapping, UpstreamResponse>> get(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key,
        const std::string &range = ""
    ) const {
        auto mapping = head(branch, bucket, key);
        if (!mapping) return std::nullopt;
        std::vector<std::string> headers;
        if (!range.empty()) headers.push_back("Range: " + range);
        UpstreamResponse response = upstream_->request(
            "GET", mapping->physical_bucket, mapping->physical_key, "", "", headers
        );
        if (response.status != 200 && response.status != 206) {
            throw std::runtime_error("upstream object GET failed with status " + std::to_string(response.status));
        }
        return std::make_pair(*mapping, std::move(response));
    }

    bool erase(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key
    ) {
        std::lock_guard<std::mutex> guard(write_mutex_);
        NativeBranchStore store(database_url_);
        auto session = store.checkout(branch);
        auto existing = lookup(session, bucket, key);
        if (!existing) return false;
        session.delete_rows(
            kObjectTable,
            kObjectColumns,
            kObjectPrimaryKey,
            mapping_rows(*existing)
        );
        return true;
    }

    std::vector<ObjectMapping> list(
        const std::string &branch,
        const std::string &bucket,
        const std::string &prefix,
        const std::string &start_after,
        std::size_t limit
    ) const {
        NativeBranchStore store(database_url_);
        auto session = store.checkout(branch);
        std::string where = "bucket = ? AND object_key LIKE ?";
        std::vector<IntervalValue> params = {bucket, prefix + "%"};
        if (!start_after.empty()) {
            where += " AND object_key > ?";
            params.push_back(start_after);
        }
        auto rows = session.query_visible(
            kObjectTable,
            kObjectColumns,
            where,
            params,
            "ORDER BY object_key LIMIT " + std::to_string(limit)
        );
        std::vector<ObjectMapping> result;
        result.reserve(rows.size());
        for (const auto &row : rows) result.push_back(mapping_from_row(row));
        return result;
    }

    void create_branch(const std::string &branch, const std::string &from_branch) {
        NativeBranchStore store(database_url_);
        store.create_branch(branch, from_branch);
    }

    void delete_branch(const std::string &branch) {
        NativeBranchStore store(database_url_);
        store.delete_branch(branch);
    }

    std::vector<std::string> branches() const {
        NativeBranchStore store(database_url_);
        return store.branches();
    }

    std::size_t bootstrap(const std::string &branch, const std::string &bucket) {
        std::size_t imported = 0;
        std::string continuation;
        do {
            std::string query = "list-type=2&max-keys=1000";
            if (!continuation.empty()) {
                query += "&continuation-token=" + url_encode(continuation);
            }
            UpstreamResponse response = upstream_->request("GET", bucket, "", query);
            if (response.status != 200) {
                throw std::runtime_error("upstream bucket listing failed with status " + std::to_string(response.status));
            }
            pugi::xml_document document;
            if (!document.load_string(response.body.c_str())) {
                throw std::runtime_error("invalid upstream ListObjectsV2 response");
            }
            pugi::xml_node root = document.document_element();
            std::vector<ObjectMapping> batch;
            for (pugi::xml_node object : root.children("Contents")) {
                std::string key = object.child_value("Key");
                if (key.rfind("_chronos/", 0) == 0) continue;
                std::string etag = strip_quotes(object.child_value("ETag"));
                std::int64_t size = std::stoll(object.child_value("Size"));
                batch.push_back({
                    bucket,
                    key,
                    bucket,
                    key,
                    etag,
                    size,
                    unix_millis(),
                    "application/octet-stream",
                    "",
                    "",
                    "{}",
                    "adopted",
                    1,
                });
            }
            if (!batch.empty()) {
                std::lock_guard<std::mutex> guard(write_mutex_);
                NativeBranchStore store(database_url_);
                auto session = store.checkout(branch);
                IntervalRows rows;
                rows.reserve(batch.size());
                for (const auto &mapping : batch) rows.push_back(mapping_rows(mapping).front());
                session.upsert_rows(kObjectTable, kObjectColumns, kObjectPrimaryKey, rows);
                imported += batch.size();
            }
            continuation = root.child_value("NextContinuationToken");
            if (std::string(root.child_value("IsTruncated")) != "true") break;
        } while (!continuation.empty());
        return imported;
    }

    void create_bucket(const std::string &bucket) {
        UpstreamResponse response = upstream_->request("PUT", bucket);
        if (response.status != 200 && response.status != 201 &&
            response.status != 204 && response.status != 409) {
            throw std::runtime_error("upstream bucket creation failed with status " + std::to_string(response.status));
        }
    }

    std::size_t collect_garbage(std::int64_t grace_ms) {
        std::lock_guard<std::mutex> gc_guard(gc_mutex_);
        NativeBranchStore control(database_url_);
        if (!control.list_checkpoints().empty()) {
            return 0;
        }
        std::vector<std::tuple<std::string, std::string, std::int64_t>> candidates;
        with_operational_db([&](sqlite3 *database) {
            sqlite3_stmt *statement = nullptr;
            const char *sql =
                "SELECT physical_bucket, physical_key, created_ms "
                "FROM chronos_s3_managed_objects WHERE created_ms <= ?";
            if (sqlite3_prepare_v2(database, sql, -1, &statement, nullptr) != SQLITE_OK) {
                throw std::runtime_error(sqlite3_errmsg(database));
            }
            sqlite3_bind_int64(statement, 1, unix_millis() - std::max<std::int64_t>(0, grace_ms));
            while (sqlite3_step(statement) == SQLITE_ROW) {
                candidates.emplace_back(
                    reinterpret_cast<const char *>(sqlite3_column_text(statement, 0)),
                    reinterpret_cast<const char *>(sqlite3_column_text(statement, 1)),
                    sqlite3_column_int64(statement, 2)
                );
            }
            sqlite3_finalize(statement);
        });
        if (candidates.empty()) return 0;

        std::unordered_map<std::string, std::unordered_set<std::string>> referenced;
        std::unordered_set<std::string> buckets;
        for (const auto &[bucket, key, created] : candidates) {
            (void)key;
            (void)created;
            buckets.insert(bucket);
        }
        for (const auto &branch : control.branches()) {
            for (const auto &bucket : buckets) {
                std::string start_after;
                while (true) {
                    auto mappings = list(branch, bucket, "", start_after, 10000);
                    for (const auto &mapping : mappings) {
                        referenced[mapping.physical_bucket].insert(mapping.physical_key);
                    }
                    if (mappings.size() < 10000) break;
                    start_after = mappings.back().object_key;
                }
            }
        }

        std::size_t deleted = 0;
        for (const auto &[bucket, key, created] : candidates) {
            (void)created;
            if (referenced[bucket].find(key) != referenced[bucket].end()) continue;
            UpstreamResponse response = upstream_->request("DELETE", bucket, key);
            if (response.status < 200 || response.status >= 300) continue;
            forget_managed(bucket, key);
            ++deleted;
        }
        return deleted;
    }

  private:
    static std::string strip_quotes(std::string value) {
        value = trim(std::move(value));
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            return value.substr(1, value.size() - 2);
        }
        return value;
    }

    static std::optional<ObjectMapping> lookup(
        NativeBranchSession &session,
        const std::string &bucket,
        const std::string &key
    ) {
        auto rows = session.query_visible(
            kObjectTable,
            kObjectColumns,
            "bucket = ? AND object_key = ?",
            {bucket, key}
        );
        if (rows.empty()) return std::nullopt;
        return mapping_from_row(rows.front());
    }

    template <typename Function>
    void with_operational_db(Function &&function) const {
        sqlite3 *database = nullptr;
        if (sqlite3_open_v2(
                database_path_.c_str(),
                &database,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                nullptr
            ) != SQLITE_OK) {
            std::string message = database ? sqlite3_errmsg(database) : "unknown sqlite error";
            if (database) sqlite3_close(database);
            throw std::runtime_error(message);
        }
        sqlite3_busy_timeout(database, 30000);
        try {
            function(database);
            sqlite3_close(database);
        } catch (...) {
            sqlite3_close(database);
            throw;
        }
    }

    void remember_managed(const ObjectMapping &mapping) {
        with_operational_db([&](sqlite3 *database) {
            sqlite3_stmt *statement = nullptr;
            const char *sql =
                "INSERT OR IGNORE INTO chronos_s3_managed_objects "
                "(physical_bucket, physical_key, created_ms) VALUES (?, ?, ?)";
            if (sqlite3_prepare_v2(database, sql, -1, &statement, nullptr) != SQLITE_OK) {
                throw std::runtime_error(sqlite3_errmsg(database));
            }
            sqlite3_bind_text(statement, 1, mapping.physical_bucket.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, 2, mapping.physical_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, 3, mapping.last_modified_ms);
            if (sqlite3_step(statement) != SQLITE_DONE) {
                std::string message = sqlite3_errmsg(database);
                sqlite3_finalize(statement);
                throw std::runtime_error(message);
            }
            sqlite3_finalize(statement);
        });
    }

    void forget_managed(const std::string &bucket, const std::string &key) {
        with_operational_db([&](sqlite3 *database) {
            sqlite3_stmt *statement = nullptr;
            const char *sql =
                "DELETE FROM chronos_s3_managed_objects "
                "WHERE physical_bucket = ? AND physical_key = ?";
            if (sqlite3_prepare_v2(database, sql, -1, &statement, nullptr) != SQLITE_OK) {
                throw std::runtime_error(sqlite3_errmsg(database));
            }
            sqlite3_bind_text(statement, 1, bucket.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, 2, key.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(statement) != SQLITE_DONE) {
                std::string message = sqlite3_errmsg(database);
                sqlite3_finalize(statement);
                throw std::runtime_error(message);
            }
            sqlite3_finalize(statement);
        });
    }

    std::string database_url_;
    std::string database_path_;
    std::shared_ptr<CurlS3Backend> upstream_;
    mutable std::mutex write_mutex_;
    mutable std::mutex gc_mutex_;
};

class MultipartManager {
  public:
    MultipartManager(
        std::string database_path,
        std::shared_ptr<CurlS3Backend> upstream,
        BranchableObjectStore &objects
    ) : database_path_(sqlite_file_path(database_path)),
        upstream_(std::move(upstream)),
        objects_(objects) {
        with_db([&](sqlite3 *database) {
            exec(database,
                "CREATE TABLE IF NOT EXISTS chronos_s3_multipart_uploads ("
                "upload_id TEXT PRIMARY KEY, branch_id TEXT NOT NULL, "
                "bucket TEXT NOT NULL, object_key TEXT NOT NULL, "
                "content_type TEXT NOT NULL, created_ms BIGINT NOT NULL)");
            exec(database,
                "CREATE TABLE IF NOT EXISTS chronos_s3_multipart_parts ("
                "upload_id TEXT NOT NULL, part_number INTEGER NOT NULL, "
                "physical_bucket TEXT NOT NULL, physical_key TEXT NOT NULL, "
                "etag TEXT NOT NULL, size_bytes BIGINT NOT NULL, "
                "PRIMARY KEY (upload_id, part_number))");
        });
    }

    std::string create(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key,
        const std::string &content_type
    ) {
        const std::string upload_id = random_id();
        std::lock_guard<std::mutex> guard(mutex_);
        with_db([&](sqlite3 *database) {
            Statement statement(
                database,
                "INSERT INTO chronos_s3_multipart_uploads "
                "(upload_id, branch_id, bucket, object_key, content_type, created_ms) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            );
            statement.text(1, upload_id);
            statement.text(2, branch);
            statement.text(3, bucket);
            statement.text(4, key);
            statement.text(5, content_type);
            statement.integer(6, unix_millis());
            statement.done();
        });
        return upload_id;
    }

    std::string upload_part(
        const std::string &branch,
        const std::string &upload_id,
        int part_number,
        const std::string &body
    ) {
        if (part_number < 1 || part_number > 10000) {
            throw std::runtime_error("invalid multipart part number");
        }
        Upload upload = load_upload(upload_id);
        if (upload.branch != branch) throw std::runtime_error("multipart upload belongs to another branch");
        const std::string physical_key =
            "_chronos/multipart/" + upload_id + "/" + std::to_string(part_number);
        UpstreamResponse response = upstream_->request(
            "PUT", upload.bucket, physical_key, "", body
        );
        if (response.status < 200 || response.status >= 300) {
            throw std::runtime_error("upstream multipart part PUT failed");
        }
        const std::string etag = md5_hex(body);
        std::lock_guard<std::mutex> guard(mutex_);
        with_db([&](sqlite3 *database) {
            Statement statement(
                database,
                "INSERT INTO chronos_s3_multipart_parts "
                "(upload_id, part_number, physical_bucket, physical_key, etag, size_bytes) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(upload_id, part_number) DO UPDATE SET "
                "physical_bucket=excluded.physical_bucket, physical_key=excluded.physical_key, "
                "etag=excluded.etag, size_bytes=excluded.size_bytes"
            );
            statement.text(1, upload_id);
            statement.integer(2, part_number);
            statement.text(3, upload.bucket);
            statement.text(4, physical_key);
            statement.text(5, etag);
            statement.integer(6, static_cast<std::int64_t>(body.size()));
            statement.done();
        });
        return etag;
    }

    ObjectMapping complete(
        const std::string &branch,
        const std::string &upload_id,
        const std::vector<std::pair<int, std::string>> &requested_parts
    ) {
        Upload upload = load_upload(upload_id);
        if (upload.branch != branch) throw std::runtime_error("multipart upload belongs to another branch");
        auto parts = load_parts(upload_id);
        if (parts.empty()) throw std::runtime_error("multipart upload has no parts");
        std::vector<Part> selected;
        if (requested_parts.empty()) {
            selected = parts;
        } else {
            for (const auto &[number, requested_etag] : requested_parts) {
                auto found = std::find_if(parts.begin(), parts.end(), [&](const Part &part) {
                    return part.number == number;
                });
                if (found == parts.end() || (!requested_etag.empty() &&
                    found->etag != strip_quotes(requested_etag))) {
                    throw std::runtime_error("invalid multipart completion part");
                }
                selected.push_back(*found);
            }
        }
        std::string body;
        std::size_t total = 0;
        for (const auto &part : selected) total += static_cast<std::size_t>(part.size);
        body.reserve(total);
        for (const auto &part : selected) {
            UpstreamResponse response = upstream_->request(
                "GET", part.bucket, part.key
            );
            if (response.status != 200) {
                throw std::runtime_error("failed to read multipart part");
            }
            body += response.body;
        }
        ObjectMapping mapping = objects_.put(
            branch, upload.bucket, upload.key, body, upload.content_type
        );
        cleanup(upload_id, parts);
        return mapping;
    }

    void abort(const std::string &branch, const std::string &upload_id) {
        Upload upload = load_upload(upload_id);
        if (upload.branch != branch) throw std::runtime_error("multipart upload belongs to another branch");
        cleanup(upload_id, load_parts(upload_id));
    }

    struct Part {
        int number;
        std::string bucket;
        std::string key;
        std::string etag;
        std::int64_t size;
    };

    std::vector<Part> list_parts(
        const std::string &branch,
        const std::string &upload_id
    ) {
        Upload upload = load_upload(upload_id);
        if (upload.branch != branch) throw std::runtime_error("multipart upload belongs to another branch");
        return load_parts(upload_id);
    }

  private:
    class Statement {
      public:
        Statement(sqlite3 *database, const char *sql) {
            if (sqlite3_prepare_v2(database, sql, -1, &statement_, nullptr) != SQLITE_OK) {
                throw std::runtime_error(sqlite3_errmsg(database));
            }
        }
        ~Statement() { sqlite3_finalize(statement_); }
        void text(int index, const std::string &value) {
            if (sqlite3_bind_text(statement_, index, value.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
                throw std::runtime_error("failed to bind sqlite text");
            }
        }
        void integer(int index, std::int64_t value) {
            if (sqlite3_bind_int64(statement_, index, value) != SQLITE_OK) {
                throw std::runtime_error("failed to bind sqlite integer");
            }
        }
        bool row() {
            int code = sqlite3_step(statement_);
            if (code == SQLITE_ROW) return true;
            if (code == SQLITE_DONE) return false;
            throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(statement_)));
        }
        void done() {
            if (sqlite3_step(statement_) != SQLITE_DONE) {
                throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(statement_)));
            }
        }
        std::string string(int column) const {
            const unsigned char *value = sqlite3_column_text(statement_, column);
            return value ? reinterpret_cast<const char *>(value) : "";
        }
        std::int64_t number(int column) const {
            return sqlite3_column_int64(statement_, column);
        }
      private:
        sqlite3_stmt *statement_ = nullptr;
    };

    struct Upload {
        std::string branch;
        std::string bucket;
        std::string key;
        std::string content_type;
    };

    template <typename Function>
    void with_db(Function &&function) const {
        sqlite3 *database = nullptr;
        if (sqlite3_open_v2(
                database_path_.c_str(),
                &database,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                nullptr
            ) != SQLITE_OK) {
            std::string message = database ? sqlite3_errmsg(database) : "unknown sqlite error";
            if (database) sqlite3_close(database);
            throw std::runtime_error(message);
        }
        sqlite3_busy_timeout(database, 30000);
        try {
            function(database);
            sqlite3_close(database);
        } catch (...) {
            sqlite3_close(database);
            throw;
        }
    }

    static void exec(sqlite3 *database, const char *sql) {
        char *error = nullptr;
        if (sqlite3_exec(database, sql, nullptr, nullptr, &error) != SQLITE_OK) {
            std::string message = error ? error : sqlite3_errmsg(database);
            sqlite3_free(error);
            throw std::runtime_error(message);
        }
    }

    static std::string strip_quotes(std::string value) {
        value = trim(std::move(value));
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            return value.substr(1, value.size() - 2);
        }
        return value;
    }

    Upload load_upload(const std::string &upload_id) const {
        std::lock_guard<std::mutex> guard(mutex_);
        std::optional<Upload> result;
        with_db([&](sqlite3 *database) {
            Statement statement(
                database,
                "SELECT branch_id, bucket, object_key, content_type "
                "FROM chronos_s3_multipart_uploads WHERE upload_id = ?"
            );
            statement.text(1, upload_id);
            if (statement.row()) {
                result = Upload{
                    statement.string(0),
                    statement.string(1),
                    statement.string(2),
                    statement.string(3),
                };
            }
        });
        if (!result) throw std::runtime_error("unknown multipart upload");
        return *result;
    }

    std::vector<Part> load_parts(const std::string &upload_id) const {
        std::lock_guard<std::mutex> guard(mutex_);
        std::vector<Part> result;
        with_db([&](sqlite3 *database) {
            Statement statement(
                database,
                "SELECT part_number, physical_bucket, physical_key, etag, size_bytes "
                "FROM chronos_s3_multipart_parts WHERE upload_id = ? ORDER BY part_number"
            );
            statement.text(1, upload_id);
            while (statement.row()) {
                result.push_back({
                    static_cast<int>(statement.number(0)),
                    statement.string(1),
                    statement.string(2),
                    statement.string(3),
                    statement.number(4),
                });
            }
        });
        return result;
    }

    void cleanup(const std::string &upload_id, const std::vector<Part> &parts) {
        for (const auto &part : parts) {
            upstream_->request("DELETE", part.bucket, part.key);
        }
        std::lock_guard<std::mutex> guard(mutex_);
        with_db([&](sqlite3 *database) {
            exec(database, "BEGIN IMMEDIATE");
            try {
                Statement delete_parts(
                    database, "DELETE FROM chronos_s3_multipart_parts WHERE upload_id = ?"
                );
                delete_parts.text(1, upload_id);
                delete_parts.done();
                Statement delete_upload(
                    database, "DELETE FROM chronos_s3_multipart_uploads WHERE upload_id = ?"
                );
                delete_upload.text(1, upload_id);
                delete_upload.done();
                exec(database, "COMMIT");
            } catch (...) {
                exec(database, "ROLLBACK");
                throw;
            }
        });
    }

    std::string database_path_;
    std::shared_ptr<CurlS3Backend> upstream_;
    BranchableObjectStore &objects_;
    mutable std::mutex mutex_;
};

class IcebergRestCatalog {
  public:
    struct CatalogError : public std::runtime_error {
        CatalogError(int status, std::string type, std::string message)
            : std::runtime_error(std::move(message)), status(status), type(std::move(type)) {}
        int status;
        std::string type;
    };

    IcebergRestCatalog(
        BranchableObjectStore &objects,
        std::string warehouse_location
    ) : objects_(objects), warehouse_location_(std::move(warehouse_location)) {
        const std::string prefix = "s3://";
        if (warehouse_location_.rfind(prefix, 0) != 0) {
            throw std::runtime_error("Iceberg warehouse must use an s3:// location");
        }
        std::string path = warehouse_location_.substr(prefix.size());
        std::size_t slash = path.find('/');
        warehouse_bucket_ = path.substr(0, slash);
        warehouse_prefix_ = slash == std::string::npos ? "" : path.substr(slash + 1);
        while (!warehouse_prefix_.empty() && warehouse_prefix_.back() == '/') {
            warehouse_prefix_.pop_back();
        }
    }

    const std::string &warehouse_location() const { return warehouse_location_; }

    Json config() const {
        return {
            {"defaults", Json::object()},
            {"overrides", {
                {"warehouse", warehouse_location_},
            }},
            {"endpoints", {
                "GET /v1/config",
                "GET /v1/namespaces",
                "POST /v1/namespaces",
                "GET /v1/namespaces/{namespace}",
                "GET /v1/namespaces/{namespace}/tables",
                "POST /v1/namespaces/{namespace}/tables",
                "GET /v1/namespaces/{namespace}/tables/{table}",
                "POST /v1/namespaces/{namespace}/tables/{table}",
                "DELETE /v1/namespaces/{namespace}/tables/{table}",
            }},
        };
    }

    Json list_namespaces(const std::string &branch) const {
        Json namespaces = Json::array();
        for (const auto &mapping : objects_.list(
                 branch, warehouse_bucket_, namespace_prefix(), "", 10000)) {
            auto document = get_json(branch, mapping.object_key);
            if (document && document->contains("namespace")) {
                namespaces.push_back((*document)["namespace"]);
            }
        }
        return {{"namespaces", namespaces}};
    }

    Json create_namespace(const std::string &branch, const Json &request) {
        if (!request.contains("namespace") || !request["namespace"].is_array() ||
            request["namespace"].empty()) {
            throw CatalogError(400, "BadRequestException", "namespace is required");
        }
        const auto names = request["namespace"].get<std::vector<std::string>>();
        const std::string key = namespace_key(names);
        if (objects_.head(branch, warehouse_bucket_, key)) {
            throw CatalogError(409, "AlreadyExistsException", "namespace already exists");
        }
        Json value = {
            {"namespace", names},
            {"properties", request.value("properties", Json::object())},
        };
        put_json(branch, key, value, std::nullopt, true);
        return value;
    }

    Json load_namespace(
        const std::string &branch,
        const std::vector<std::string> &names
    ) const {
        auto value = get_json(branch, namespace_key(names));
        if (!value) throw CatalogError(404, "NoSuchNamespaceException", "namespace does not exist");
        return *value;
    }

    Json list_tables(
        const std::string &branch,
        const std::vector<std::string> &names
    ) const {
        (void)load_namespace(branch, names);
        Json identifiers = Json::array();
        const std::string prefix = table_prefix(names);
        for (const auto &mapping : objects_.list(
                 branch, warehouse_bucket_, prefix, "", 10000)) {
            if (mapping.object_key.size() < 10 ||
                mapping.object_key.substr(mapping.object_key.size() - 10) != "/head.json") {
                continue;
            }
            auto head = get_json(branch, mapping.object_key);
            if (head && !head->value("staged", false) && head->contains("identifier")) {
                identifiers.push_back((*head)["identifier"]);
            }
        }
        return {{"identifiers", identifiers}};
    }

    Json create_table(
        const std::string &branch,
        const std::vector<std::string> &names,
        const Json &request
    ) {
        (void)load_namespace(branch, names);
        const std::string table_name = request.value("name", "");
        const bool staged = request.value("stage-create", false);
        if (table_name.empty() || !request.contains("schema")) {
            throw CatalogError(400, "BadRequestException", "table name and schema are required");
        }
        const std::string head_key = table_head_key(names, table_name);
        if (objects_.head(branch, warehouse_bucket_, head_key)) {
            throw CatalogError(409, "AlreadyExistsException", "table already exists");
        }

        const std::string location = request.value(
            "location",
            table_location(names, table_name)
        );
        Json schema = request["schema"];
        schema["schema-id"] = schema.value("schema-id", 0);
        Json partition_spec = request.value(
            "partition-spec",
            Json{{"spec-id", 0}, {"fields", Json::array()}}
        );
        partition_spec.erase("type");
        partition_spec["spec-id"] = partition_spec.value("spec-id", 0);
        Json sort_order = request.value(
            "write-order",
            Json{{"order-id", 0}, {"fields", Json::array()}}
        );
        sort_order["order-id"] = sort_order.value("order-id", 0);
        Json properties = request.value("properties", Json::object());
        int format_version = 2;
        if (properties.contains("format-version")) {
            format_version = std::stoi(properties["format-version"].get<std::string>());
        }
        properties.erase("format-version");

        Json metadata = {
            {"format-version", format_version},
            {"table-uuid", uuid()},
            {"location", location},
            {"last-sequence-number", 0},
            {"last-updated-ms", unix_millis()},
            {"last-column-id", max_field_id(schema)},
            {"schemas", Json::array({schema})},
            {"current-schema-id", schema["schema-id"]},
            {"partition-specs", Json::array({partition_spec})},
            {"default-spec-id", partition_spec["spec-id"]},
            {"last-partition-id", max_partition_id(partition_spec)},
            {"properties", properties},
            {"snapshots", Json::array()},
            {"snapshot-log", Json::array()},
            {"metadata-log", Json::array()},
            {"sort-orders", Json::array({sort_order})},
            {"default-sort-order-id", sort_order["order-id"]},
            {"refs", Json::object()},
        };
        const std::string metadata_location = write_metadata(branch, metadata, 0);
        Json head = {
            {"identifier", {{"namespace", names}, {"name", table_name}}},
            {"metadata-location", metadata_location},
            {"version", 0},
            {"staged", staged},
        };
        put_json(branch, head_key, head, std::nullopt, true);
        return load_result(metadata_location, metadata);
    }

    Json load_table(
        const std::string &branch,
        const std::vector<std::string> &names,
        const std::string &table_name
    ) const {
        auto head = get_json(branch, table_head_key(names, table_name));
        if (!head) throw CatalogError(404, "NoSuchTableException", "table does not exist");
        const std::string metadata_location = (*head)["metadata-location"].get<std::string>();
        auto metadata = get_location_json(branch, metadata_location);
        if (!metadata) {
            throw CatalogError(500, "RuntimeException", "table metadata object is missing");
        }
        return load_result(metadata_location, *metadata);
    }

    Json commit_table(
        const std::string &branch,
        const std::vector<std::string> &names,
        const std::string &table_name,
        const Json &request
    ) {
        const std::string head_key = table_head_key(names, table_name);
        auto head_mapping = objects_.head(branch, warehouse_bucket_, head_key);
        if (!head_mapping) throw CatalogError(404, "NoSuchTableException", "table does not exist");
        auto head = get_json(branch, head_key);
        if (!head) throw CatalogError(404, "NoSuchTableException", "table does not exist");
        const std::string old_location = (*head)["metadata-location"].get<std::string>();
        auto metadata_value = get_location_json(branch, old_location);
        if (!metadata_value) {
            throw CatalogError(500, "RuntimeException", "table metadata object is missing");
        }
        Json metadata = *metadata_value;
        const bool staged = head->value("staged", false);
        validate_requirements(
            metadata,
            request.value("requirements", Json::array()),
            staged
        );
        for (const auto &update : request.value("updates", Json::array())) {
            apply_update(metadata, update);
        }
        metadata["last-updated-ms"] = unix_millis();
        if (!metadata.contains("metadata-log")) metadata["metadata-log"] = Json::array();
        metadata["metadata-log"].push_back({
            {"timestamp-ms", metadata["last-updated-ms"]},
            {"metadata-file", old_location},
        });
        const std::int64_t version = head->value("version", 0) + 1;
        const std::string new_location = write_metadata(branch, metadata, version);
        (*head)["metadata-location"] = new_location;
        (*head)["version"] = version;
        (*head)["staged"] = false;
        try {
            put_json(branch, head_key, *head, head_mapping->etag, false);
        } catch (const std::exception &error) {
            if (std::string(error.what()).rfind("precondition failed", 0) == 0) {
                throw CatalogError(409, "CommitFailedException", "table metadata changed concurrently");
            }
            throw;
        }
        return load_result(new_location, metadata);
    }

    void drop_table(
        const std::string &branch,
        const std::vector<std::string> &names,
        const std::string &table_name
    ) {
        if (!objects_.erase(branch, warehouse_bucket_, table_head_key(names, table_name))) {
            throw CatalogError(404, "NoSuchTableException", "table does not exist");
        }
    }

  private:
    static std::string uuid() {
        std::string value = random_id();
        return value.substr(0, 8) + "-" + value.substr(8, 4) + "-" +
            value.substr(12, 4) + "-" + value.substr(16, 4) + "-" +
            value.substr(20, 12);
    }

    static int max_field_id(const Json &schema) {
        int maximum = 0;
        std::function<void(const Json &)> visit = [&](const Json &type) {
            if (!type.is_object()) return;
            if (type.contains("id") && type["id"].is_number_integer()) {
                maximum = std::max(maximum, type["id"].get<int>());
            }
            for (const char *name : {"fields", "element", "key", "value"}) {
                if (!type.contains(name)) continue;
                if (type[name].is_array()) {
                    for (const auto &child : type[name]) visit(child);
                } else {
                    visit(type[name]);
                }
            }
            if (type.contains("element-id")) maximum = std::max(maximum, type["element-id"].get<int>());
            if (type.contains("key-id")) maximum = std::max(maximum, type["key-id"].get<int>());
            if (type.contains("value-id")) maximum = std::max(maximum, type["value-id"].get<int>());
        };
        visit(schema);
        return maximum;
    }

    static int max_partition_id(const Json &spec) {
        int maximum = 999;
        for (const auto &field : spec.value("fields", Json::array())) {
            maximum = std::max(maximum, field.value("field-id", 999));
        }
        return maximum;
    }

    static std::string join(const std::vector<std::string> &parts, const std::string &separator) {
        std::ostringstream out;
        for (std::size_t i = 0; i < parts.size(); ++i) {
            if (i) out << separator;
            out << parts[i];
        }
        return out.str();
    }

    std::string namespace_prefix() const {
        return "_chronos/catalog/namespaces/";
    }

    std::string namespace_key(const std::vector<std::string> &names) const {
        return namespace_prefix() + url_encode(join(names, "\x1f")) + ".json";
    }

    std::string table_prefix(const std::vector<std::string> &names) const {
        return "_chronos/catalog/tables/" + url_encode(join(names, "\x1f")) + "/";
    }

    std::string table_head_key(
        const std::vector<std::string> &names,
        const std::string &table_name
    ) const {
        return table_prefix(names) + url_encode(table_name) + "/head.json";
    }

    std::string table_location(
        const std::vector<std::string> &names,
        const std::string &table_name
    ) const {
        std::string suffix = join(names, "/") + "/" + table_name;
        return warehouse_location_ + "/" + suffix;
    }

    static std::pair<std::string, std::string> parse_s3_location(
        const std::string &location
    ) {
        if (location.rfind("s3://", 0) != 0) {
            throw CatalogError(400, "BadRequestException", "metadata location must use s3://");
        }
        std::string path = location.substr(std::strlen("s3://"));
        std::size_t slash = path.find('/');
        if (slash == std::string::npos) {
            throw CatalogError(400, "BadRequestException", "metadata location must include an object key");
        }
        return {path.substr(0, slash), path.substr(slash + 1)};
    }

    std::optional<Json> get_json(
        const std::string &branch,
        const std::string &key
    ) const {
        auto object = objects_.get(branch, warehouse_bucket_, key);
        if (!object) return std::nullopt;
        return Json::parse(object->second.body);
    }

    std::optional<Json> get_location_json(
        const std::string &branch,
        const std::string &location
    ) const {
        auto [bucket, key] = parse_s3_location(location);
        auto object = objects_.get(branch, bucket, key);
        if (!object) return std::nullopt;
        return Json::parse(object->second.body);
    }

    ObjectMapping put_json(
        const std::string &branch,
        const std::string &key,
        const Json &value,
        const std::optional<std::string> &if_match,
        bool if_none_match
    ) {
        return objects_.put(
            branch,
            warehouse_bucket_,
            key,
            value.dump(),
            "application/json",
            "",
            "",
            "{}",
            if_match,
            if_none_match
        );
    }

    std::string write_metadata(
        const std::string &branch,
        const Json &metadata,
        std::int64_t version
    ) {
        auto [bucket, base_key] = parse_s3_location(metadata["location"].get<std::string>());
        std::ostringstream filename;
        filename << base_key << "/metadata/" << std::setfill('0') << std::setw(5)
                 << version << "-" << uuid() << ".metadata.json";
        objects_.put(
            branch,
            bucket,
            filename.str(),
            metadata.dump(),
            "application/json"
        );
        return "s3://" + bucket + "/" + filename.str();
    }

    static Json load_result(const std::string &location, const Json &metadata) {
        return {
            {"metadata-location", location},
            {"metadata", metadata},
            {"config", Json::object()},
        };
    }

    static const Json *snapshot_ref(const Json &metadata, const std::string &name) {
        if (!metadata.contains("refs") || !metadata["refs"].contains(name)) return nullptr;
        return &metadata["refs"][name];
    }

    static void validate_requirements(
        const Json &metadata,
        const Json &requirements,
        bool staged_create
    ) {
        for (const auto &requirement : requirements) {
            const std::string type = requirement.value("type", "");
            bool valid = true;
            if (type == "assert-table-uuid") {
                valid = metadata.value("table-uuid", "") == requirement.value("uuid", "");
            } else if (type == "assert-ref-snapshot-id") {
                const std::string ref = requirement.value("ref", "main");
                const Json *current = snapshot_ref(metadata, ref);
                if (requirement.contains("snapshot-id") && !requirement["snapshot-id"].is_null()) {
                    valid = current && current->value("snapshot-id", std::int64_t{-1}) ==
                        requirement["snapshot-id"].get<std::int64_t>();
                } else {
                    valid = current == nullptr;
                }
            } else if (type == "assert-last-assigned-field-id") {
                valid = metadata.value("last-column-id", -1) ==
                    requirement.value("last-assigned-field-id", -2);
            } else if (type == "assert-current-schema-id") {
                valid = metadata.value("current-schema-id", -1) ==
                    requirement.value("current-schema-id", -2);
            } else if (type == "assert-last-assigned-partition-id") {
                valid = metadata.value("last-partition-id", -1) ==
                    requirement.value("last-assigned-partition-id", -2);
            } else if (type == "assert-default-spec-id") {
                valid = metadata.value("default-spec-id", -1) ==
                    requirement.value("default-spec-id", -2);
            } else if (type == "assert-default-sort-order-id") {
                valid = metadata.value("default-sort-order-id", -1) ==
                    requirement.value("default-sort-order-id", -2);
            } else if (type == "assert-create") {
                valid = staged_create;
            }
            if (!valid) {
                throw CatalogError(409, "CommitFailedException", "Iceberg commit requirement failed: " + type);
            }
        }
    }

    static void replace_by_id(Json &array, const std::string &id_name, const Json &value) {
        const auto id = value.at(id_name);
        for (auto &existing : array) {
            if (existing.at(id_name) == id) {
                existing = value;
                return;
            }
        }
        array.push_back(value);
    }

    static void apply_update(Json &metadata, const Json &update) {
        const std::string action = update.value("action", "");
        if (action == "assign-uuid") {
            metadata["table-uuid"] = update["uuid"];
        } else if (action == "upgrade-format-version") {
            metadata["format-version"] = update["format-version"];
        } else if (action == "add-schema") {
            Json schema = update["schema"];
            replace_by_id(metadata["schemas"], "schema-id", schema);
            metadata["last-column-id"] = std::max(
                metadata.value("last-column-id", 0),
                update.value("last-column-id", max_field_id(schema))
            );
        } else if (action == "set-current-schema") {
            int schema_id = update.value("schema-id", -1);
            if (schema_id == -1 && !metadata["schemas"].empty()) {
                schema_id = metadata["schemas"].back()["schema-id"].get<int>();
            }
            metadata["current-schema-id"] = schema_id;
        } else if (action == "add-spec") {
            Json spec = update["spec"];
            replace_by_id(metadata["partition-specs"], "spec-id", spec);
            metadata["last-partition-id"] = std::max(
                metadata.value("last-partition-id", 999),
                max_partition_id(spec)
            );
        } else if (action == "set-default-spec") {
            int spec_id = update.value("spec-id", -1);
            if (spec_id == -1 && !metadata["partition-specs"].empty()) {
                spec_id = metadata["partition-specs"].back()["spec-id"].get<int>();
            }
            metadata["default-spec-id"] = spec_id;
        } else if (action == "add-sort-order") {
            replace_by_id(metadata["sort-orders"], "order-id", update["sort-order"]);
        } else if (action == "set-default-sort-order") {
            int order_id = update.value("sort-order-id", -1);
            if (order_id == -1 && !metadata["sort-orders"].empty()) {
                order_id = metadata["sort-orders"].back()["order-id"].get<int>();
            }
            metadata["default-sort-order-id"] = order_id;
        } else if (action == "add-snapshot") {
            Json snapshot = update["snapshot"];
            replace_by_id(metadata["snapshots"], "snapshot-id", snapshot);
            metadata["last-sequence-number"] = std::max(
                metadata.value("last-sequence-number", std::int64_t{0}),
                snapshot.value("sequence-number", std::int64_t{0})
            );
        } else if (action == "set-snapshot-ref") {
            const std::string ref_name = update["ref-name"].get<std::string>();
            Json ref;
            if (update.contains("snapshot-ref")) {
                ref = update["snapshot-ref"];
            } else {
                ref = {
                    {"type", update.value("type", "branch")},
                    {"snapshot-id", update["snapshot-id"]},
                };
                for (const char *property : {
                         "max-ref-age-ms",
                         "max-snapshot-age-ms",
                         "min-snapshots-to-keep",
                     }) {
                    if (update.contains(property)) ref[property] = update[property];
                }
            }
            metadata["refs"][ref_name] = ref;
            if (ref_name == "main") {
                metadata["current-snapshot-id"] = ref["snapshot-id"];
                metadata["snapshot-log"].push_back({
                    {"timestamp-ms", unix_millis()},
                    {"snapshot-id", ref["snapshot-id"]},
                });
            }
        } else if (action == "remove-snapshots") {
            const auto ids = update.value("snapshot-ids", std::vector<std::int64_t>{});
            Json retained = Json::array();
            for (const auto &snapshot : metadata["snapshots"]) {
                if (std::find(ids.begin(), ids.end(), snapshot["snapshot-id"].get<std::int64_t>()) == ids.end()) {
                    retained.push_back(snapshot);
                }
            }
            metadata["snapshots"] = std::move(retained);
        } else if (action == "remove-snapshot-ref") {
            metadata["refs"].erase(update["ref-name"].get<std::string>());
        } else if (action == "set-location") {
            metadata["location"] = update["location"];
        } else if (action == "set-properties") {
            for (auto item = update["updates"].begin(); item != update["updates"].end(); ++item) {
                metadata["properties"][item.key()] = item.value();
            }
        } else if (action == "remove-properties") {
            for (const auto &name : update["removals"]) {
                metadata["properties"].erase(name.get<std::string>());
            }
        } else if (action == "set-statistics") {
            if (!metadata.contains("statistics")) metadata["statistics"] = Json::array();
            replace_by_id(metadata["statistics"], "snapshot-id", update["statistics"]);
        } else if (action == "remove-statistics") {
            const auto id = update["snapshot-id"];
            Json retained = Json::array();
            for (const auto &item : metadata.value("statistics", Json::array())) {
                if (item["snapshot-id"] != id) retained.push_back(item);
            }
            metadata["statistics"] = std::move(retained);
        } else if (action == "set-partition-statistics") {
            if (!metadata.contains("partition-statistics")) metadata["partition-statistics"] = Json::array();
            replace_by_id(metadata["partition-statistics"], "snapshot-id", update["partition-statistics"]);
        } else if (action == "remove-partition-statistics") {
            const auto id = update["snapshot-id"];
            Json retained = Json::array();
            for (const auto &item : metadata.value("partition-statistics", Json::array())) {
                if (item["snapshot-id"] != id) retained.push_back(item);
            }
            metadata["partition-statistics"] = std::move(retained);
        } else {
            throw CatalogError(400, "UnsupportedOperationException", "unsupported Iceberg update: " + action);
        }
    }

    BranchableObjectStore &objects_;
    std::string warehouse_location_;
    std::string warehouse_bucket_;
    std::string warehouse_prefix_;
};

class ChronosLakeServer {
  public:
    ChronosLakeServer(
        std::string database_path,
        std::string upstream_endpoint,
        std::string upstream_access_key,
        std::string upstream_secret_key,
        std::string host,
        std::uint16_t port,
        std::string region,
        std::string client_access_key,
        std::string client_secret_key,
        std::string warehouse_location,
        std::size_t worker_threads,
        std::int64_t gc_interval_seconds,
        std::int64_t gc_grace_seconds
    ) : host_(std::move(host)),
        port_(port),
        upstream_(std::make_shared<CurlS3Backend>(
            upstream_endpoint,
            upstream_access_key,
            upstream_secret_key,
            region
        )),
        objects_(database_path, upstream_),
        multipart_(database_path, upstream_, objects_),
        catalog_(objects_, std::move(warehouse_location)),
        worker_count_(std::max<std::size_t>(1, worker_threads)),
        gc_interval_seconds_(std::max<std::int64_t>(0, gc_interval_seconds)),
        gc_grace_ms_(std::max<std::int64_t>(0, gc_grace_seconds) * 1000) {
        bind_credentials(
            client_access_key.empty() ? upstream_access_key : client_access_key,
            client_secret_key.empty() ? upstream_secret_key : client_secret_key,
            "main"
        );
    }

    ~ChronosLakeServer() { stop(); }

    void start() {
        if (running_.exchange(true)) {
            return;
        }
        workers_.reserve(worker_count_);
        for (std::size_t i = 0; i < worker_count_; ++i) {
            workers_.emplace_back([this]() { worker_loop(); });
        }
        if (gc_interval_seconds_ > 0) {
            gc_thread_ = std::thread([this]() { gc_loop(); });
        }
        thread_ = std::thread([this]() { serve(); });
        for (int attempt = 0; attempt < 100 && !ready_.load(); ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!ready_.load()) {
            stop();
            throw std::runtime_error("Chronos lake server failed to start");
        }
    }

    void stop() {
        const bool was_running = running_.exchange(false);
        if (!was_running && !thread_.joinable() && workers_.empty()) {
            return;
        }
        if (was_running && ready_.load()) {
            asio::io_context wake_context;
            tcp::socket wake_socket(wake_context);
            boost::system::error_code ignored;
            wake_socket.connect(
                tcp::endpoint(asio::ip::make_address(host_), port_),
                ignored
            );
            wake_socket.shutdown(tcp::socket::shutdown_both, ignored);
            wake_socket.close(ignored);
        }
        gc_condition_.notify_all();
        {
            std::lock_guard<std::mutex> guard(active_sockets_mutex_);
            for (int descriptor : active_sockets_) {
                ::shutdown(descriptor, SHUT_RDWR);
            }
        }
        if (thread_.joinable()) {
            thread_.join();
        }
        queue_condition_.notify_all();
        for (auto &worker : workers_) {
            if (worker.joinable()) worker.join();
        }
        workers_.clear();
        if (gc_thread_.joinable()) gc_thread_.join();
        {
            std::lock_guard<std::mutex> guard(queue_mutex_);
            pending_sockets_.clear();
        }
        acceptor_.reset();
        io_context_.reset();
        ready_.store(false);
    }

    bool running() const { return running_.load(); }
    std::uint16_t port() const { return port_; }

    void create_bucket(const std::string &bucket) { objects_.create_bucket(bucket); }

    ObjectMapping put_object(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key,
        const std::string &body,
        const std::string &content_type
    ) {
        return objects_.put(branch, bucket, key, body, content_type);
    }

    std::optional<std::string> get_object(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key
    ) const {
        auto object = objects_.get(branch, bucket, key);
        if (!object) return std::nullopt;
        return object->second.body;
    }

    bool delete_object(
        const std::string &branch,
        const std::string &bucket,
        const std::string &key
    ) {
        return objects_.erase(branch, bucket, key);
    }

    std::vector<std::string> list_object_keys(
        const std::string &branch,
        const std::string &bucket,
        const std::string &prefix
    ) const {
        std::vector<std::string> result;
        for (const auto &mapping : objects_.list(branch, bucket, prefix, "", 10000)) {
            result.push_back(mapping.object_key);
        }
        return result;
    }

    void create_branch(const std::string &branch, const std::string &from_branch) {
        objects_.create_branch(branch, from_branch);
    }

    void delete_branch(const std::string &branch) { objects_.delete_branch(branch); }
    std::vector<std::string> branches() const { return objects_.branches(); }
    std::size_t bootstrap(const std::string &branch, const std::string &bucket) {
        return objects_.bootstrap(branch, bucket);
    }
    std::size_t collect_garbage(std::int64_t grace_seconds) {
        return objects_.collect_garbage(std::max<std::int64_t>(0, grace_seconds) * 1000);
    }

    void bind_credentials(
        const std::string &access_key,
        const std::string &secret_key,
        const std::string &branch
    ) {
        if (access_key.empty() || secret_key.empty()) {
            throw std::runtime_error("ChronosS3 credentials cannot be empty");
        }
        auto branch_names = objects_.branches();
        if (std::find(branch_names.begin(), branch_names.end(), branch) == branch_names.end()) {
            throw std::runtime_error("unknown ChronosS3 branch: " + branch);
        }
        std::lock_guard<std::mutex> guard(credentials_mutex_);
        credentials_[access_key] = {secret_key, branch};
    }

    void bind_catalog_token(const std::string &token, const std::string &branch) {
        if (token.empty()) throw std::runtime_error("Iceberg catalog token cannot be empty");
        auto branch_names = objects_.branches();
        if (std::find(branch_names.begin(), branch_names.end(), branch) == branch_names.end()) {
            throw std::runtime_error("unknown ChronosS3 branch: " + branch);
        }
        std::lock_guard<std::mutex> guard(catalog_tokens_mutex_);
        catalog_tokens_[token] = branch;
    }

  private:
    void serve() {
        try {
            io_context_ = std::make_unique<asio::io_context>();
            auto address = asio::ip::make_address(host_);
            acceptor_ = std::make_unique<tcp::acceptor>(
                *io_context_, tcp::endpoint(address, port_)
            );
            port_ = acceptor_->local_endpoint().port();
            ready_.store(true);

            while (running_.load()) {
                tcp::socket socket(*io_context_);
                boost::system::error_code error;
                acceptor_->accept(socket, error);
                if (error) {
                    if (running_.load()) {
                        last_error_ = error.message();
                    }
                    continue;
                }
                if (!running_.load()) {
                    break;
                }
                {
                    std::lock_guard<std::mutex> guard(queue_mutex_);
                    pending_sockets_.push_back(std::move(socket));
                }
                queue_condition_.notify_one();
            }
            boost::system::error_code ignored;
            acceptor_->close(ignored);
        } catch (const std::exception &error) {
            last_error_ = error.what();
            ready_.store(false);
            running_.store(false);
            queue_condition_.notify_all();
        }
    }

    void worker_loop() {
        while (true) {
            std::optional<tcp::socket> socket;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_condition_.wait(lock, [this]() {
                    return !running_.load() || !pending_sockets_.empty();
                });
                if (pending_sockets_.empty()) {
                    if (!running_.load()) return;
                    continue;
                }
                socket.emplace(std::move(pending_sockets_.front()));
                pending_sockets_.pop_front();
            }
            try {
                handle(std::move(*socket));
            } catch (...) {
            }
        }
    }

    void gc_loop() {
        std::unique_lock<std::mutex> lock(gc_condition_mutex_);
        while (running_.load()) {
            if (gc_condition_.wait_for(
                    lock,
                    std::chrono::seconds(gc_interval_seconds_),
                    [this]() { return !running_.load(); }
                )) {
                break;
            }
            lock.unlock();
            try {
                objects_.collect_garbage(gc_grace_ms_);
            } catch (...) {
            }
            lock.lock();
        }
    }

    struct Credential {
        std::string secret;
        std::string branch;
    };

    static std::string request_header(
        const http::request<http::string_body> &request,
        const std::string &name
    ) {
        auto found = request.find(name);
        return found == request.end() ? "" : std::string(found->value());
    }

    std::string authenticate_s3(const http::request<http::string_body> &request) const {
        const std::string authorization = request_header(request, "authorization");
        if (authorization.rfind("AWS4-HMAC-SHA256 ", 0) != 0) {
            throw std::runtime_error("missing AWS Signature Version 4 authorization");
        }
        std::map<std::string, std::string> attributes;
        std::string fields = authorization.substr(std::strlen("AWS4-HMAC-SHA256 "));
        std::size_t start = 0;
        while (start < fields.size()) {
            std::size_t comma = fields.find(',', start);
            if (comma == std::string::npos) comma = fields.size();
            std::string field = trim(fields.substr(start, comma - start));
            std::size_t equals = field.find('=');
            if (equals != std::string::npos) {
                attributes[field.substr(0, equals)] = field.substr(equals + 1);
            }
            start = comma + 1;
        }
        auto credential_it = attributes.find("Credential");
        auto signed_it = attributes.find("SignedHeaders");
        auto signature_it = attributes.find("Signature");
        if (credential_it == attributes.end() ||
            signed_it == attributes.end() ||
            signature_it == attributes.end()) {
            throw std::runtime_error("invalid AWS authorization fields");
        }

        std::vector<std::string> scope;
        std::stringstream credential_stream(credential_it->second);
        std::string component;
        while (std::getline(credential_stream, component, '/')) scope.push_back(component);
        if (scope.size() != 5 || scope[3] != "s3" || scope[4] != "aws4_request") {
            throw std::runtime_error("invalid AWS credential scope");
        }

        Credential credential;
        {
            std::lock_guard<std::mutex> guard(credentials_mutex_);
            auto found = credentials_.find(scope[0]);
            if (found == credentials_.end()) {
                throw std::runtime_error("unknown ChronosS3 access key");
            }
            credential = found->second;
        }

        std::string target = std::string(request.target());
        std::size_t question = target.find('?');
        std::string canonical_uri = question == std::string::npos
            ? target : target.substr(0, question);
        std::string query = question == std::string::npos
            ? "" : target.substr(question + 1);
        std::vector<std::pair<std::string, std::string>> query_items;
        std::size_t query_start = 0;
        while (query_start <= query.size() && !query.empty()) {
            std::size_t end = query.find('&', query_start);
            if (end == std::string::npos) end = query.size();
            std::string pair = query.substr(query_start, end - query_start);
            std::size_t equals = pair.find('=');
            query_items.emplace_back(
                url_encode(url_decode(pair.substr(0, equals))),
                equals == std::string::npos ? "" :
                    url_encode(url_decode(pair.substr(equals + 1)))
            );
            if (end == query.size()) break;
            query_start = end + 1;
        }
        std::sort(query_items.begin(), query_items.end());
        std::ostringstream canonical_query;
        for (std::size_t i = 0; i < query_items.size(); ++i) {
            if (i) canonical_query << '&';
            canonical_query << query_items[i].first << '=' << query_items[i].second;
        }

        std::ostringstream canonical_headers;
        std::stringstream signed_stream(signed_it->second);
        while (std::getline(signed_stream, component, ';')) {
            std::string value = request_header(request, component);
            if (value.empty() && component == "host") {
                value = request_header(request, "Host");
            }
            canonical_headers << lower(component) << ':' << normalize_header_value(value) << '\n';
        }
        std::string payload_hash = request_header(request, "x-amz-content-sha256");
        if (payload_hash.empty()) payload_hash = hex(sha256(request.body()));
        std::string canonical_request =
            std::string(request.method_string()) + "\n" +
            canonical_uri + "\n" +
            canonical_query.str() + "\n" +
            canonical_headers.str() + "\n" +
            signed_it->second + "\n" +
            payload_hash;
        std::string timestamp = request_header(request, "x-amz-date");
        if (timestamp.empty()) throw std::runtime_error("missing x-amz-date");
        std::string string_to_sign =
            "AWS4-HMAC-SHA256\n" + timestamp + "\n" +
            scope[1] + "/" + scope[2] + "/s3/aws4_request\n" +
            hex(sha256(canonical_request));
        auto date_key = hmac_sha256("AWS4" + credential.secret, scope[1]);
        auto region_key = hmac_sha256(date_key, scope[2]);
        auto service_key = hmac_sha256(region_key, "s3");
        auto signing_key = hmac_sha256(service_key, "aws4_request");
        std::string expected = hex(hmac_sha256(signing_key, string_to_sign));
        if (expected != lower(signature_it->second)) {
            throw std::runtime_error("AWS request signature mismatch");
        }
        return credential.branch;
    }

    static http::response<http::string_body> json_response(
        const http::request<http::string_body> &request,
        http::status status,
        const Json &body
    ) {
        http::response<http::string_body> response(status, request.version());
        response.keep_alive(false);
        response.set(http::field::server, "chronos-lake");
        response.set(http::field::content_type, "application/json");
        response.body() = body.dump();
        response.prepare_payload();
        return response;
    }

    static http::response<http::string_body> s3_error(
        const http::request<http::string_body> &request,
        http::status status,
        const std::string &code,
        const std::string &message
    ) {
        http::response<http::string_body> response(status, request.version());
        response.keep_alive(false);
        response.set(http::field::server, "chronos-lake");
        response.set(http::field::content_type, "application/xml");
        response.body() =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Error><Code>" + code + "</Code><Message>" + message +
            "</Message></Error>";
        response.prepare_payload();
        return response;
    }

    static std::vector<std::string> split_path(const std::string &path) {
        std::vector<std::string> parts;
        std::size_t start = 0;
        while (start < path.size()) {
            std::size_t slash = path.find('/', start);
            if (slash == std::string::npos) slash = path.size();
            if (slash > start) parts.push_back(url_decode(path.substr(start, slash - start)));
            start = slash + 1;
        }
        return parts;
    }

    static std::vector<std::string> split_namespace(const std::string &encoded) {
        std::vector<std::string> result;
        std::size_t start = 0;
        while (start <= encoded.size()) {
            std::size_t separator = encoded.find('\x1f', start);
            if (separator == std::string::npos) separator = encoded.size();
            result.push_back(encoded.substr(start, separator - start));
            if (separator == encoded.size()) break;
            start = separator + 1;
        }
        return result;
    }

    http::response<http::string_body> handle_iceberg(
        const http::request<http::string_body> &request
    ) {
        try {
            std::string target = std::string(request.target());
            std::size_t question = target.find('?');
            std::string path = question == std::string::npos ? target : target.substr(0, question);
            const std::string base = "/iceberg/v1";
            if (path.rfind(base, 0) != 0) {
                throw IcebergRestCatalog::CatalogError(404, "NoSuchEndpointException", "unknown Iceberg endpoint");
            }
            std::string suffix = path.substr(base.size());
            if (!suffix.empty() && suffix.front() == '/') suffix.erase(suffix.begin());
            const auto parts = split_path(suffix);
            std::string branch = "main";
            const std::string authorization = request_header(request, "authorization");
            if (authorization.rfind("Bearer ", 0) == 0) {
                std::lock_guard<std::mutex> guard(catalog_tokens_mutex_);
                auto found = catalog_tokens_.find(authorization.substr(std::strlen("Bearer ")));
                if (found == catalog_tokens_.end()) {
                    throw IcebergRestCatalog::CatalogError(
                        401, "NotAuthorizedException", "unknown Iceberg catalog token"
                    );
                }
                branch = found->second;
            } else {
                const std::string branch_header = request_header(request, "x-chronos-branch");
                if (!branch_header.empty()) branch = branch_header;
            }

            if (parts.size() == 1 && parts[0] == "config" &&
                request.method() == http::verb::get) {
                return json_response(request, http::status::ok, catalog_.config());
            }
            if (parts.size() == 1 && parts[0] == "namespaces") {
                if (request.method() == http::verb::get) {
                    return json_response(
                        request, http::status::ok, catalog_.list_namespaces(branch)
                    );
                }
                if (request.method() == http::verb::post) {
                    return json_response(
                        request,
                        http::status::ok,
                        catalog_.create_namespace(branch, Json::parse(request.body()))
                    );
                }
            }
            if (parts.size() >= 2 && parts[0] == "namespaces") {
                const auto names = split_namespace(parts[1]);
                if (parts.size() == 2 && request.method() == http::verb::get) {
                    return json_response(
                        request, http::status::ok, catalog_.load_namespace(branch, names)
                    );
                }
                if (parts.size() == 3 && parts[2] == "tables") {
                    if (request.method() == http::verb::get) {
                        return json_response(
                            request, http::status::ok, catalog_.list_tables(branch, names)
                        );
                    }
                    if (request.method() == http::verb::post) {
                        return json_response(
                            request,
                            http::status::ok,
                            catalog_.create_table(branch, names, Json::parse(request.body()))
                        );
                    }
                }
                if (parts.size() == 4 && parts[2] == "tables") {
                    const std::string &table_name = parts[3];
                    if (request.method() == http::verb::get) {
                        return json_response(
                            request,
                            http::status::ok,
                            catalog_.load_table(branch, names, table_name)
                        );
                    }
                    if (request.method() == http::verb::post) {
                        return json_response(
                            request,
                            http::status::ok,
                            catalog_.commit_table(
                                branch, names, table_name, Json::parse(request.body())
                            )
                        );
                    }
                    if (request.method() == http::verb::delete_) {
                        catalog_.drop_table(branch, names, table_name);
                        http::response<http::string_body> response(
                            http::status::no_content, request.version()
                        );
                        response.keep_alive(false);
                        return response;
                    }
                }
            }
            throw IcebergRestCatalog::CatalogError(
                404, "NoSuchEndpointException", "unknown Iceberg endpoint"
            );
        } catch (const IcebergRestCatalog::CatalogError &error) {
            return json_response(
                request,
                static_cast<http::status>(error.status),
                {{"error", {
                    {"message", error.what()},
                    {"type", error.type},
                    {"code", error.status},
                }}}
            );
        } catch (const Json::exception &error) {
            return json_response(
                request,
                http::status::bad_request,
                {{"error", {
                    {"message", error.what()},
                    {"type", "BadRequestException"},
                    {"code", 400},
                }}}
            );
        } catch (const std::exception &error) {
            return json_response(
                request,
                http::status::internal_server_error,
                {{"error", {
                    {"message", error.what()},
                    {"type", "RuntimeException"},
                    {"code", 500},
                }}}
            );
        }
    }

    http::response<http::string_body> handle_s3(
        const http::request<http::string_body> &request
    ) {
        std::string branch;
        try {
            branch = authenticate_s3(request);
        } catch (const std::exception &error) {
            return s3_error(request, http::status::forbidden, "SignatureDoesNotMatch", error.what());
        }
        std::string target = std::string(request.target());
        std::size_t question = target.find('?');
        std::string path = question == std::string::npos ? target : target.substr(0, question);
        std::string query_text = question == std::string::npos ? "" : target.substr(question + 1);
        if (!path.empty() && path.front() == '/') path.erase(path.begin());
        std::size_t slash = path.find('/');
        std::string bucket = url_decode(path.substr(0, slash));
        std::string key = slash == std::string::npos ? "" : url_decode(path.substr(slash + 1));
        auto query = parse_query(query_text);
        if (bucket.empty()) {
            return s3_error(request, http::status::bad_request, "InvalidBucketName", "bucket is required");
        }

        try {
            if (key.empty() && request.method() == http::verb::put) {
                objects_.create_bucket(bucket);
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.prepare_payload();
                return response;
            }
            if (key.empty() && request.method() == http::verb::get &&
                query.find("list-type") != query.end()) {
                const std::string prefix = query["prefix"];
                const std::string start_after = query.count("continuation-token")
                    ? query["continuation-token"] : query["start-after"];
                const std::size_t max_keys = query.count("max-keys")
                    ? std::min<std::size_t>(1000, std::stoull(query["max-keys"]))
                    : 1000;
                auto mappings = objects_.list(branch, bucket, prefix, start_after, max_keys + 1);
                const bool truncated = mappings.size() > max_keys;
                if (truncated) mappings.resize(max_keys);
                pugi::xml_document document;
                auto root = document.append_child("ListBucketResult");
                root.append_attribute("xmlns") = "http://s3.amazonaws.com/doc/2006-03-01/";
                root.append_child("Name").text().set(bucket.c_str());
                root.append_child("Prefix").text().set(prefix.c_str());
                root.append_child("KeyCount").text().set(static_cast<unsigned int>(mappings.size()));
                root.append_child("MaxKeys").text().set(static_cast<unsigned int>(max_keys));
                root.append_child("IsTruncated").text().set(truncated);
                for (const auto &mapping : mappings) {
                    auto content = root.append_child("Contents");
                    content.append_child("Key").text().set(mapping.object_key.c_str());
                    content.append_child("LastModified").text().set(
                        iso8601(mapping.last_modified_ms).c_str()
                    );
                    content.append_child("ETag").text().set(("\"" + mapping.etag + "\"").c_str());
                    content.append_child("Size").text().set(mapping.size_bytes);
                    content.append_child("StorageClass").text().set("STANDARD");
                }
                if (truncated && !mappings.empty()) {
                    root.append_child("NextContinuationToken").text().set(
                        mappings.back().object_key.c_str()
                    );
                }
                std::ostringstream xml;
                document.save(xml, "", pugi::format_raw);
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::content_type, "application/xml");
                response.body() = xml.str();
                response.prepare_payload();
                return response;
            }
            if (key.empty() && request.method() == http::verb::post &&
                query.find("delete") != query.end()) {
                pugi::xml_document input;
                if (!input.load_string(request.body().c_str())) {
                    return s3_error(request, http::status::bad_request, "MalformedXML", "invalid delete request");
                }
                pugi::xml_document output;
                auto root = output.append_child("DeleteResult");
                root.append_attribute("xmlns") = "http://s3.amazonaws.com/doc/2006-03-01/";
                for (pugi::xml_node item : input.document_element().children("Object")) {
                    const std::string delete_key = item.child_value("Key");
                    objects_.erase(branch, bucket, delete_key);
                    auto deleted = root.append_child("Deleted");
                    deleted.append_child("Key").text().set(delete_key.c_str());
                }
                std::ostringstream xml;
                output.save(xml, "", pugi::format_raw);
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::content_type, "application/xml");
                response.body() = xml.str();
                response.prepare_payload();
                return response;
            }
            if (key.empty()) {
                return s3_error(request, http::status::not_implemented, "NotImplemented", "bucket operation is unsupported");
            }
            if (request.method() == http::verb::post && query.find("uploads") != query.end()) {
                const std::string upload_id = multipart_.create(
                    branch,
                    bucket,
                    key,
                    request_header(request, "content-type").empty()
                        ? "application/octet-stream" : request_header(request, "content-type")
                );
                pugi::xml_document document;
                auto root = document.append_child("InitiateMultipartUploadResult");
                root.append_attribute("xmlns") = "http://s3.amazonaws.com/doc/2006-03-01/";
                root.append_child("Bucket").text().set(bucket.c_str());
                root.append_child("Key").text().set(key.c_str());
                root.append_child("UploadId").text().set(upload_id.c_str());
                std::ostringstream xml;
                document.save(xml, "", pugi::format_raw);
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::content_type, "application/xml");
                response.body() = xml.str();
                response.prepare_payload();
                return response;
            }
            if (request.method() == http::verb::put &&
                query.find("uploadId") != query.end() &&
                query.find("partNumber") != query.end()) {
                const std::string etag = multipart_.upload_part(
                    branch,
                    query["uploadId"],
                    std::stoi(query["partNumber"]),
                    request.body()
                );
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::etag, "\"" + etag + "\"");
                response.prepare_payload();
                return response;
            }
            if (request.method() == http::verb::post &&
                query.find("uploadId") != query.end()) {
                pugi::xml_document document;
                if (!document.load_string(request.body().c_str())) {
                    return s3_error(request, http::status::bad_request, "MalformedXML", "invalid multipart completion");
                }
                std::vector<std::pair<int, std::string>> parts;
                for (pugi::xml_node part : document.document_element().children("Part")) {
                    parts.emplace_back(
                        std::stoi(part.child_value("PartNumber")),
                        part.child_value("ETag")
                    );
                }
                ObjectMapping mapping = multipart_.complete(branch, query["uploadId"], parts);
                pugi::xml_document output;
                auto root = output.append_child("CompleteMultipartUploadResult");
                root.append_attribute("xmlns") = "http://s3.amazonaws.com/doc/2006-03-01/";
                root.append_child("Location").text().set(
                    ("s3://" + bucket + "/" + key).c_str()
                );
                root.append_child("Bucket").text().set(bucket.c_str());
                root.append_child("Key").text().set(key.c_str());
                root.append_child("ETag").text().set(("\"" + mapping.etag + "\"").c_str());
                std::ostringstream xml;
                output.save(xml, "", pugi::format_raw);
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::content_type, "application/xml");
                response.body() = xml.str();
                response.prepare_payload();
                return response;
            }
            if (query.find("uploadId") != query.end() &&
                request.method() == http::verb::delete_) {
                multipart_.abort(branch, query["uploadId"]);
                http::response<http::string_body> response(http::status::no_content, request.version());
                response.keep_alive(false);
                return response;
            }
            if (query.find("uploadId") != query.end() &&
                request.method() == http::verb::get) {
                auto parts = multipart_.list_parts(branch, query["uploadId"]);
                pugi::xml_document document;
                auto root = document.append_child("ListPartsResult");
                root.append_attribute("xmlns") = "http://s3.amazonaws.com/doc/2006-03-01/";
                root.append_child("Bucket").text().set(bucket.c_str());
                root.append_child("Key").text().set(key.c_str());
                root.append_child("UploadId").text().set(query["uploadId"].c_str());
                for (const auto &part : parts) {
                    auto item = root.append_child("Part");
                    item.append_child("PartNumber").text().set(part.number);
                    item.append_child("ETag").text().set(("\"" + part.etag + "\"").c_str());
                    item.append_child("Size").text().set(part.size);
                }
                root.append_child("IsTruncated").text().set(false);
                std::ostringstream xml;
                document.save(xml, "", pugi::format_raw);
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::content_type, "application/xml");
                response.body() = xml.str();
                response.prepare_payload();
                return response;
            }
            if (request.method() == http::verb::put) {
                const std::string copy_source = request_header(request, "x-amz-copy-source");
                if (!copy_source.empty()) {
                    std::string source = url_decode(copy_source);
                    if (!source.empty() && source.front() == '/') source.erase(source.begin());
                    std::size_t source_slash = source.find('/');
                    if (source_slash == std::string::npos) {
                        return s3_error(request, http::status::bad_request, "InvalidArgument", "invalid copy source");
                    }
                    auto source_object = objects_.get(
                        branch,
                        source.substr(0, source_slash),
                        source.substr(source_slash + 1)
                    );
                    if (!source_object) {
                        return s3_error(request, http::status::not_found, "NoSuchKey", "copy source does not exist");
                    }
                    ObjectMapping mapping = objects_.put(
                        branch,
                        bucket,
                        key,
                        source_object->second.body,
                        source_object->first.content_type,
                        source_object->first.content_encoding,
                        source_object->first.cache_control,
                        source_object->first.user_metadata
                    );
                    pugi::xml_document document;
                    auto root = document.append_child("CopyObjectResult");
                    root.append_child("LastModified").text().set(
                        iso8601(mapping.last_modified_ms).c_str()
                    );
                    root.append_child("ETag").text().set(("\"" + mapping.etag + "\"").c_str());
                    std::ostringstream xml;
                    document.save(xml, "", pugi::format_raw);
                    http::response<http::string_body> response(http::status::ok, request.version());
                    response.keep_alive(false);
                    response.set(http::field::content_type, "application/xml");
                    response.body() = xml.str();
                    response.prepare_payload();
                    return response;
                }
                std::optional<std::string> if_match;
                const std::string if_match_header = request_header(request, "if-match");
                if (!if_match_header.empty()) if_match = if_match_header;
                const bool if_none_match = request_header(request, "if-none-match") == "*";
                Json metadata = Json::object();
                for (const auto &field : request) {
                    std::string name = lower(std::string(field.name_string()));
                    if (name.rfind("x-amz-meta-", 0) == 0) {
                        metadata[name.substr(std::strlen("x-amz-meta-"))] =
                            std::string(field.value());
                    }
                }
                ObjectMapping mapping = objects_.put(
                    branch,
                    bucket,
                    key,
                    request.body(),
                    request_header(request, "content-type").empty()
                        ? "application/octet-stream" : request_header(request, "content-type"),
                    request_header(request, "content-encoding"),
                    request_header(request, "cache-control"),
                    metadata.dump(),
                    if_match,
                    if_none_match
                );
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::etag, "\"" + mapping.etag + "\"");
                response.set(http::field::last_modified, http_date(mapping.last_modified_ms));
                response.prepare_payload();
                return response;
            }
            if (request.method() == http::verb::head) {
                auto mapping = objects_.head(branch, bucket, key);
                if (!mapping) {
                    return s3_error(request, http::status::not_found, "NoSuchKey", "object does not exist");
                }
                http::response<http::string_body> response(http::status::ok, request.version());
                response.keep_alive(false);
                response.set(http::field::etag, "\"" + mapping->etag + "\"");
                response.set(http::field::last_modified, http_date(mapping->last_modified_ms));
                response.set(http::field::content_type, mapping->content_type);
                response.content_length(mapping->size_bytes);
                return response;
            }
            if (request.method() == http::verb::get) {
                auto object = objects_.get(
                    branch,
                    bucket,
                    key,
                    request_header(request, "range")
                );
                if (!object) {
                    return s3_error(request, http::status::not_found, "NoSuchKey", "object does not exist");
                }
                http::status status = object->second.status == 206
                    ? http::status::partial_content : http::status::ok;
                http::response<http::string_body> response(status, request.version());
                response.keep_alive(false);
                response.set(http::field::etag, "\"" + object->first.etag + "\"");
                response.set(http::field::last_modified, http_date(object->first.last_modified_ms));
                response.set(http::field::content_type, object->first.content_type);
                auto range = object->second.headers.find("content-range");
                if (range != object->second.headers.end()) {
                    response.set(http::field::content_range, range->second);
                }
                response.body() = std::move(object->second.body);
                response.prepare_payload();
                return response;
            }
            if (request.method() == http::verb::delete_) {
                objects_.erase(branch, bucket, key);
                http::response<http::string_body> response(http::status::no_content, request.version());
                response.keep_alive(false);
                return response;
            }
            return s3_error(request, http::status::not_implemented, "NotImplemented", "operation is unsupported");
        } catch (const std::exception &error) {
            const std::string message = error.what();
            if (message.rfind("precondition failed", 0) == 0) {
                return s3_error(request, http::status::precondition_failed, "PreconditionFailed", message);
            }
            return s3_error(request, http::status::internal_server_error, "InternalError", message);
        }
    }

    void handle(tcp::socket socket) {
        beast::tcp_stream stream(std::move(socket));
        const int descriptor = stream.socket().native_handle();
        {
            std::lock_guard<std::mutex> guard(active_sockets_mutex_);
            active_sockets_.insert(descriptor);
        }
        auto active_guard = std::shared_ptr<void>(
            nullptr,
            [this, descriptor](void *) {
                std::lock_guard<std::mutex> guard(active_sockets_mutex_);
                active_sockets_.erase(descriptor);
            }
        );
        beast::flat_buffer buffer;
        boost::system::error_code error;
        while (running_.load()) {
            stream.expires_after(std::chrono::seconds(2));
            http::request_parser<http::string_body> parser;
            parser.body_limit(1024ULL * 1024ULL * 1024ULL);
            http::read(stream, buffer, parser, error);
            if (error == http::error::end_of_stream) break;
            if (error == beast::error::timeout) break;
            if (error) return;
            auto request = parser.release();

            http::response<http::string_body> response;
            if (request.method() == http::verb::get && request.target() == "/healthz") {
                response = json_response(request, http::status::ok, {{"status", "ok"}});
            } else if (std::string(request.target()).rfind("/iceberg/", 0) == 0) {
                response = handle_iceberg(request);
            } else {
                response = handle_s3(request);
            }
            const bool keep_alive = request.keep_alive();
            response.keep_alive(keep_alive);
            stream.expires_after(std::chrono::seconds(120));
            http::write(stream, response, error);
            if (error || !keep_alive) break;
        }
        stream.socket().shutdown(tcp::socket::shutdown_send, error);
    }

    std::string host_;
    std::uint16_t port_;
    std::atomic<bool> running_{false};
    std::atomic<bool> ready_{false};
    std::unique_ptr<asio::io_context> io_context_;
    std::unique_ptr<tcp::acceptor> acceptor_;
    std::thread thread_;
    std::string last_error_;
    std::shared_ptr<CurlS3Backend> upstream_;
    BranchableObjectStore objects_;
    MultipartManager multipart_;
    IcebergRestCatalog catalog_;
    mutable std::mutex credentials_mutex_;
    std::unordered_map<std::string, Credential> credentials_;
    mutable std::mutex catalog_tokens_mutex_;
    std::unordered_map<std::string, std::string> catalog_tokens_;
    std::size_t worker_count_;
    std::mutex queue_mutex_;
    std::condition_variable queue_condition_;
    std::deque<tcp::socket> pending_sockets_;
    std::vector<std::thread> workers_;
    std::int64_t gc_interval_seconds_;
    std::int64_t gc_grace_ms_;
    std::mutex gc_condition_mutex_;
    std::condition_variable gc_condition_;
    std::thread gc_thread_;
    std::mutex active_sockets_mutex_;
    std::unordered_set<int> active_sockets_;
};

} // namespace

void bind_chronos_s3(pybind11::module_ &m) {
    pybind11::class_<ObjectMapping>(m, "ChronosS3Object")
        .def_readonly("bucket", &ObjectMapping::bucket)
        .def_readonly("key", &ObjectMapping::object_key)
        .def_readonly("physical_bucket", &ObjectMapping::physical_bucket)
        .def_readonly("physical_key", &ObjectMapping::physical_key)
        .def_readonly("etag", &ObjectMapping::etag)
        .def_readonly("size", &ObjectMapping::size_bytes)
        .def_readonly("last_modified_ms", &ObjectMapping::last_modified_ms)
        .def_readonly("ownership", &ObjectMapping::ownership)
        .def_readonly("generation", &ObjectMapping::generation);

    pybind11::class_<ChronosLakeServer>(m, "NativeChronosLakeServer")
        .def(pybind11::init<
                 std::string,
                 std::string,
                 std::string,
                 std::string,
                 std::string,
                 std::uint16_t,
                 std::string,
                 std::string,
                 std::string,
                 std::string,
                 std::size_t,
                 std::int64_t,
                 std::int64_t>(),
             pybind11::arg("database_path"),
             pybind11::arg("upstream_endpoint") = "http://127.0.0.1:9000",
             pybind11::arg("upstream_access_key") = "minioadmin",
             pybind11::arg("upstream_secret_key") = "minioadmin",
             pybind11::arg("host") = "127.0.0.1",
             pybind11::arg("port") = 0,
             pybind11::arg("region") = "us-east-1",
             pybind11::arg("client_access_key") = "",
             pybind11::arg("client_secret_key") = "",
             pybind11::arg("warehouse_location") = "s3://warehouse/iceberg",
             pybind11::arg("worker_threads") = 32,
             pybind11::arg("gc_interval_seconds") = 30,
             pybind11::arg("gc_grace_seconds") = 300)
        .def("start", &ChronosLakeServer::start, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("stop", &ChronosLakeServer::stop, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("create_bucket", &ChronosLakeServer::create_bucket, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def(
            "put_object",
            &ChronosLakeServer::put_object,
            pybind11::arg("branch"),
            pybind11::arg("bucket"),
            pybind11::arg("key"),
            pybind11::arg("body"),
            pybind11::arg("content_type") = "application/octet-stream",
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def(
            "get_object",
            &ChronosLakeServer::get_object,
            pybind11::arg("branch"),
            pybind11::arg("bucket"),
            pybind11::arg("key"),
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def(
            "delete_object",
            &ChronosLakeServer::delete_object,
            pybind11::arg("branch"),
            pybind11::arg("bucket"),
            pybind11::arg("key"),
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def(
            "list_object_keys",
            &ChronosLakeServer::list_object_keys,
            pybind11::arg("branch"),
            pybind11::arg("bucket"),
            pybind11::arg("prefix") = "",
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def(
            "create_branch",
            &ChronosLakeServer::create_branch,
            pybind11::arg("branch"),
            pybind11::arg("from_branch") = "main",
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def("delete_branch", &ChronosLakeServer::delete_branch, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("branches", &ChronosLakeServer::branches, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def(
            "bootstrap",
            &ChronosLakeServer::bootstrap,
            pybind11::arg("branch"),
            pybind11::arg("bucket"),
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def(
            "collect_garbage",
            &ChronosLakeServer::collect_garbage,
            pybind11::arg("grace_seconds") = 300,
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def(
            "bind_credentials",
            &ChronosLakeServer::bind_credentials,
            pybind11::arg("access_key"),
            pybind11::arg("secret_key"),
            pybind11::arg("branch"),
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def(
            "bind_catalog_token",
            &ChronosLakeServer::bind_catalog_token,
            pybind11::arg("token"),
            pybind11::arg("branch"),
            pybind11::call_guard<pybind11::gil_scoped_release>()
        )
        .def_property_readonly("running", &ChronosLakeServer::running)
        .def_property_readonly("port", &ChronosLakeServer::port);
}

} // namespace chronos::native
