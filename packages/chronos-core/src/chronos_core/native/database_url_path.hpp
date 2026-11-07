#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace chronos::native {

// Database URLs use percent-encoded paths. Decode once, without treating '+'
// as a space; raw filesystem paths must not pass through this helper.
inline std::string decode_database_url_path(std::string_view path) {
    const auto hex_digit = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
        return -1;
    };
    std::string decoded;
    decoded.reserve(path.size());
    for (std::size_t i = 0; i < path.size(); ++i) {
        char ch = path[i];
        if (ch == '%' && i + 2 < path.size()) {
            const int high = hex_digit(path[i + 1]);
            const int low = hex_digit(path[i + 2]);
            if (high >= 0 && low >= 0) {
                ch = static_cast<char>((high << 4) | low);
                i += 2;
            }
        }
        if (ch == '\0') {
            throw std::invalid_argument("database path must not contain NUL bytes");
        }
        decoded.push_back(ch);
    }
    return decoded;
}

}  // namespace chronos::native
