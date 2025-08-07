#ifndef UNICODE_HPP
#define UNICODE_HPP

#include <string>
#include <cctype>
#include <cstddef>

namespace unicode {
    inline std::size_t columnWidth(const std::string& str) {
        // Simple implementation: 1 char = 1 column
        return str.size();
    }
}

#endif // UNICODE_HPP
