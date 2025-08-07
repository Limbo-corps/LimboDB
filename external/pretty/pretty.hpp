#ifndef PRETTY_HPP
#define PRETTY_HPP

#include <vector>
#include <string>
#include "unicode.hpp"

namespace pretty {

enum class FrameStyle {
    Basic,
    Bold,
    Double
};

class Table {
public:
    void add_row(const std::vector<std::string>& row);
    size_t num_columns() const;
    const std::vector<std::vector<std::string>>& get_rows() const;
    
private:
    std::vector<std::vector<std::string>> rows;
};

class Printer {
public:
    Printer& frame(FrameStyle style);
    std::string operator()(const Table& table) const;
    
private:
    FrameStyle current_style = FrameStyle::Basic;
};

} // namespace pretty

#endif // PRETTY_HPP
