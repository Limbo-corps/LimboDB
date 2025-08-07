#include "pretty.hpp"
#include <sstream>
#include <iomanip>
#include <vector>
#include <string>
#include <algorithm>

namespace pretty {

void Table::add_row(const std::vector<std::string>& row) {
    rows.push_back(row);
}

size_t Table::num_columns() const {
    size_t max_cols = 0;
    for (const auto& row : rows) {
        max_cols = std::max(max_cols, row.size());
    }
    return max_cols;
}

const std::vector<std::vector<std::string>>& Table::get_rows() const {
    return rows;
}

Printer& Printer::frame(FrameStyle style) {
    current_style = style;
    return *this;
}

std::string Printer::operator()(const Table& table) const {
    const auto& rows = table.get_rows();
    const size_t num_columns = table.num_columns();
    
    if (rows.empty()) return "";
    
    // Compute column widths with minimum width and extra padding
    std::vector<size_t> col_widths(num_columns, 10); // Minimum width of 10
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size() && i < num_columns; ++i) {
            col_widths[i] = std::max(col_widths[i], unicode::columnWidth(row[i]) + 4);
        }
    }
    
    std::ostringstream out;
    
    // Print top border
    out << '+';
    for (size_t i = 0; i < num_columns; ++i) {
        out << std::string(col_widths[i], '-') << '+';
    }
    out << '\n';
    
    // Print header row (first row) with special formatting
    if (!rows.empty()) {
        out << '|';
        for (size_t i = 0; i < num_columns; ++i) {
            std::string content = (i < rows[0].size()) ? rows[0][i] : "";
            size_t total_padding = col_widths[i] - unicode::columnWidth(content);
            size_t left_pad = total_padding / 2;
            size_t right_pad = total_padding - left_pad;
            
            out << std::string(left_pad, ' ') << content << std::string(right_pad, ' ') << '|';
        }
        out << '\n';
        
        // Print header separator with double lines
        out << '+';
        for (size_t i = 0; i < num_columns; ++i) {
            out << std::string(col_widths[i], '=') << '+';
        }
        out << '\n';
        
        // Print data rows
        for (size_t row_idx = 1; row_idx < rows.size(); ++row_idx) {
            const auto& row = rows[row_idx];
            out << '|';
            for (size_t i = 0; i < num_columns; ++i) {
                std::string content = (i < row.size()) ? row[i] : "";
                size_t content_width = unicode::columnWidth(content);
                size_t padding = col_widths[i] - content_width;
                size_t left_pad = 2; // Fixed left padding
                size_t right_pad = padding - left_pad;
                
                out << std::string(left_pad, ' ') << content << std::string(right_pad, ' ') << '|';
            }
            out << '\n';
        }
    }
    
    // Print bottom border
    out << '+';
    for (size_t i = 0; i < num_columns; ++i) {
        out << std::string(col_widths[i], '-') << '+';
    }
    out << '\n';
    
    return out.str();
}

} // namespace pretty
