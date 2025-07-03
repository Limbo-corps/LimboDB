#include "../include/catalog_manager.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype>
#include "../include/record_iterator.h"
#include "../include/table_manager.h"

// ANSI color codes for debug output
#define COLOR_RESET   "\033[0m"
#define COLOR_YELLOW  "\033[33m"
#define COLOR_CYAN    "\033[36m"

#define DEBUG_CATALOG(msg) \
    std::cout << COLOR_YELLOW << "[DEBUG]" << COLOR_CYAN << "[CATALOG_MANAGER] " << COLOR_RESET << msg << std::endl;

// ---------- Normalization Utilities ----------

namespace {
    // Trim whitespace from both ends
    std::string trim(const std::string& s) {
        size_t start = s.find_first_not_of(" \t\r\n");
        size_t end = s.find_last_not_of(" \t\r\n");
        if (start == std::string::npos) return "";
        return s.substr(start, end - start + 1);
    }

    // Convert string to lower case
    std::string to_lower(const std::string& s) {
        std::string result = s;
        std::transform(result.begin(), result.end(), result.begin(),
            [](unsigned char c) { return std::tolower(c); });
        return result;
    }

    // Normalize identifier: trim and lowercase
    std::string normalize_identifier(const std::string& s) {
        return to_lower(trim(s));
    }

    // Normalize vector of identifiers
    std::vector<std::string> normalize_identifiers(const std::vector<std::string>& v) {
        std::vector<std::string> result;
        for (const auto& s : v) {
            result.push_back(normalize_identifier(s));
        }
        return result;
    }
}

// ---------- TableSchema Methods ----------

std::string TableSchema::serialize() const {
    std::ostringstream oss;
    oss << "SCHEMA|" << table_name << "|";
    for (size_t i = 0; i < columns.size(); ++i) {
        oss << columns[i];
        if (i + 1 < columns.size()) oss << ",";
    }
    oss<< "|";
    for(size_t i = 0; i < column_types.size(); ++i){
        oss<<to_string(column_types[i]);
        if(i + 1 < column_types.size()) oss << ",";
    }
    oss<<"|";
    oss << primary_key_idx;
    DEBUG_CATALOG("Serialized schema for table '" << table_name << "': " << oss.str());
    return oss.str();
}

TableSchema TableSchema::deserialize(const std::string& record_str) {
    const std::string prefix = "SCHEMA|";
    if (record_str.rfind(prefix, 0) != 0) {
        DEBUG_CATALOG("Skipped non-schema record: '" << record_str << "'");
        return TableSchema{};
    }

    std::string content = record_str.substr(prefix.size());
    std::vector<string> parts;
    size_t start = 0, end;

    while((end = content.find('|', start)) != std::string::npos){
        parts.push_back(content.substr(start, end - start));
        start = end + 1;
    }
    parts.push_back(content.substr(start));

    if(parts.size() != 4) {
        DEBUG_CATALOG("Failed to decerialize: expected 4 parts but got " << parts.size());
        return TableSchema{};
    }

    TableSchema schema;
    schema.table_name = normalize_identifier(parts[0]);

    std::stringstream col_ss(parts[1]);
    std::string col;

    while(std::getline(col_ss, col, ',')){
        schema.columns.push_back(normalize_identifier(col));
    }

    //Split types
    std::stringstream type_ss(parts[2]);
    std::string type_str;
    while(std::getline(type_ss, type_str, ',')){
        schema.column_types.push_back(parse_type(type_str));
    }

    //Primary key index
    try{
        schema.primary_key_idx = std::stoi(parts[3]);
    } catch(...) {
        DEBUG_CATALOG("Failed to parse primary_key_idx from '" << parts[3] << "'");
        schema.primary_key_idx = -1;
    }

    DEBUG_CATALOG("Deserialized schema for table '" << schema.table_name << "' with columns: " << parts[1]);
    return schema;
}

// ---------- CatalogManager Methods ----------

CatalogManager::CatalogManager(RecordManager& rm, IndexManager& im)
    : record_manager(rm), index_manager(im) {
    DEBUG_CATALOG("Initializing CatalogManager");
    load_catalog();
}

void CatalogManager::load_catalog() {
    DEBUG_CATALOG("Loading catalog from disk");
    RecordIterator iter(record_manager.get_disk());
    int count = 0;

    while (iter.has_next()) {
        try {
            auto [rec, page_id, slot_id] = iter.next_with_location();
            TableSchema schema = TableSchema::deserialize(rec.to_string());
            if (!schema.table_name.empty()) {
                schema_cache[schema.table_name] = schema;
                ++count;
            }
        } catch (const std::exception& e) {
            DEBUG_CATALOG("Error loading schema: " << e.what());
        }
    }

    DEBUG_CATALOG("Loaded " << count << " table schemas into cache");
}

bool CatalogManager::create_table(const std::string& table_name, const std::vector<std::string>& columns, const std::vector<DataType>& types, int primary_key_idx) {
    std::string norm_table = normalize_identifier(table_name);
    std::vector<std::string> norm_columns = normalize_identifiers(columns);

    DEBUG_CATALOG("Attempting to create table '" << norm_table << "'");
    if (schema_cache.count(norm_table)) {
        DEBUG_CATALOG("Table '" << norm_table << "' already exists");
        return false;
    }

    //Ensure the number of types matches the lenght of the column
    if(columns.size() != types.size()){
        DEBUG_CATALOG("Mismatch between number of columns and types");
        return false;
    }

    if(primary_key_idx < 0 || primary_key_idx >= (int)columns.size()){
        DEBUG_CATALOG("Invalid Primary key Index");
        return false;
    }

    TableSchema schema;
    schema.table_name = norm_table;
    schema.columns = norm_columns;
    schema.column_types = types;
    schema.primary_key_idx = primary_key_idx;
    
    Record record(schema.serialize());
    record_manager.insert_record(record);
    schema_cache[norm_table] = schema;

    DEBUG_CATALOG("Table '" << norm_table << "' created with columns: " << schema.serialize());
    return true;
}

bool CatalogManager::drop_table(const std::string& table_name) {
    std::string norm_table = normalize_identifier(table_name);
    DEBUG_CATALOG("Attempting to drop table '" << norm_table << "'");

    if (!schema_cache.count(norm_table)) {
        DEBUG_CATALOG("Table '" << norm_table << "' does not exist");
        return false;
    }

    TableSchema schema = schema_cache[norm_table];
    std::string serialized_schema = schema.serialize();

    RecordIterator iterator(record_manager.get_disk());
    bool found = false;

    while (iterator.has_next()) {
        auto [rec, page_id, slot_id] = iterator.next_with_location();
        if (rec.to_string() == serialized_schema) {
            RecordID rid(page_id, slot_id);
            int record_id = rid.encode();
            record_manager.delete_record(record_id);
            DEBUG_CATALOG("Deleted schema for '" << norm_table << "' at page " << page_id << ", slot " << slot_id);
            found = true;
            break;
        }
    }

    if (!found) {
        DEBUG_CATALOG("Failed to find serialized schema for '" << norm_table << "' to delete");
        return false;
    }

    TableManager tm(*this, record_manager, index_manager);
    int deleted = tm.delete_from(norm_table, -1);
    DEBUG_CATALOG("Deleted " << deleted << " data records from table '" << norm_table << "'");

    schema_cache.erase(norm_table);
    DEBUG_CATALOG("Table '" << norm_table << "' dropped from cache (record data remains)");
    return true;
}

TableSchema CatalogManager::get_schema(const std::string& table_name) {
    std::string norm_table = normalize_identifier(table_name);
    DEBUG_CATALOG("Fetching schema for table '" << norm_table << "'");
    if (!schema_cache.count(norm_table)) {
        DEBUG_CATALOG("Table '" << norm_table << "' not found in catalog");
        return TableSchema{};
    }
    return schema_cache[norm_table];
}

std::vector<std::string> CatalogManager::list_tables() {
    std::vector<std::string> names;
    for (const auto& [name, _] : schema_cache) {
        names.push_back(name);
    }
    DEBUG_CATALOG("Listing tables: " << names.size() << " found");
    return names;
}

bool CatalogManager::column_exists(const std::string& table_name, const std::string& column_name) {
    std::string norm_table = normalize_identifier(table_name);
    std::string norm_col = normalize_identifier(column_name);
    if (!schema_cache.count(norm_table)) return false;
    const auto& columns = schema_cache[norm_table].columns;
    return std::find(columns.begin(), columns.end(), norm_col) != columns.end();
}
