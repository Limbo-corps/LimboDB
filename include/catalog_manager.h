#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include "./record_manager.h"
#include "./index_manager.h"

struct TableSchema {
    std::string table_name;
    std::vector<std::string> columns;

    std::string serialize() const;
    static TableSchema deserialize(const std::string& record_str);
};

class CatalogManager {
private:
    RecordManager& record_manager;
    IndexManager& index_manager;
    std::unordered_map<std::string, TableSchema> schema_cache;

    void load_catalog();

public:
    CatalogManager(RecordManager& rm, IndexManager& im);

    bool create_table(const std::string& table_name, const std::vector<std::string>& columns);
    bool drop_table(const std::string& table_name);

    TableSchema get_schema(const std::string& table_name);
    std::vector<std::string> list_tables();

    // New helper
    bool column_exists(const std::string& table_name, const std::string& column_name);
};
