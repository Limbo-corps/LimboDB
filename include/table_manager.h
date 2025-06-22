#pragma once

#include "./catalog_manager.h"
#include "./record_manager.h"
#include "./index_manager.h"
#include <string>
#include <vector>

using namespace std;

class TableManager {
private:
    CatalogManager& catalog;
    RecordManager& record_mgr;
    IndexManager& index_mgr;

public:
    TableManager(CatalogManager& cat, RecordManager& rm, IndexManager& im);

    int insert_into(const string& table_name, const vector<string>& values);
    bool delete_from(const string& table_name, int record_id);
    bool update(const string& table_name, int record_id, const vector<string>& new_values);
    Record select(const string& table_name, int record_id);
    vector<Record> scan(const string& table_name); // optional: full scan
    void printTable(const std::string& tableName);
};
