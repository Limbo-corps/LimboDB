#include "../include/table_manager.h"
#include "../include/catalog_manager.h"
#include "../include/record_manager.h"
#include "../include/record_iterator.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <algorithm>
#include <numeric>
#include "pretty.hpp"

using namespace std;

#define DEBUG_COLOR_RESET      "\033[0m"
#define DEBUG_COLOR_YELLOW     "\033[33m"
#define DEBUG_COLOR_CYAN       "\033[36m"
#define DEBUG_DEBUG_LABEL      DEBUG_COLOR_YELLOW "[DEBUG]" DEBUG_COLOR_RESET
#define DEBUG_TABLE_LABEL      DEBUG_COLOR_CYAN "[TABLE_MANAGER]" DEBUG_COLOR_RESET
#define DEBUG_TABLE_MANAGER    std::cout << DEBUG_DEBUG_LABEL << DEBUG_TABLE_LABEL << " "

TableManager::TableManager(CatalogManager& cat, RecordManager& rm, IndexManager& im)
    : catalog(cat), record_mgr(rm), index_mgr(im) {
    DEBUG_TABLE_MANAGER << "Initialized TableManager with IndexManager" << std::endl;
}

int TableManager::insert_into(const string& table_name, const vector<string>& values) {
    DEBUG_TABLE_MANAGER << "insert_into called for table: " << table_name << std::endl;
    TableSchema schema = catalog.get_schema(table_name);
    if (values.size() != schema.columns.size()) {
        DEBUG_TABLE_MANAGER << "Insert failed: value count does not match schema" << std::endl;
        return -1;
    }

    // Primary Key Uniqueness check
    if(schema.primary_key_idx != -1){
        const string& pk_column = schema.columns[schema.primary_key_idx];
        const string& pk_value = values[schema.primary_key_idx];

        vector<int> existing = index_mgr.search(table_name, pk_column, pk_value);
        if(!existing.empty()){
            DEBUG_TABLE_MANAGER << "[ERROR] Duplicate entry for PRIMARY KEY: " << pk_value << endl;
            return -1;
        }
    }

    stringstream ss;
    ss << table_name << "|";
    for (size_t i = 0; i < values.size(); ++i) {
        ss << values[i];
        if (i < values.size() - 1) ss << "|";
    }

    Record record(ss.str());
    int record_id = record_mgr.insert_record(record);

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        index_mgr.insert_entry(table_name, schema.columns[i], values[i], record_id);
    }

    DEBUG_TABLE_MANAGER << "Inserted record_id: " << record_id << std::endl;
    return record_id;
}

bool TableManager::delete_from(const std::string& table_name, int record_id) {
    DEBUG_TABLE_MANAGER << "delete_from called for table: " << table_name 
                         << ", record_id: " << record_id << std::endl;

    if (record_id == -1) {
        int deleted_count = 0;
        std::vector<int> to_delete;

        RecordIterator iterator(record_mgr.get_disk());
        const std::string table_prefix = table_name + "|";
        const std::string schema_prefix = "SCHEMA|";

        while (iterator.has_next()) {
            auto [rec, page_id, slot_id] = iterator.next_with_location();
            std::string rec_str = rec.to_string();

            if (rec_str.rfind(schema_prefix, 0) == 0) continue;
            if (rec_str.rfind(table_prefix, 0) != 0) continue;

            RecordID rid(page_id, slot_id);
            to_delete.push_back(rid.encode());
        }

        for (int rid_encoded : to_delete) {
            delete_from(table_name, rid_encoded);
            deleted_count++;
        }

        DEBUG_TABLE_MANAGER << "Deleted " << deleted_count 
                            << " records from table: " << table_name << std::endl;
        return true;
    }

    record_mgr.delete_record(record_id);
    return true;
}

bool TableManager::update(const string& table_name, int record_id, const vector<string>& new_values) {
    DEBUG_TABLE_MANAGER << "update called for table: " << table_name << std::endl;
    TableSchema schema = catalog.get_schema(table_name);
    if (new_values.size() != schema.columns.size()) {
        DEBUG_TABLE_MANAGER << "Update failed: value count mismatch" << std::endl;
        return false;
    }

    Record old_record = record_mgr.get_record(record_id);
    stringstream ss_old(old_record.to_string());
    vector<string> old_tokens;
    string token;
    
    // Extract table name and values
    std::getline(ss_old, token, '|'); // Skip table name
    while (std::getline(ss_old, token, '|')) {
        old_tokens.push_back(token);
    }

    for (size_t i = 0; i < old_tokens.size(); ++i) {
        index_mgr.delete_entry(table_name, schema.columns[i], old_tokens[i], record_id);
    }

    stringstream ss;
    ss << table_name << "|";
    for (size_t i = 0; i < new_values.size(); ++i) {
        ss << new_values[i];
        if (i < new_values.size() - 1) ss << "|";
    }

    Record new_record(ss.str());
    record_mgr.update_record(record_id, new_record);

    for (size_t i = 0; i < new_values.size(); ++i) {
        index_mgr.insert_entry(table_name, schema.columns[i], new_values[i], record_id);
    }

    return true;
}

Record TableManager::select(const string& table_name, int record_id) {
    DEBUG_TABLE_MANAGER << "select called for table: " << table_name 
                         << ", record_id: " << record_id << std::endl;
    return record_mgr.get_record(record_id);
}

vector<Record> TableManager::scan(const string& table_name) {
    DEBUG_TABLE_MANAGER << "scan called for table: " << table_name << std::endl;
    vector<Record> records;
    RecordIterator it(record_mgr.get_disk());
    const std::string schema_prefix = "SCHEMA|";
    const std::string table_prefix = table_name + "|";
    
    while (it.has_next()) {
        Record rec = it.next();
        std::string rec_str(rec.data.begin(), rec.data.end());
        
        if (rec_str.rfind(schema_prefix, 0) == 0) continue;
        if (rec_str.rfind(table_prefix, 0) != 0) continue;
        
        rec_str = rec_str.substr(table_prefix.size());
        rec.data = std::vector<char>(rec_str.begin(), rec_str.end());
        records.push_back(rec);
    }
    
    DEBUG_TABLE_MANAGER << "Scanned " << records.size() 
                        << " records from table: " << table_name << std::endl;
    return records;
}

void TableManager::printTable(const std::string& tableName) {
    TableSchema schema = catalog.get_schema(tableName);
    if (schema.table_name.empty()) {
        std::cerr << "[ERROR] Table '" << tableName << "' does not exist." << std::endl;
        return;
    }

    std::vector<Record> records = scan(tableName);
    pretty::Table table;

    // Add header row
    table.add_row(schema.columns);

    // Add data rows
    for (const auto& rec : records) {
        std::string recStr(rec.data.begin(), rec.data.end());
        std::vector<std::string> values;
        std::stringstream ss(recStr);
        std::string field;
        
        while (std::getline(ss, field, '|')) {
            values.push_back(field);
        }
        
        // Pad with empty strings if needed
        if (values.size() < schema.columns.size()) {
            values.resize(schema.columns.size(), "");
        }
        table.add_row(values);
    }

    // Handle empty table
    if (records.empty()) {
        std::vector<std::string> emptyRow(schema.columns.size(), "");
        emptyRow[0] = "No records found";
        table.add_row(emptyRow);
    }

    // Print table with enhanced formatting
    pretty::Printer printer;
    printer.frame(pretty::FrameStyle::Basic);
    std::cout << printer(table) << std::endl;
}

std::vector<string> TableManager::unpack_record(const Record& rec, const TableSchema& schema) {
    DEBUG_TABLE_MANAGER << "unpack_record called for table: " << schema.table_name << std::endl;
    string record_str = string(rec.data.begin(), rec.data.end());
    DEBUG_TABLE_MANAGER << "Raw record string: " << record_str << std::endl;

    stringstream ss(record_str);
    string token;

    // Skip table name
    getline(ss, token, '|');

    vector<string> fields;
    while (getline(ss, token, '|')) {
        fields.push_back(token);
    }

    if (fields.size() != schema.columns.size()) {
        DEBUG_TABLE_MANAGER << "[WARN] unpacked field count mismatch: expected "
                            << schema.columns.size() << ", got " << fields.size() << std::endl;
    } else {
        DEBUG_TABLE_MANAGER << "Unpacked fields: ";
        for (const auto& f : fields) {
            std::cout << "[" << f << "] ";
        }
        std::cout << std::endl;
    }

    return fields;
}

bool TableManager::create_table(const string& table_name, const vector<string>& columns, const vector<DataType>& types, int primary_key_idx){
    if(!catalog.create_table(table_name, columns, types, primary_key_idx)){
        return false;
    }

    const string& pk_column = columns[primary_key_idx];
    index_mgr.create_index(table_name, pk_column);

    return true;
}