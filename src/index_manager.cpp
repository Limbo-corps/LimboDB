#include "../include/index_manager.h"
#include <iostream>
#include <algorithm>

using namespace std;

#define DEBUG_INDEX_MANAGER(msg) cout << "[DEBUG][INDEX_MANAGER] " << msg << endl;

// Destructor to clean up B+ trees
IndexManager::~IndexManager() {
    for (auto& table : indexes) {
        for (auto& column : table.second) {
            delete column.second;
        }
    }
}

// Create index
bool IndexManager::create_index(const string& table_name, const string& column_name) {
    DEBUG_INDEX_MANAGER("Creating index on table '" << table_name << "', column '" << column_name << "'");
    
    // Check if index already exists
    if (indexes[table_name].find(column_name) != indexes[table_name].end()) {
        DEBUG_INDEX_MANAGER("Index already exists");
        return false;
    }
    
    indexes[table_name][column_name] = new BPlusTree<string, set<int>>();
    DEBUG_INDEX_MANAGER("Index created successfully");
    return true;
}

// Drop index
bool IndexManager::drop_index(const string& table_name, const string& column_name) {
    DEBUG_INDEX_MANAGER("Dropping index on table '" << table_name << "', column '" << column_name << "'");
    
    auto table_it = indexes.find(table_name);
    if (table_it != indexes.end()) {
        auto col_it = table_it->second.find(column_name);
        if (col_it != table_it->second.end()) {
            delete col_it->second; // Clean up B+ tree
            table_it->second.erase(col_it);
            if (table_it->second.empty()) {
                indexes.erase(table_it);
            }
            DEBUG_INDEX_MANAGER("Index dropped successfully");
            return true;
        }
    }
    DEBUG_INDEX_MANAGER("Index not found to drop");
    return false;
}

// Insert entry
bool IndexManager::insert_entry(const string& table_name, const string& column_name, const string& key, int record_id) {
    DEBUG_INDEX_MANAGER("Inserting entry: table='" << table_name << "', column='" << column_name << "', key='" << key << "', record_id=" << record_id);
    
    auto table_it = indexes.find(table_name);
    if (table_it == indexes.end()) {
        DEBUG_INDEX_MANAGER("Table not found in indexes");
        return false;
    }
    
    auto col_it = table_it->second.find(column_name);
    if (col_it == table_it->second.end()) {
        DEBUG_INDEX_MANAGER("Column index not found");
        return false;
    }
    
    BPlusTree<string, set<int>>* btree = col_it->second;
    
    // Search for existing entry
    vector<set<int>> existing = btree->search(key);
    set<int> record_set;
    
    if (!existing.empty()) {
        record_set = existing[0]; // Get the existing set
    }
    
    record_set.insert(record_id);
    btree->insert(key, record_set); // Insert/update the set
    
    DEBUG_INDEX_MANAGER("Entry inserted successfully");
    return true;
}

// Delete entry
bool IndexManager::delete_entry(const string& table_name, const string& column_name, const string& key, int record_id) {
    DEBUG_INDEX_MANAGER("Deleting entry: table='" << table_name << "', column='" << column_name << "', key='" << key << "', record_id=" << record_id);
    
    auto table_it = indexes.find(table_name);
    if (table_it == indexes.end()) {
        DEBUG_INDEX_MANAGER("Table not found in indexes");
        return false;
    }
    
    auto col_it = table_it->second.find(column_name);
    if (col_it == table_it->second.end()) {
        DEBUG_INDEX_MANAGER("Column index not found");
        return false;
    }
    
    BPlusTree<string, set<int>>* btree = col_it->second;
    
    // Search for existing entry
    vector<set<int>> existing = btree->search(key);
    if (existing.empty()) {
        DEBUG_INDEX_MANAGER("Key not found in index");
        return false;
    }
    
    set<int> record_set = existing[0];
    record_set.erase(record_id);
    
    if (record_set.empty()) {
        btree->remove(key, existing[0]); // Remove the entire entry
        DEBUG_INDEX_MANAGER("Key '" << key << "' erased from index as it became empty");
    } else {
        btree->insert(key, record_set); // Update with new set
    }
    
    DEBUG_INDEX_MANAGER("Entry deleted successfully");
    return true;
}

// Search by key
vector<int> IndexManager::search(const string& table_name, const string& column_name, const string& key) {
    DEBUG_INDEX_MANAGER("Searching for key '" << key << "' in table '" << table_name << "', column '" << column_name << "'");
    vector<int> result;
    
    auto table_it = indexes.find(table_name);
    if (table_it == indexes.end()) {
        DEBUG_INDEX_MANAGER("Table not found in indexes");
        return result;
    }
    
    auto col_it = table_it->second.find(column_name);
    if (col_it == table_it->second.end()) {
        DEBUG_INDEX_MANAGER("Column index not found");
        return result;
    }
    
    BPlusTree<string, set<int>>* btree = col_it->second;
    vector<set<int>> search_result = btree->search(key);
    
    if (!search_result.empty()) {
        const set<int>& record_set = search_result[0];
        result.assign(record_set.begin(), record_set.end());
    }
    
    DEBUG_INDEX_MANAGER("Search found " << result.size() << " record(s)");
    return result;
}

// Range search
vector<int> IndexManager::range_search(const string& table_name, const string& column_name, const string& start_key, const string& end_key) {
    DEBUG_INDEX_MANAGER("Range search: table='" << table_name << "', column='" << column_name << "', start_key='" << start_key << "', end_key='" << end_key << "'");
    vector<int> result;
    
    auto table_it = indexes.find(table_name);
    if (table_it == indexes.end()) {
        DEBUG_INDEX_MANAGER("Table not found in indexes");
        return result;
    }
    
    auto col_it = table_it->second.find(column_name);
    if (col_it == table_it->second.end()) {
        DEBUG_INDEX_MANAGER("Column index not found");
        return result;
    }
    
    BPlusTree<string, set<int>>* btree = col_it->second;
    vector<set<int>> range_result = btree->range_search(start_key, end_key);
    
    set<int> unique_records; // Use set to avoid duplicates
    for (const auto& record_set : range_result) {
        unique_records.insert(record_set.begin(), record_set.end());
    }
    
    result.assign(unique_records.begin(), unique_records.end());
    
    DEBUG_INDEX_MANAGER("Range search found " << result.size() << " record(s)");
    return result;
}