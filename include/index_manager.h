#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <set>
#include "btree.h"
#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

using namespace std;

class IndexManager {
private:
    // table -> column -> BPlusTree<string, set<int>>
    unordered_map<string, unordered_map<string, BPlusTree<string, set<int>>*>> indexes;

public:
    bool column_exists(const string& table_name, const string& column_name);

    IndexManager();
    ~IndexManager();
    
    void save_indexes();
    void load_indexes();
    
    bool create_index(const string& table_name, const string& column_name);
    bool drop_index(const string& table_name, const string& column_name);

    bool insert_entry(const string& table_name, const string& column_name, const string& key, int record_id);
    bool delete_entry(const string& table_name, const string& column_name, const string& key, int record_id);

    vector<int> search(const string& table_name, const string& column_name, const string& key);
    vector<int> range_search(const string& table_name, const string& column_name, const string& start_key, const string& end_key);
};