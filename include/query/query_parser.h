#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include <string>
#include <vector>
#include "../catalog_manager.h"
#include "../table_manager.h"
#include "../index_manager.h"
#include "../record_manager.h"

class QueryParser {
public:
    QueryParser(CatalogManager& cm, TableManager& tm, IndexManager& im);

    // Main entry point: execute a SQL query string
    // Returns true if successful, false otherwise.
    
    bool execute_query(const std::string& query);
    void run_interactive();
    
    private:
    CatalogManager& catalog_manager;
    TableManager& table_manager;
    IndexManager& index_manager;
    
    // Parse and execute different types of queries
    bool parse_create_table(const std::string& query);
    bool parse_drop_table(const std::string& query);
    bool parse_insert(const std::string& query);
    bool parse_delete(const std::string& query);
    bool parse_update(const std::string& query);
    bool parse_select(const std::string& query);
    bool parse_print_table(const std::string& query);
    
    
    // Utility parsing helpers
    static void trim(std::string& s);
    static std::vector<std::string> split(const std::string& s, char delimiter);

};

#endif // QUERY_PARSER_H
