#include "./include/query/query_parser.h"
#include "./include/disk_manager.h"
#include "./include/record_manager.h"
#include "./include/catalog_manager.h"
#include "./include/table_manager.h"
#include "./include/index_manager.h"
#include "./include/global-state.h"
#include "./include/utils/string_utils.h"

#include<filesystem>
#include<iostream>
#include<string>

using namespace std;

namespace fs = std::filesystem;

void run_sql_shell() {
    std::string query;
    std::cout << "Welcome to LimboDB (Multi-Database Mode)\n";
    std::cout << "Type `HELP;` for commands.\n";

    QueryParser* parser = nullptr;

    // These need to live outside the loop and be managed properly
    DiskManager* disk_manager = nullptr;
    RecordManager* record_manager = nullptr;
    IndexManager* index_manager = nullptr;
    CatalogManager* catalog_manager = nullptr;
    TableManager* table_manager = nullptr;

    while (true) {
        std::cout << "lsql> ";
        std::getline(std::cin, query);

        if (query == "exit" || query == "quit") {
            std::cout << "Exiting LimboDB.\n";
            break;
        }

        if (query.empty()) continue;

        std::string q_lower = query;
        std::transform(q_lower.begin(), q_lower.end(), q_lower.begin(), ::tolower);

        // CREATE DATABASE
        if (q_lower.find("create database ") == 0) {
            std::string dbname = query.substr(16);
            if (!dbname.empty() && dbname.back() == ';') dbname.pop_back();
            trim(dbname);

            std::string path = "data/" + dbname;
            if (fs::exists(path)) {
                std::cout << "[ERROR] Database already exists.\n";
            } else {
                fs::create_directories(path);
                std::ofstream(path + "/pages.db"); // create empty file
                std::cout << "[INFO] Database '" << dbname << "' created.\n";
            }
            continue;
        }

        // SHOW DATABASES
        if (q_lower == "show databases;" || q_lower == "show databases") {
            const std::string data_path = "data";
            if (!fs::exists(data_path)) {
                std::cout << "[INFO] No databases found. 'data/' directory does not exist.\n";
            } else {
                bool any = false;
                for (const auto& entry : fs::directory_iterator(data_path)) {
                    if (entry.is_directory()) {
                        std::cout << entry.path().filename().string() << "\n";
                        any = true;
                    }
                }
                if (!any) {
                    std::cout << "[INFO] No databases found.\n";
                }
            }
            continue;
        }

        // USE DATABASE
        if (q_lower.find("use ") == 0) {
            std::string dbname = query.substr(4);
            if (!dbname.empty() && dbname.back() == ';') dbname.pop_back();
            trim(dbname);

            std::string db_path = "data/" + dbname;
            if (!fs::exists(db_path)) {
                std::cout << "[ERROR] Database '" << dbname << "' does not exist.\n";
                continue;
            }

            
            // Clean up old managers
            delete parser;
            delete table_manager;
            delete catalog_manager;
            delete index_manager;
            delete record_manager;
            delete disk_manager;
            
            CURRENT_DATABASE = dbname;
            // Re-initialize managers with new database path
            disk_manager = new DiskManager(db_path + "/pages.db");
            record_manager = new RecordManager(*disk_manager);
            index_manager = new IndexManager();
            catalog_manager = new CatalogManager(*record_manager, *index_manager);
            table_manager = new TableManager(*catalog_manager, *record_manager, *index_manager);
            parser = new QueryParser(*catalog_manager, *table_manager, *index_manager);

            std::cout << "[INFO] Switched to database: " << dbname << "\n";
            continue;
        }

        if (CURRENT_DATABASE.empty()) {
            std::cout << "[ERROR] No database selected. Use: USE dbname;\n";
            continue;
        }

        if (parser) {
            bool success = parser->execute_query(query);
            if (!success) {
                std::cout << "[ERROR] Failed to execute query.\n";
            }
        }
    }

    // Clean up on exit
    delete parser;
    delete table_manager;
    delete catalog_manager;
    delete index_manager;
    delete record_manager;
    delete disk_manager;
}


int main() {
    run_sql_shell();
    return 0;
}