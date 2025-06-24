#include "../../include/query/query_parser.h"
#include <sstream>
#include <iostream>
#include <algorithm>

using namespace std;

QueryParser::QueryParser(CatalogManager& cm, TableManager& tm, IndexManager& im)
    : catalog_manager(cm), table_manager(tm), index_manager(im) {}


bool QueryParser::execute_query(const std::string& query) {
    string q = query;
    transform(q.begin(), q.end(), q.begin(), ::tolower);

    if (q.find("create table") == 0) {
        return parse_create_table(query);
    } else if (q.find("drop table") == 0) {
        return parse_drop_table(query);
    } else if (q.find("insert into") == 0) {
        return parse_insert(query);
    } else if (q.find("delete from") == 0) {
        return parse_delete(query);
    } else if (q.find("update") == 0) {
        return parse_update(query);
    } else if (q.find("select * from") == 0) {
        return parse_print_table(query);
    } else if (q.find("select") == 0) {
        return parse_select(query);
    }

    cout << "[ERROR] Unsupported or invalid query." << endl;
    return false;
}

void QueryParser::trim(string& s) {
    const char* whitespace = " \t\n\r";
    size_t start = s.find_first_not_of(whitespace);
    size_t end = s.find_last_not_of(whitespace);
    if (start == string::npos || end == string::npos) {
        s = "";
    } else {
        s = s.substr(start, end - start + 1);
    }
}

vector<string> QueryParser::split(const string& s, char delimiter) {
    vector<string> tokens;
    string token;
    istringstream tokenStream(s);
    while (getline(tokenStream, token, delimiter)) {
        trim(token);
        tokens.push_back(token);
    }
    return tokens;
}


bool QueryParser::parse_create_table(const std::string& query) {
    std::string query_lower = query;
    std::transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos1 = query_lower.find("table");
    if (pos1 == std::string::npos) {
        cout << "[ERROR] Syntax error in CREATE TABLE." << endl;
        return false;
    }

    // Use pos1 to slice the original query to preserve case
    std::string after_table = query.substr(pos1 + 5);
    trim(after_table);

    size_t pos_paren_open = after_table.find('(');
    size_t pos_paren_close = after_table.find(')');

    if (pos_paren_open == std::string::npos || pos_paren_close == std::string::npos || pos_paren_close < pos_paren_open) {
        cout << "[ERROR] Syntax error: missing parentheses in CREATE TABLE." << endl;
        return false;
    }

    std::string table_name = after_table.substr(0, pos_paren_open);
    trim(table_name);

    std::string cols_str = after_table.substr(pos_paren_open + 1, pos_paren_close - pos_paren_open - 1);

    // Remove trailing semicolon if present
    if (!cols_str.empty() && cols_str.back() == ';') {
        cols_str.pop_back();
    }

    vector<string> columns = split(cols_str, ',');
    if (columns.empty()) {
        cout << "[ERROR] No columns specified for CREATE TABLE." << endl;
        return false;
    }

    bool success = catalog_manager.create_table(table_name, columns);
    if (success) {
        cout << "[INFO] Table '" << table_name << "' created." << endl;
    } else {
        cout << "[ERROR] Table creation failed. Table may already exist." << endl;
    }
    return success;
}


bool QueryParser::parse_drop_table(const std::string& query) {
    // Expected format: DROP TABLE table_name;
    std::string query_lower = query;
    std::transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos1 = query_lower.find("table");
    if (pos1 == string::npos) {
        cout << "[ERROR] Syntax error in DROP TABLE." << endl;
        return false;
    }
    string after_table = query.substr(pos1 + 5);
    trim(after_table);

    string table_name = after_table;
    // Remove trailing semicolon if exists
    if (!table_name.empty() && table_name.back() == ';') {
        table_name.pop_back();
        trim(table_name);
    }

    bool success = catalog_manager.drop_table(table_name);
    if (success) {
        cout << "[INFO] Table '" << table_name << "' dropped." << endl;
    } else {
        cout << "[ERROR] Table drop failed. Table may not exist." << endl;
    }
    return success;
}

bool QueryParser::parse_insert(const std::string& query) {
    // Expected format:
    // INSERT INTO table_name [(col1, col2, ...)] VALUES (val1, val2, ...);
    std::string query_lower = query;
    std::transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos_into = query_lower.find("into");
    if (pos_into == string::npos) {
        cout << "[ERROR] Syntax error in INSERT INTO." << endl;
        return false;
    }
    string after_into = query.substr(pos_into + 4);
    trim(after_into);

    size_t pos_values = query_lower.substr(pos_into + 4).find("values");
    if (pos_values == string::npos) {
        cout << "[ERROR] Syntax error: missing VALUES clause." << endl;
        return false;
    }

    // Extract table name and optional column list
    string table_and_cols = after_into.substr(0, pos_values);
    trim(table_and_cols);

    string table_name;
    vector<string> column_list;
    size_t paren_open = table_and_cols.find('(');
    size_t paren_close = table_and_cols.find(')');
    if (paren_open != string::npos && paren_close != string::npos && paren_close > paren_open) {
        // There is a column list
        table_name = table_and_cols.substr(0, paren_open);
        trim(table_name);
        string cols_str = table_and_cols.substr(paren_open + 1, paren_close - paren_open - 1);
        column_list = split(cols_str, ',');
    } else {
        // No column list
        table_name = table_and_cols;
        trim(table_name);
    }

    string after_values = after_into.substr(pos_values + 6);
    trim(after_values);

    if (after_values.empty() || after_values.front() != '(' || (after_values.back() != ';' && after_values.back() != ')')) {
        cout << "[ERROR] Syntax error in VALUES clause." << endl;
        return false;
    }

    // remove trailing ';' if exists
    if (after_values.back() == ';') {
        after_values.pop_back();
    }

    if (after_values.front() == '(' && after_values.back() == ')') {
        after_values = after_values.substr(1, after_values.size() - 2);
    }

    vector<string> values = split(after_values, ',');

    // If column_list is empty, assume all columns in schema order
    // Otherwise, reorder values to match schema order
    if (!column_list.empty()) {
        auto schema = catalog_manager.get_schema(table_name);
        if (schema.columns.size() != values.size() || column_list.size() != values.size()) {
            cout << "[ERROR] Number of columns and values do not match." << endl;
            return false;
        }
        // Map column_list to schema order
        vector<string> reordered_values(schema.columns.size());
        for (size_t i = 0; i < column_list.size(); ++i) {
            string col = column_list[i];
            trim(col);
            auto it = std::find(schema.columns.begin(), schema.columns.end(), col);
            if (it == schema.columns.end()) {
                cout << "[ERROR] Column '" << col << "' not found in table '" << table_name << "'." << endl;
                return false;
            }
            size_t idx = std::distance(schema.columns.begin(), it);
            reordered_values[idx] = values[i];
        }
        values = reordered_values;
    }

    int record_id = table_manager.insert_into(table_name, values);
    if (record_id == -1) {
        cout << "[ERROR] Insert failed." << endl;
        return false;
    }

    cout << "[INFO] Inserted record ID: " << record_id << endl;
    return true;
}

bool QueryParser::parse_delete(const std::string& query) {
    // Expected format:
    // DELETE FROM table_name WHERE record_id = some_id;
    std::string query_lower = query;
    std::transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos_from = query_lower.find("from");
    if (pos_from == string::npos) {
        cout << "[ERROR] Syntax error in DELETE." << endl;
        return false;
    }
    string after_from = query.substr(pos_from + 4);
    trim(after_from);

    size_t pos_where = query_lower.substr(pos_from + 4).find("where");
    if (pos_where == string::npos) {
        cout << "[ERROR] DELETE requires WHERE clause." << endl;
        return false;
    }
    string table_name = after_from.substr(0, pos_where);
    trim(table_name);

    string where_clause = after_from.substr(pos_where + 5);
    trim(where_clause);

    // Currently support only WHERE record_id = <id>
    string prefix = "record_id =";
    if (where_clause.find(prefix) != 0) {
        cout << "[ERROR] DELETE only supports WHERE record_id = <id> for now." << endl;
        return false;
    }
    string id_str = where_clause.substr(prefix.size());
    trim(id_str);

    // remove trailing ';' if any
    if (!id_str.empty() && id_str.back() == ';') {
        id_str.pop_back();
    }

    int record_id = stoi(id_str);

    bool success = table_manager.delete_from(table_name, record_id);
    if (!success) {
        cout << "[ERROR] Delete failed." << endl;
    } else {
        cout << "[INFO] Record deleted successfully." << endl;
    }
    return success;
}

bool QueryParser::parse_update(const std::string& query) {
    // Expected format:
    // UPDATE table_name SET col1 = val1, col2 = val2 WHERE record_id = some_id;
    std::string query_lower = query;
    std::transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos_set = query_lower.find("set");
    if (pos_set == string::npos) {
        cout << "[ERROR] Syntax error in UPDATE: missing SET." << endl;
        return false;
    }
    string before_set = query.substr(6, pos_set - 6); // 6 is length of "update"
    trim(before_set);
    string table_name = before_set;

    size_t pos_where = query_lower.find("where");
    if (pos_where == string::npos) {
        cout << "[ERROR] UPDATE requires WHERE clause." << endl;
        return false;
    }
    string set_clause = query.substr(pos_set + 3, pos_where - (pos_set + 3));
    trim(set_clause);

    string where_clause = query.substr(pos_where + 5);
    trim(where_clause);

    // parse record_id from WHERE clause (support only record_id = <id>)
    string prefix = "record_id =";
    if (where_clause.find(prefix) != 0) {
        cout << "[ERROR] UPDATE only supports WHERE record_id = <id> for now." << endl;
        return false;
    }
    string id_str = where_clause.substr(prefix.size());
    trim(id_str);
    if (!id_str.empty() && id_str.back() == ';') {
        id_str.pop_back();
    }
    int record_id = stoi(id_str);

    // parse set clause: col1=val1, col2=val2
    vector<string> assignments = split(set_clause, ',');
    if (assignments.empty()) {
        cout << "[ERROR] No assignments in SET clause." << endl;
        return false;
    }

    // fetch current record and schema
    auto schema = catalog_manager.get_schema(table_name);
    if (schema.columns.empty()) {
        cout << "[ERROR] Table '" << table_name << "' does not exist." << endl;
        return false;
    }

    // Get the current record data as vector<string>
    Record rec = table_manager.select(table_name, record_id);
    vector<string> current_record;
    for (const auto& c : rec.data) {
        current_record.push_back(string(1, c));
    }
    if (current_record.empty()) {
        cout << "[ERROR] Record ID " << record_id << " not found." << endl;
        return false;
    }

    // update fields
    for (const string& assign : assignments) {
        size_t eq_pos = assign.find('=');
        if (eq_pos == string::npos) {
            cout << "[ERROR] Invalid assignment: " << assign << endl;
            return false;
        }
        string col = assign.substr(0, eq_pos);
        string val = assign.substr(eq_pos + 1);
        trim(col);
        trim(val);

        // find index of column in schema
        auto it = find(schema.columns.begin(), schema.columns.end(), col);
        if (it == schema.columns.end()) {
            cout << "[ERROR] Column '" << col << "' not found in table '" << table_name << "'." << endl;
            return false;
        }
        int col_idx = distance(schema.columns.begin(), it);
        current_record[col_idx] = val;
    }

    // update record in table
    bool success = table_manager.update(table_name, record_id, current_record);
    if (success) {
        cout << "[INFO] Record updated successfully." << endl;
    } else {
        cout << "[ERROR] Update failed." << endl;
    }
    return success;
}

bool QueryParser::parse_select(const std::string& query) {
    // Support only:
    // SELECT * FROM table_name WHERE record_id = some_id;
    // or
    // SELECT * FROM table_name;
    std::string query_lower = query;
    std::transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos_from = query_lower.find("from");
    if (pos_from == string::npos) {
        cout << "[ERROR] Syntax error in SELECT: missing FROM." << endl;
        return false;
    }

    string after_from = query.substr(pos_from + 4);
    trim(after_from);

    // Check for WHERE clause
    size_t pos_where = query_lower.substr(pos_from + 4).find("where");
    string table_name;
    string where_clause;
    if (pos_where == string::npos) {
        // No WHERE, select all
        table_name = after_from;
        // Remove trailing semicolon if exists
        if (!table_name.empty() && table_name.back() == ';') {
            table_name.pop_back();
        }
        trim(table_name);

        vector<Record> records = table_manager.scan(table_name);
        if (records.empty()) {
            cout << "[INFO] No records found in '" << table_name << "'." << endl;
            return true;
        }

        // Print header
        auto schema = catalog_manager.get_schema(table_name);
        for (const auto& col : schema.columns) {
            cout << col << "\t";
        }
        cout << endl;

        // Print rows
        for (const auto& rec : records) {
            string rec_str(rec.data.begin(), rec.data.end());
            vector<string> row = split(rec_str, '|');
            for (const auto& val : row) {
                cout << val << "\t";
            }
            cout << endl;
        }
        return true;
    } else {
        table_name = after_from.substr(0, pos_where);
        trim(table_name);
        where_clause = after_from.substr(pos_where + 5);
        trim(where_clause);

        // Remove trailing semicolon if any
        if (!where_clause.empty() && where_clause.back() == ';') {
            where_clause.pop_back();
        }

        // Only support WHERE record_id = <id>
        string prefix = "record_id =";
        if (where_clause.find(prefix) != 0) {
            cout << "[ERROR] SELECT only supports WHERE record_id = <id> for now." << endl;
            return false;
        }

        string id_str = where_clause.substr(prefix.size());
        trim(id_str);

        int record_id = stoi(id_str);

        Record rec = table_manager.select(table_name, record_id);
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> record = split(rec_str, '|');
        if (record.empty()) {
            cout << "[INFO] No record found with ID " << record_id << endl;
            return true;
        }

        auto schema = catalog_manager.get_schema(table_name);
        // Print header
        for (const auto& col : schema.columns) {
            cout << col << "\t";
        }
        cout << endl;

        // Print record
        for (const auto& val : record) {
            cout << val << "\t";
        }
        cout << endl;

        return true;
    }
}


void QueryParser::run_interactive() {
    std::string query;
    std::cout << "Enter SQL queries (type 'exit' to quit):\n";
    while (true) {
        std::cout << "SQL> ";
        std::getline(std::cin, query);

        if (query == "exit" || query == "quit") {
            std::cout << "Exiting interactive mode.\n";
            break;
        }
        if (query.empty()) {
            continue;
        }

        bool success = this->execute_query(query);
        if (!success) {
            std::cout << "[ERROR] Failed to execute query.\n";
        }
    }
}

bool QueryParser::parse_print_table(const std::string& query) {
    std::string q = query;
    std::transform(q.begin(), q.end(), q.begin(), ::tolower);
    
    // Extract table name
    size_t pos = q.find("select * from ");
    if (pos == std::string::npos) return false;
    
    std::string tableName = query.substr(pos + 13); // "print table" is 11 chars
    trim(tableName);
    
    // Remove trailing semicolon
    if (!tableName.empty() && tableName.back() == ';') {
        tableName.pop_back();
    }
    trim(tableName);
    
    if (tableName.empty()) {
        std::cout << "[ERROR] Missing table name" << std::endl;
        return false;
    }
    
    table_manager.printTable(tableName);
    return true;
}