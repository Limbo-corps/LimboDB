#include "../../include/query/query_parser.h"
#include <sstream>
#include <iostream>
#include <algorithm>
#include "../../include/record_manager.h"
#include "pretty.hpp"

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
    // } else if (q.find("select * from") == 0) {
    //     return parse_print_table(query);
    } else if (q.find("select") == 0) {
        return parse_select(query);
    } else if(q.find("create index on ") == 0){
        return parse_create_index(query);
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
    transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t start = query_lower.find("table");
    if (start == std::string::npos) {
        cout << "[ERROR] Syntax error in CREATE TABLE." << endl;
        return false;
    }

    size_t open_paren = query.find('(', start);
    size_t close_paren = query.find_last_of(')');

    if(open_paren == std::string::npos && close_paren == std::string::npos){
        cout<<"[ERROR] Invalid syntax: expected column definition list in parentheses." << endl;
        return false;
    }

    std::string table_name = query.substr(start + 5, open_paren - (start + 5));
    trim(table_name);

    string columns_str = query.substr(open_paren + 1, close_paren - open_paren - 1);
    cout<< "[DEBUG] columns str: "<< columns_str<<endl;
    vector<string> column_defs = split(columns_str, ',');
    vector<string> columns;
    vector<DataType> types;
    string primary_key_name;

    for(auto& col_def : column_defs){
        trim(col_def);
        string col_def_lower = col_def;
        std::transform(col_def_lower.begin(), col_def_lower.end(), col_def_lower.begin(), ::tolower);

        if(col_def_lower.rfind("primary key", 0) == 0){
            size_t pk_start = col_def.find('(');
            size_t pk_end = col_def.find(')');
            if(pk_start == string::npos || pk_end == string::npos || pk_end <= pk_start + 1){
                cout<<"[ERROR] Invalid PRIMARY KEY syntax."<<endl;
                return false;
            }

            primary_key_name = col_def.substr(pk_start + 1, pk_end - pk_start - 1);
            trim(primary_key_name);
            continue;
        }

        auto parts = split(col_def, ' ');
        if(parts.size() != 2){
            cout<< "[ERROR] Invalid column definition: "<<col_def<<endl;
            return false;
        }

        string col_name = parts[0];
        cout<<"[DEBUG] Column name: " << col_name<<endl;
        string type_name = parts[1];
        cout<<"[DEBUG] Type name: " << type_name<<endl;

        std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::toupper);

        DataType type = parse_type(type_name);
        if(type == DataType::UNKNOWN){
            cout<<"[ERROR] Unsupported type: " << type_name << endl;
            return false;
        }

        columns.push_back(col_name);
        types.push_back(type);
    }

    //Finding the index of the primary key
    if(primary_key_name.empty()){
        cout<<"[ERROR] PRIMARY KEY must be specified."<<endl;
        return false;
    }

    int pk_idx = -1;
    for(size_t i = 0; i < columns.size(); i++){
        if(columns[i] == primary_key_name){
            pk_idx = static_cast<int>(i);
            break;
        }
    }

    if(pk_idx == -1){
        cout<< "[ERROR] PRIMARY KEY column '" << primary_key_name << "' not found in the column list." << endl;
        return false;
    }

    //Call Catalog Manager to create the table
    return table_manager.create_table(table_name, columns, types, pk_idx);
}


bool QueryParser::parse_drop_table(const std::string& query) {
    std::string query_lower = query;
    transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos1 = query_lower.find("table");
    if (pos1 == string::npos) {
        cout << "[ERROR] Syntax error in DROP TABLE." << endl;
        return false;
    }
    string after_table = query.substr(pos1 + 5);
    trim(after_table);

    string table_name = after_table;
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
    std::string query_lower = query;
    transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

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

    string table_and_cols = after_into.substr(0, pos_values);
    trim(table_and_cols);

    string table_name;
    vector<string> column_list;
    size_t paren_open = table_and_cols.find('(');
    size_t paren_close = table_and_cols.find(')');
    if (paren_open != string::npos && paren_close != string::npos && paren_close > paren_open) {
        table_name = table_and_cols.substr(0, paren_open);
        trim(table_name);
        string cols_str = table_and_cols.substr(paren_open + 1, paren_close - paren_open - 1);
        column_list = split(cols_str, ',');
    } else {
        table_name = table_and_cols;
        trim(table_name);
    }

    string after_values = after_into.substr(pos_values + 6);
    trim(after_values);

    if (after_values.empty() || after_values.front() != '(' || (after_values.back() != ';' && after_values.back() != ')')) {
        cout << "[ERROR] Syntax error in VALUES clause." << endl;
        return false;
    }

    if (after_values.back() == ';') {
        after_values.pop_back();
    }

    if (after_values.front() == '(' && after_values.back() == ')') {
        after_values = after_values.substr(1, after_values.size() - 2);
    }

    vector<string> values = split(after_values, ',');

    if (!column_list.empty()) {
        auto schema = catalog_manager.get_schema(table_name);
        if (schema.columns.size() != values.size() || column_list.size() != values.size()) {
            cout << "[ERROR] Number of columns and values do not match." << endl;
            return false;
        }
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
    std::string query_lower = query;
    transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

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

    string prefix = "record_id =";
    if (where_clause.find(prefix) != 0) {
        cout << "[ERROR] DELETE only supports WHERE record_id = <id> for now." << endl;
        return false;
    }
    string id_str = where_clause.substr(prefix.size());
    trim(id_str);

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
    //UPDATE users SET name = 'Alice', age = 30 WHERE id = 1;
    string query_lower = query;
    transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);
    
    size_t pos_set = query_lower.find("set");
    if(pos_set == string::npos){
        cout<<"[ERROR] Syntax Error: missing SET"<<endl;
        return false;
    }

    size_t pos_where = query_lower.find("where");
    if(pos_where == string::npos){
        cout<<"[ERROR] UPDATE requires a WHERE clause."<<endl;
        return false;
    }

    string table_name = query.substr(6, pos_set - 6);
    trim(table_name);

    string set_clause = query.substr(pos_set + 3, pos_where - (pos_set + 3));
    trim(set_clause);

    string where_clause = query.substr(pos_where + 5);
    trim(where_clause);
    if(!where_clause.empty() && where_clause.back() == ';') where_clause.pop_back();

    size_t eq_pos = where_clause.find('=');
    if(eq_pos == string::npos){
        cout<<"[ERROR] Invalid WHERE clause."<<endl;
        return false;
    }
    string where_col = where_clause.substr(0, eq_pos);
    string where_val = where_clause.substr(eq_pos + 1);
    trim(where_col);
    trim(where_val);

    //validate table and column
    TableSchema schema = catalog_manager.get_schema(table_name);
    cout << "[INFO] Columns in table '" << table_name << "': ";
    for (const auto& col : schema.columns) {
        cout << col << " ";
    }
    cout << endl;
    if (schema.columns.empty()) {
        cout << "[ERROR] Table " << table_name << " does not exist." << endl;
        return false;
    }

    auto where_it = find(schema.columns.begin(), schema.columns.end(), where_col);
    if(where_it == schema.columns.end()){
        cout<<"[ERROR] Column '"<< where_col <<"' not found in table '" << table_name <<"'."<<endl;
        return false;
    }

    int where_col_idx = distance(schema.columns.begin(), where_it);
    vector<int> matching_ids = index_manager.search(table_name, where_col, where_val);

    //Parse SET assignments
    vector<string> assignments = split(set_clause, ',');
    if(assignments.empty()){
        cout<<"[ERROR] No assignments in SET clause."<<endl;
        return false;
    }

    //apply changes
    bool any_success = false;
    for(int record_id : matching_ids){
        Record rec = table_manager.select(table_name, record_id);
        if(rec.data.empty()){
            cout<< "[WARNING] Skipping invalid RecordID: "<<record_id <<endl;
            continue;
        }

        vector<string> current_record = table_manager.unpack_record(rec, schema);

        for(const string& assign : assignments){
            size_t eq_pos = assign.find("=");

            if(eq_pos == string::npos){
                cout<<"[ERROR] Invalid assignment: "<< assign << endl;
                return false;
            }

            string col = assign.substr(0, eq_pos);
            string val = assign.substr(eq_pos + 1);
            trim(col);
            trim(val);

            auto it = find(schema.columns.begin(), schema.columns.end(), col);
            if(it == schema.columns.end()){
                cout<<"[ERROR] Column '"<< col << "' not found in table '" << table_name << "'." << endl;
                return false;
            }

            int col_idx = distance(schema.columns.begin(), it);
            current_record[col_idx] = val;
        }

        if(table_manager.update(table_name, record_id, current_record)){
            any_success = true;
            cout<< "[INFO] Record " << record_id << " updated." << endl;
        } else {
            cout << "[ERROR] Failed to update record ID " << record_id << "." << endl;
        }
    }

    if(!any_success){
        cout << "[INFO] No matching records were updated." << endl;
    }

    return any_success;
}
bool QueryParser::parse_select(const std::string& query) {
    std::string query_lower = query;
    transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t pos_select = query_lower.find("select");
    size_t pos_from = query_lower.find("from");
    if (pos_select == string::npos || pos_from == string::npos || pos_from <= pos_select + 6) {
        cout << "[ERROR] Syntax error in SELECT: Missing or misplaced SELECT/FROM clause." << endl;
        return false;
    }

    string select_clause = query.substr(pos_select + 6, pos_from - (pos_select + 6));
    trim(select_clause);

    string after_from = query.substr(pos_from + 4);
    trim(after_from);

    size_t pos_where_global = query_lower.find("where", pos_from + 4);
    string table_name;
    string where_clause;

    if (pos_where_global != string::npos) {
        table_name = query.substr(pos_from + 4, pos_where_global - (pos_from + 4));
        trim(table_name);
        where_clause = query.substr(pos_where_global + 5);  // 5 = length of "where"
        trim(where_clause);
        if (!where_clause.empty() && where_clause.back() == ';') where_clause.pop_back();
    } else {
        table_name = query.substr(pos_from + 4);
        if (!table_name.empty() && table_name.back() == ';') table_name.pop_back();
        trim(table_name);
    }

    TableSchema schema = catalog_manager.get_schema(table_name);
    if (schema.table_name.empty()) {
        cout << "[ERROR] Table '" << table_name << "' does not exist." << endl;
        return false;
    }

    vector<string> selected_columns;
    vector<int> selected_indices;

    if (select_clause == "*") {
        selected_columns = schema.columns;
        for (int i = 0; i < schema.columns.size(); ++i) selected_indices.push_back(i);
    } else {
        selected_columns = split(select_clause, ',');
        for (auto& col : selected_columns) trim(col);

        for (const string& col : selected_columns) {
            auto it = find(schema.columns.begin(), schema.columns.end(), col);
            if (it == schema.columns.end()) {
                cout << "[ERROR] Column '" << col << "' not found in table '" << table_name << "'" << endl;
                return false;
            }
            selected_indices.push_back(it - schema.columns.begin());
        }
    }

    vector<Record> results;
    if (!where_clause.empty()) {
        results = where_clause_handler(where_clause, schema, table_name);
    } else {
        results = table_manager.scan(table_name);
    }

    // Output using pretty table
    pretty::Table output_table;
    output_table.add_row(selected_columns);

    for (const Record& rec : results) {
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> row = split(rec_str, '|');

        vector<string> selected_row;
        for (int idx : selected_indices) {
            if (idx < row.size()) selected_row.push_back(row[idx]);
            else selected_row.push_back("");
        }

        output_table.add_row(selected_row);
    }

    if (results.empty()) {
        vector<string> empty_row(selected_columns.size(), "");
        empty_row[0] = "No matching records";
        output_table.add_row(empty_row);
    }

    pretty::Printer printer;
    printer.frame(pretty::FrameStyle::Basic);
    cout << printer(output_table) << endl;

    return true;
}


void QueryParser::run_interactive() {
    std::string query;
    std::cout << "Enter SQL queries (type 'exit' to quit):\n";
    while (true) {
        std::cout << "lsql> ";
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
    transform(q.begin(), q.end(), q.begin(), ::tolower);
    
    size_t pos = q.find("select * from ");
    if (pos == std::string::npos) return false;
    
    std::string tableName = query.substr(pos + 13);
    trim(tableName);
    
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

bool QueryParser::parse_create_index(const std::string& query){
    string query_lower = query;
    transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);

    size_t on_pos = query_lower.find("on");
    if(on_pos == string::npos){
        std::cout << "[ERROR] Syntax error in CREATE INDEX query." << std::endl;
        return false;
    }

    string after_on = query.substr(on_pos + 2);
    trim(after_on);

    size_t paren_start = after_on.find('(');
    size_t paren_end = after_on.find(')');

    if (paren_start == std::string::npos || paren_end == std::string::npos || paren_end <= paren_start) {
        std::cout << "[ERROR] Expected format: CREATE INDEX ON table(column);" << std::endl;
        return false;
    }

    std::string table = after_on.substr(0, paren_start);
    std::string column = after_on.substr(paren_start + 1, paren_end - paren_start - 1);
    trim(table);
    trim(column);

    if (!catalog_manager.column_exists(table, column)) {
        std::cout << "[ERROR] Column '" << column << "' does not exist in table '" << table << "'\n";
        return false;
    }

    if (index_manager.create_index(table, column)) {
        std::cout << "[INFO] Index created on " << table << "(" << column << ")\n";
        return true;
    } else {
        std::cout << "[ERROR] Failed to create index.\n";
        return false;
    }
}

// Defined Equation handler

#define DEBUG_COLOR_RESET  "\033[0m"
#define DEBUG_COLOR_RED    "\033[31m"
#define DEBUG_COLOR_GREEN  "\033[32m"
#define DEBUG_COLOR_YELLOW "\033[33m"
#define DEBUG_ERROR_LABEL       DEBUG_COLOR_RED "[ERROR]" DEBUG_COLOR_RESET
#define DEBUG_SUCCESS_LABEL     DEBUG_COLOR_GREEN "[SUCCESS]" DEBUG_COLOR_RESET
#define DEBUG_LABEL             DEBUG_COLOR_YELLOW "[DEBUG]" DEBUG_COLOR_RESET
#define DEBUG_EQHANDLER         DEBUG_COLOR_YELLOW "[EQHANDLER]" DEBUG_COLOR_RESET
#define DEBUG_ERROR             std::cout << DEBUG_ERROR_LABEL << DEBUG_EQHANDLER << " "
#define DEBUG_SUCCESS           std::cout << DEBUG_SUCCESS_LABEL << DEBUG_EQHANDLER << " "
#define DEBUG                   std::cout << DEBUG_LABEL << DEBUG_EQHANDLER << " "

vector<Record> QueryParser::where_clause_handler(const std::string& where_clause,const TableSchema& schema, const string& table_name) {
    // Priority order matters to avoid splitting at '=' before checking '>=' or '<='
    if (where_clause.find(">=") != string::npos) {
        return handle_greater_equal(where_clause, schema, table_name);
    } else if (where_clause.find("<=") != string::npos) {
        return handle_lesser_equal(where_clause, schema, table_name);
    } else if (where_clause.find("!=") != string::npos) {
        return handle_not_equal(where_clause, schema, table_name);
    } else if (where_clause.find('=') != string::npos) {
        return handle_equal(where_clause, schema, table_name);
    } else if (where_clause.find('>') != string::npos) {
        return handle_greater(where_clause, schema, table_name);
    } else if (where_clause.find('<') != string::npos) {
        return handle_lesser(where_clause, schema, table_name);
    } else {
        DEBUG_ERROR << "Unsupported or malformed WHERE clause: " << where_clause << std::endl;
        return {};
    }
}


vector<Record> QueryParser::handle_equal(const std::string& where_clause,const TableSchema& schema, const string& table_name){
    size_t op_pos = where_clause.find("=");
    vector<Record> results;

    if(op_pos == string::npos){
        DEBUG_ERROR << "Expected operator '=' not found in the WHERE clause." << std::endl;
        return {};
    }

    string col = where_clause.substr(0, op_pos);
    string val = where_clause.substr(op_pos + 1);
    trim(col), trim(val);

    auto it = find(schema.columns.begin(), schema.columns.end(), col);
    if(it == schema.columns.end()) {
        DEBUG_ERROR << "Column '"<<col<< "' not found in the table '"<< table_name << endl;
        return {};
    }

    int col_idx = it - schema.columns.begin();

    vector<int> matching_ids = index_manager.search(table_name, col, val);
    if(matching_ids.empty()){
        DEBUG_ERROR << "No Matching ids found for " << col << " = " << val << " in the table '" << table_name << "'" << endl;
        return {};
    }

    for(int id : matching_ids){
        Record rec = table_manager.select(table_name, id);
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> row = split(rec_str, '|');
        if(row.size() > col_idx && row[col_idx] == val){
            results.push_back(rec);
        }
    }

    if (!results.empty()) {
        DEBUG_SUCCESS << "Found " << results.size() << " record(s) for " << col << " = " << val << " in table '" << table_name << "'" << std::endl;
    }
    return results;
}

vector<Record> QueryParser::handle_not_equal(const std::string& where_clause, const TableSchema& schema, const string& table_name) {
    size_t op_pos = where_clause.find("!=");
    vector<Record> results;

    if(op_pos == string::npos){
        DEBUG_ERROR << "Expected operator '!=' not found in the WHERE clause." << std::endl;
        return {};
    }

    string col = where_clause.substr(0, op_pos);
    string val = where_clause.substr(op_pos + 2); // Skip "!="
    trim(col), trim(val);

    auto it = find(schema.columns.begin(), schema.columns.end(), col);
    if (it == schema.columns.end()) {
        DEBUG_ERROR << "Column '" << col << "' not found in table '" << table_name << "'" << std::endl;
        return {};
    }

    int col_idx = it - schema.columns.begin();

    // If index exists, use two range searches
    vector<int> matching_ids;
    if (index_manager.column_exists(table_name, col)) {
        vector<int> less_than = index_manager.range_search(table_name, col, "", val);     // col < val
        vector<int> greater_than = index_manager.range_search(table_name, col, val + '\1', "~"); // col > val ('~' is high ASCII)

        matching_ids.insert(matching_ids.end(), less_than.begin(), less_than.end());
        matching_ids.insert(matching_ids.end(), greater_than.begin(), greater_than.end());
    } else {
        // Fallback to full scan if no index
        for (const Record& rec : table_manager.scan(table_name)) {
            string rec_str(rec.data.begin(), rec.data.end());
            vector<string> row = split(rec_str, '|');
            if (row.size() > col_idx && row[col_idx] != val) {
                results.push_back(rec);
            }
        }
        if (!results.empty()) {
            DEBUG_SUCCESS << "Found " << results.size() << " record(s) for " << col << " != " << val << " in table '" << table_name << "'" << std::endl;
        }
        return results;
    }

    // Fetch and filter records by checking column != value
    for (int id : matching_ids) {
        Record rec = table_manager.select(table_name, id);
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> row = split(rec_str, '|');
        if (row.size() > col_idx && row[col_idx] != val) {
            results.push_back(rec);
        }
    }

    if (!results.empty()) {
        DEBUG_SUCCESS << "Found " << results.size() << " record(s) for " << col << " != " << val << " in table '" << table_name << "'" << std::endl;
    }
    return results;
}

vector<Record> QueryParser::handle_greater(const std::string& where_clause, const TableSchema& schema, const string& table_name){
    size_t op_pos = where_clause.find(">");
    vector<Record> results;

    if(op_pos == string::npos){
        DEBUG_ERROR << "Expected operator '>' not found in the WHERE clause." << std::endl;
        return {};
    }

    string col = where_clause.substr(0, op_pos);
    string val = where_clause.substr(op_pos+1);
    trim(col), trim(val);

    auto it = find(schema.columns.begin(), schema.columns.end(), col);
    if (it == schema.columns.end()) {
        DEBUG_ERROR << "Column '" << col << "' not found in table '" << table_name << "'" << std::endl;
        return {};
    }

    int col_idx = it - schema.columns.begin();

    //if Index exists use range search
    vector<int> matching_ids;
    if(index_manager.column_exists(table_name, col)){
        matching_ids = index_manager.range_search(table_name, col, val + '\1', "~");
    } else {
        DEBUG_ERROR << "Column '" << col << "' does not exist in the indices"<<endl;
    }

    for(int id : matching_ids){
        Record rec = table_manager.select(table_name, id);
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> row = split(rec_str, '|');
        if (row.size() > col_idx && row[col_idx] > val) {
            results.push_back(rec);
        }
    }
    if (!results.empty()) {
        DEBUG_SUCCESS << "Found " << results.size() << " record(s) for " << col << " > " << val << " in table '" << table_name << "'" << std::endl;
    }
    return results;
}

vector<Record> QueryParser::handle_lesser(const std::string& where_clause, const TableSchema& schema, const string& table_name) {
    size_t op_pos = where_clause.find("<");
    vector<Record> results;

    if(op_pos == string::npos){
        DEBUG_ERROR << "Expected operator '<' not found in the WHERE clause." << std::endl;
        return {};
    }

    string col = where_clause.substr(0, op_pos);
    string val = where_clause.substr(op_pos+1);
    trim(col), trim(val);

    auto it = find(schema.columns.begin(), schema.columns.end(), col);
    if (it == schema.columns.end()) {
        DEBUG_ERROR << "Column '" << col << "' not found in table '" << table_name << "'" << std::endl;
        return {};
    }

    int col_idx = it - schema.columns.begin();

    vector<int> matching_ids;
    if(index_manager.column_exists(table_name, col)){
        matching_ids = index_manager.range_search(table_name, col, "", val);
    } else {
        DEBUG_ERROR << "Column '" << col << "' does not exist in the indices" << endl;
    }

    for(int id : matching_ids){
        Record rec = table_manager.select(table_name, id);
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> row = split(rec_str, '|');
        if (row.size() > col_idx && row[col_idx] < val) {
            results.push_back(rec);
        }
    }
    if (!results.empty()) {
        DEBUG_SUCCESS << "Found " << results.size() << " record(s) for " << col << " < " << val << " in table '" << table_name << "'" << std::endl;
    }
    return results;
}

vector<Record> QueryParser::handle_greater_equal(const std::string& where_clause, const TableSchema& schema, const string& table_name) {
    size_t op_pos = where_clause.find(">=");
    vector<Record> results;

    if(op_pos == string::npos){
        DEBUG_ERROR << "Expected operator '>=' not found in the WHERE clause." << std::endl;
        return {};
    }

    string col = where_clause.substr(0, op_pos);
    string val = where_clause.substr(op_pos+2);
    trim(col), trim(val);

    auto it = find(schema.columns.begin(), schema.columns.end(), col);
    if (it == schema.columns.end()) {
        DEBUG_ERROR << "Column '" << col << "' not found in table '" << table_name << "'" << std::endl;
        return {};
    }

    int col_idx = it - schema.columns.begin();

    vector<int> matching_ids;
    if(index_manager.column_exists(table_name, col)){
        matching_ids = index_manager.range_search(table_name, col, val, "~");
    } else {
        DEBUG_ERROR << "Column '" << col << "' does not exist in the indices" << endl;
    }

    for(int id : matching_ids){
        Record rec = table_manager.select(table_name, id);
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> row = split(rec_str, '|');
        if (row.size() > col_idx && row[col_idx] >= val) {
            results.push_back(rec);
        }
    }
    if (!results.empty()) {
        DEBUG_SUCCESS << "Found " << results.size() << " record(s) for " << col << " >= " << val << " in table '" << table_name << "'" << std::endl;
    }
    return results;
}

vector<Record> QueryParser::handle_lesser_equal(const std::string& where_clause, const TableSchema& schema, const string& table_name) {
    size_t op_pos = where_clause.find("<=");
    vector<Record> results;

    if(op_pos == string::npos){
        DEBUG_ERROR << "Expected operator '<=' not found in the WHERE clause." << std::endl;
        return {};
    }

    string col = where_clause.substr(0, op_pos);
    string val = where_clause.substr(op_pos+2);
    trim(col), trim(val);

    auto it = find(schema.columns.begin(), schema.columns.end(), col);
    if (it == schema.columns.end()) {
        DEBUG_ERROR << "Column '" << col << "' not found in table '" << table_name << "'" << std::endl;
        return {};
    }

    int col_idx = it - schema.columns.begin();

    vector<int> matching_ids;
    if(index_manager.column_exists(table_name, col)){
        matching_ids = index_manager.range_search(table_name, col, "", val + '\1');
    } else {
        DEBUG_ERROR << "Column '" << col << "' does not exist in the indices" << endl;
    }

    for(int id : matching_ids){
        Record rec = table_manager.select(table_name, id);
        string rec_str(rec.data.begin(), rec.data.end());
        vector<string> row = split(rec_str, '|');
        if (row.size() > col_idx && row[col_idx] <= val) {
            results.push_back(rec);
        }
    }
    if (!results.empty()) {
        DEBUG_SUCCESS << "Found " << results.size() << " record(s) for " << col << " <= " << val << " in table '" << table_name << "'" << std::endl;
    }
    return results;
}