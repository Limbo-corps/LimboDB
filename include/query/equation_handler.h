#pragma once
#include<string>

using namespace std;

class EquationHandler {
private:
    string where_clause;
public:
    bool handle_not_equal(const std::string& where_clause);
    bool handle_equal(const std::string& where_clause);
    bool handle_greater(const std::string& where_clause);
    bool handle_lesser(const std::string& where_clause);
    bool handle_greater_equal(const std::string& where_clause);
    bool handle_lesser_equal(const std::string& where_clause);
};