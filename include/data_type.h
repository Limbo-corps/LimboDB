#pragma once
#include<string>

enum class DataType {
    INT,
    VARCHAR,
    FLOAT,
    UNKNOWN
};

inline DataType parse_type(const std::string& str){
    if(str == "INT") return DataType::INT;
    if(str == "VARCHAR") return DataType::VARCHAR;
    if(str == "FLOAT") return DataType::FLOAT;
    return DataType::UNKNOWN;
}

inline std::string to_string(DataType type){
    switch(type){
        case DataType::INT: return "INT";
        case DataType::VARCHAR: return "VARCHAR";
        case DataType::FLOAT: return "FLOAT";
        default: return "UNKNOWN";
    }
}