#pragma once
#include "./disk_manager.h"
#include<unordered_map>
#include <cstdint>
#include <vector>
#include <string>
#include<cstring>
#include "record_id.h"

using namespace std;

const int HEADER_SIZE = 4; // Size of the header in each page (2 bytes for slot count, 2 bytes for free offset)
const int SLOT_SIZE = 4; // Size of each slot in the header
const uint16_t INVALID_SLOT = 0xFFFF; // Invalid slot value

struct Record{
    vector<char> data;
    RecordID rid;

    Record(const string& str){
        data.assign(str.begin(), str.end());
    }

    Record(const vector<char>& raw){
        data = raw;
    }

    Record(const vector<char>& raw, const RecordID& id){
        data = raw;
        rid = id;
    }

    string to_string() const {
        return string(data.begin(), data.end());
    }

    RecordID get_record_id(){
        return rid;
    }
};

class RecordManager{
private:
    DiskManager& disk;
    int next_page_id;

    int find_free_page();
    // pair<int, int> decode_record_id(int record_id);
    // int encode_record_id(int page_id, int slot_id);

public:
    RecordManager(DiskManager& dm);

    DiskManager& get_disk() {
        return disk;
    }

    int insert_record(const Record& record);
    Record get_record(int record_id);
    void delete_record(int record_id);
    int update_record(int record_id, const Record& record);
};