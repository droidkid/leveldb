#include <iostream>
#include <cassert>
#include <fstream>

#include "leveldb/db.h"
#include "mod/zipf.h"
#include "mod/config.h"
using namespace std;

const std::string DB_NAME = "./DB";
const std::string VALUE = "vaaddd                                                                                dcdwcwcwcewcwecwecwecwecewcewcewdewdwcwecwecwecwecwecwecwecwecwecwecwecfevcggggggggglue::value::value::dljiosdjfskldjfsdlkjfsdlkjfsdlkfjsd";

string generate_key(const string& key) {
    int key_size=10;
    string result = string(key_size - key.length(), '0') + key;
    return std::move(result);
}

int main() {
    #if LOG_METRICS
    std::ofstream stats;
    stats.open("stats.csv", std::ofstream::out);
    stats << "NUM_ITEMS" <<",";
    stats << "COMP_COUNT" <<",";
    stats << "LEARNED_COMP_COUNT" <<",";
    stats << "CDF_ABS_ERROR" <<",";
    stats.close();
    #endif

    // Destroy the DB and create it again. 
    // We want to start from scratch.
    leveldb::Options options;
    leveldb::Status status = leveldb::DestroyDB(DB_NAME, options);
    assert(status.ok() || status.IsNotFound());

    leveldb::DB* db;
    // Probably don't reuse options, safer to create a new one.
    options.create_if_missing = true;
    status = leveldb::DB::Open(options, DB_NAME, &db);
    assert(status.ok());

    //uint64_t *elems = new uint64_t[100000];
    //create_zipfian();
    ZIPFIAN z = create_zipfian(1.5, 10000000, random);
    long g = zipfian_gen(z);


    //generate_random_keys(elems, 1000000, 100000, 1.5);

    for(int i=0; i< 5000000; i++){
        long g = zipfian_gen(z);
        string key = generate_key(to_string(g));
        string *value = new string(VALUE);
        db->Put(leveldb::WriteOptions(), key, *value);
        delete value;  
    }
    // for (int i=0; i < NUM_KEYS; i++) {
    //     string key = generate_key(to_string(rand() % NUM_KEYS));
    //     string *value = new string(VALUE);
    //     db->Put(leveldb::WriteOptions(), key, *value);
    //     delete value;
    // }

    std::cout<<"DB Stats"<<std::endl;
    std::string db_stats;
    std::cout<<db->GetProperty("leveldb.stats", &db_stats);
    std::cout<<db_stats<<std::endl;

    cout<<"Ok!"<<endl;
}
