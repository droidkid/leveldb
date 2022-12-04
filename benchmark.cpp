#include <iostream>
#include <cassert>
#include <fstream>
#include <vector>

#include "leveldb/db.h"
#include "mod/zipf.h"
#include "mod/config.h"
using namespace std;

#define BENCH_RANDOM_KEYS 0
#define BENCH_ZIPF_KEYS 1

const std::string DB_NAME = "./DB";

string generate_key(uint64_t key_value) {
    string key = to_string(key_value);
    string result = string(KEY_SIZE - key.length(), '0') + key;
    return std::move(result);
}

void generate_keys(int test_case, vector<std::string> &keys) {
    if (test_case ==  BENCH_RANDOM_KEYS) {
        for(int i=0; i< NUM_KEYS; i++){
            keys.push_back(generate_key(random() % KEY_UNIVERSE));
        }
        return;
    }
    if (test_case == BENCH_ZIPF_KEYS) {
        ZIPFIAN z = create_zipfian(ZIPF_POWER, KEY_UNIVERSE, random);
        for(int i=0; i< NUM_KEYS; i++){
            keys.push_back(generate_key(zipfian_gen(z)));
        }
        return;

    }
    assert(false);
}

int main(int argc, char **argv) {
    #if LOG_METRICS
        std::ofstream stats;
        stats.open("stats.csv", std::ofstream::out);
        stats << "NUM_ITEMS" <<",";
        stats << "COMP_COUNT" <<",";
        stats << "LEARNED_COMP_COUNT" <<",";
        stats << "CDF_ABS_ERROR" <<",";
        stats << "NUM_ITERATORS" <<"\n";
        stats.close();
    #endif

    // TODO: Add error checks here.
    int test_case = argv[1][0] - '0';
    vector<std::string> keys;
    generate_keys(test_case, keys);

    // Destroy the DB and create it again. 
    leveldb::Options options;
    leveldb::Status status = leveldb::DestroyDB(DB_NAME, options);
    assert(status.ok() || status.IsNotFound());

    leveldb::DB* db;
    options.create_if_missing = true;
    status = leveldb::DB::Open(options, DB_NAME, &db);
    assert(status.ok());

    for(auto k: keys) { 
        db->Put(leveldb::WriteOptions(), k, k);
    }

    for(auto k: keys) { 
        std::string value;
        status = db->Get(leveldb::ReadOptions(), k, &value);
        assert(status.ok() && value == k);
    }

    std::cout<<"DB Stats"<<std::endl;
    std::string db_stats;
    std::cout<<db->GetProperty("leveldb.stats", &db_stats);
    std::cout<<db_stats<<std::endl;

    cout<<"Ok!"<<endl;
}
