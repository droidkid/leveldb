#include <iostream>
#include <cassert>

#include "leveldb/db.h"
#include "mod/zipf.h"
using namespace std;

#define NUM_KEYS 5000000
const std::string DB_NAME = "./DB";
const std::string VALUE = "vaaddd                                                                                dcdwcwcwcewcwecwecwecwecewcewcewdewdwcwecwecwecwecwecwecwecwecwecwecwecfevcggggggggglue::value::value::dljiosdjfskldjfsdlkjfsdlkjfsdlkfjsd";

string generate_key(const string& key) {
    int key_size=10;
    string result = string(key_size - key.length(), '0') + key;
    return std::move(result);
}

int main() {
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
    std::string stats;
    std::cout<<db->GetProperty("leveldb.stats", &stats);
    std::cout<<stats<<std::endl;

    cout<<"Ok!"<<endl;
}
