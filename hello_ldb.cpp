#include <iostream>
#include <cassert>

#include "leveldb/db.h"

using namespace std;

#define NUM_KEYS 1000000
const std::string DB_NAME = "./DB";
const std::string VALUE = "value::value::value";

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


    for (int i=0; i < NUM_KEYS; i++) {
        string *key = new string(to_string(i));
        string *value = new string(VALUE);
        db->Put(leveldb::WriteOptions(), *key, *value);
        // cout<<"Put: "<<key<<endl;
        delete key;
        delete value;
    }

    for (int i=0; i < NUM_KEYS; i++) {
        string key(to_string(i));
        std::string value;
        status = db->Get(leveldb::ReadOptions(), key, &value);
        assert(status.ok() && value == VALUE);
    }

    cout<<"Ok!"<<endl;

}

