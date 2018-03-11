//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_KVSTORE_H
#define CELONIS_KV_SERVER_KVSTORE_H

#include <string>
#include <unordered_map>
#include "BucketManager.h"

using namespace std;



class KVStore {

public:

    KVStore();

    void put(string key, string value);

    bool get(string key, string *result);

    void del(string key);

    //debug
    void print_table_layout();
private:

    size_t global_depth = 0;

    Bucket &get_bucket(size_t hash);

    BucketManager bm;

    vector<size_t> pages;
    size_t max_bucket_no = 0;


};


#endif //CELONIS_KV_SERVER_KVSTORE_H
