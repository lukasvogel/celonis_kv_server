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

    ~KVStore();

    void put(const string &key, const string &value);

    bool get(const string &key, string &result);

    void del(const string &key);

    void stop();



    //debug
    void print_table_layout();
private:

    Bucket &get_bucket(size_t hash, bool exclusive);


    int index_file_fd;
    size_t global_depth = 0;
    size_t max_bucket_no = 0;

    BucketManager bm;
    vector<size_t> buckets;

    pthread_rwlock_t hashtable_lock;

};


#endif //CELONIS_KV_SERVER_KVSTORE_H
