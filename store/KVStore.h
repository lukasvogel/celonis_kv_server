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

    /**
     * Searches for the given key and writes its value to result.
     * Returns true, if the key has been found, false otherwise.
     */
    bool get(const string &key, string &result);

    /**
     * Searches for the given key and deletes its entry.
     */
    void del(const string &key);

    /**
     * "Stops" the store by:
     *    - Flushing all loaded buckets to disk
     *    - Flushing the index to disk
     *  WARNING: this method is not thread-safe and is meant to be called
     *  after the server is stopped to persist all changes.
     */
    void stop();


    /**
     * Prints the layout of the index for debugging purposes
     */
    void print_index_layout();
private:

    /**
     * Gets the bucket for the given hash, taking into account the global depth.
     */
    Bucket &get_bucket(size_t hash, bool exclusive);


    int index_file_fd;
    size_t global_depth = 0;
    size_t max_bucket_no = 0;

    BucketManager bm;
    vector<size_t> buckets;

    pthread_rwlock_t hashtable_lock;

};


#endif //CELONIS_KV_SERVER_KVSTORE_H
