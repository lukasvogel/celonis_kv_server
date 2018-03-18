//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_BUFFER_H
#define CELONIS_KV_SERVER_BUFFER_H


#include <sys/param.h>
#include <pthread.h>
#include <string>
#include "EntryPosition.h"
#include "EntryHeader.h"
#include "../../config/Consts.h"

class Bucket {

public:
    static const uint16_t NEWLY_CREATED_MASK = 0x1;
    static const uint16_t DIRTY_MASK = 0x2;

    struct Header {
        size_t bucket_id = 0;
        unsigned local_depth = 0;
        uint16_t status = 0;
        size_t data_begin = BUCKET_SIZE;
        size_t offset_end = 0;

    };
    int32_t ref_count;
    bool recently_used = false;
    bool contains_deleted_entries = false;
    pthread_rwlock_t rw_lock;

    Header header;

    Bucket() :
            header() {
        header.status = NEWLY_CREATED_MASK;
        data = new char[BUCKET_SIZE];
        pthread_rwlock_init(&rw_lock, nullptr);
    }


    void compact();

    /**
     * Splits this bucket into two buckets and inserts the new entry into the correct bucket.
     * The new bucket is expected to be initialized but empty.
     */
    void split(Bucket &new_bucket, size_t hash_to_insert, string key_to_insert, string value_to_insert);

    /**
     * Inserts key and value into this bucket. The caller has to make sure this is the right bucket
     * as the hash is not checked again and is only used to write the EntryPosition.
     * Should the bucket be full, the bucket will be compacted. If this is still not enough, the method
     * returns false and expects the caller to split the bucket.
     */
    bool put(size_t hash, const string &key, const string &value);

    /**
     * Searches for the given key with the given hash and writes its value to result.
     * Returns true if the key has been found, false otherwise.
     */
    bool get(size_t hash, const string &key, string &result);

    /**
     * Searches for the given key and deletes it and its value.
     */
    void del(size_t hash, const string &key);

    /**
     * A pointer to the raw data of the bucket
     */
    char *get_data();

    /**
     * For debugging.
     * Shows how full the current bucket is (in percent)
     */
    double get_usage();



private:

    char *data;

    /**
     * Finds the given key with the given hash and saves its EntryPosition and EntryHeader into the structs given
     * as argument.
     * Returns false if the key is not in the bucket. In this case, nothing is written to the pointers
     */
    bool find(size_t hash, const string &key, EntryPosition *&position, EntryHeader *&entry_header);

    /**
     * Inserts the given entry.
     * Expects the caller to check whether there is enough space to insert the entry.
     * If the caller fails to do so, data corruption will occur.
     */
    void insert(size_t hash, const string &key, const string &value);
};


#endif //CELONIS_KV_SERVER_BUFFER_H
