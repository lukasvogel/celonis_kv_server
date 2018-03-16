//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_BUFFER_H
#define CELONIS_KV_SERVER_BUFFER_H


#include <cstdint>
#include <cstdlib>
#include <stdint-gcc.h>
#include <atomic>
#include "EntryPosition.h"
#include "EntryHeader.h"
#include "../../config/Consts.h"

class Bucket {

public:
    static const uint16_t NEWLY_CREATED_MASK = 0x1;

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

    void split(size_t global_depth, Bucket &new_bucket, size_t hash_to_insert, string key_to_insert, string value_to_insert);

    bool put(size_t hash, const string &key, const string &value);

    bool get(size_t hash, const string &key, string &result);

    void del(size_t hash, const string &key);

    char *get_data();

    // for debugging
    double get_usage();



private:

    char *data;

    bool find(size_t hash, const string &key, EntryPosition *&position, EntryHeader *&entry_header);

    void insert(size_t hash, const string &key, const string &value);
};


#endif //CELONIS_KV_SERVER_BUFFER_H
