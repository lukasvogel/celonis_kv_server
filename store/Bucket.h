//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_BUFFER_H
#define CELONIS_KV_SERVER_BUFFER_H


#include <cstdint>
#include <cstdlib>
#include "EntryPosition.h"
#include "EntryHeader.h"

class Bucket {

public:
    static const size_t SIZE = 1024 * 1024;

    explicit Bucket(size_t bucket_id) :
        data_begin(SIZE),
        offset_end(0),
        bucket_id(bucket_id) {
        data = new char[Bucket::SIZE];
    }

    Bucket() :
            data_begin(SIZE),
            offset_end(0),
            bucket_id(0) {
        data = new char[Bucket::SIZE];
    }


    void compact();

    void split(size_t global_depth, Bucket &new_bucket);

    bool put(size_t hash, string key, string value);

    bool get(size_t hash, string key, string *result);

    void del(size_t hash, string key);

    char* get_data();

    // for debugging
    double get_usage();

    size_t bucket_id;

    int local_depth = 0;

    unsigned ref_count = 0;

private:
    char *data{};
    size_t data_begin;
    size_t offset_end;

    bool find(size_t hash, string key, EntryPosition **position, EntryHeader **header);

    void insert(size_t hash, string key, string value);
};


#endif //CELONIS_KV_SERVER_BUFFER_H
