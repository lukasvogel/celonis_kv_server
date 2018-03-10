//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_BUFFER_H
#define CELONIS_KV_SERVER_BUFFER_H


#include <cstdint>
#include <cstdlib>
#include <stdint-gcc.h>
#include "EntryPosition.h"
#include "EntryHeader.h"

class Bucket {

public:
    static const size_t SIZE = 1024;
    static const uint16_t DIRTY_MASK = 0x1;
    static const uint16_t NEWLY_CREATED_MASK = 0x2;


    explicit Bucket(size_t bucket_id) :
        data_begin(SIZE),
        offset_end(0),
        bucket_id(),
        status(0){
        data = new char[Bucket::SIZE];
    }

    Bucket() :
            data_begin(SIZE),
            offset_end(0),
            bucket_id(0),
            status(NEWLY_CREATED_MASK){
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
    uint16_t status = 0;

private:
    size_t data_begin;
    size_t offset_end;
    char *data;

    bool find(size_t hash, string key, EntryPosition **position, EntryHeader **header);

    void insert(size_t hash, string key, string value);
};


#endif //CELONIS_KV_SERVER_BUFFER_H
