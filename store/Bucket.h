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
#include "../Consts.h"

class Bucket {

public:
    static const uint16_t DIRTY_MASK = 0x1;
    static const uint16_t NEWLY_CREATED_MASK = 0x2;


    struct Header {
        size_t bucket_id = 0;
        unsigned local_depth = 0;
        unsigned ref_count = 0;
        uint16_t status = 0;
        size_t data_begin = BUCKET_SIZE;
        size_t offset_end = 0;

    };

    Header header;

    explicit Bucket(size_t bucket_id) :
            header() {
        header.bucket_id = bucket_id;
        data = new char[BUCKET_SIZE];
    }

    Bucket() :
            header() {
        header.status = NEWLY_CREATED_MASK;
        data = new char[BUCKET_SIZE];
    }


    void compact();

    void split(size_t global_depth, Bucket &new_bucket);

    bool put(size_t hash, string key, string value);

    bool get(size_t hash, string key, string *result);

    void del(size_t hash, string key);

    char *get_data();

    // for debugging
    double get_usage();


private:

    char *data;

    bool find(size_t hash, string key, EntryPosition **position, EntryHeader **entry_header);

    void insert(size_t hash, string key, string value);
};


#endif //CELONIS_KV_SERVER_BUFFER_H
