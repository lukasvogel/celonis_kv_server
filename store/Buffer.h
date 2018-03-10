//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_BUFFER_H
#define CELONIS_KV_SERVER_BUFFER_H


#include <cstdint>
#include <cstdlib>
#include "EntryPosition.h"
#include "EntryHeader.h"

class Buffer {

public:
    static const size_t SIZE = u n1024 * 1024;

    Buffer() {
        data = static_cast<char *>(malloc(SIZE));
        data_begin = SIZE;
        offset_end = 0;
    }

    void compact();

    void put(size_t hash, string key, string value);

    bool get(size_t hash, string key, string *result);

    void del(size_t hash, string key);

    // for debugging
    double get_usage();

private:
    char *data;
    size_t data_begin;
    size_t offset_end;

    bool find(size_t hash, string key, EntryPosition **position, EntryHeader **header);

    void insert(size_t hash, string key, string value);
};


#endif //CELONIS_KV_SERVER_BUFFER_H
