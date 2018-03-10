//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_ENTRYHEADER_H
#define CELONIS_KV_SERVER_ENTRYHEADER_H


#include <cstring>

class EntryHeader {

public:
    size_t key_size;
    size_t value_size;

    EntryHeader() :
            key_size(0),
            value_size(0) {

    }

    EntryHeader(size_t key_size, size_t value_size) :
            key_size(key_size),
            value_size(value_size) {
    }

};


#endif //CELONIS_KV_SERVER_ENTRYHEADER_H
