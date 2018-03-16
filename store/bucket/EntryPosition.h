//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_ENTRY_H
#define CELONIS_KV_SERVER_ENTRY_H


#include <cstdint>

using namespace std;

class EntryPosition {

public:

    enum class Status { ACTIVE, DELETED };

    size_t offset;
    size_t hash_code;
    Status status;

    EntryPosition() :
            offset(0),
            hash_code(0),
            status(Status::DELETED) {

    }

    EntryPosition(size_t offset, size_t hash_code) :
        offset(offset),
        hash_code(hash_code),
        status(Status::ACTIVE){
    }


    ~EntryPosition() = default;

};

#endif //CELONIS_KV_SERVER_ENTRY_H