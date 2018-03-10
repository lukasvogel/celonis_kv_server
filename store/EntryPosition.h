//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_ENTRY_H
#define CELONIS_KV_SERVER_ENTRY_H


#include <cstdint>
#include <string>
#include <cstring>
#include <iostream>

using namespace std;

class EntryPosition {

public:

    enum class Status { ACTIVE, DELETED };

    size_t offset;
    Status status;

    EntryPosition() :
            offset(0),
            status(Status::DELETED) {

    }

    EntryPosition(size_t offset) :
        offset(offset),
        status(Status::ACTIVE){
    }


    ~EntryPosition() = default;

};

#endif //CELONIS_KV_SERVER_ENTRY_H