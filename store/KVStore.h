//
// Created by Lukas on 09.03.2018.
//

#ifndef CELONIS_KV_SERVER_KVSTORE_H
#define CELONIS_KV_SERVER_KVSTORE_H

#include <string>
#include <unordered_map>
#include "EntryPosition.h"
#include "Buffer.h"

using namespace std;



class KVStore {

public:


    void put(string key, string value);

    string get(string key);

    void del(string key);


private:

    Buffer entries;
};


#endif //CELONIS_KV_SERVER_KVSTORE_H
