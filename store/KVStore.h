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

    static const int PAGES_IN_MEM = 10;


    void put(string key, string value);

    bool get(string key, string *result);

    void del(string key);

    Buffer entries[PAGES_IN_MEM];


private:

    int global_depth = 0;

    Buffer* get_page(string key);


};


#endif //CELONIS_KV_SERVER_KVSTORE_H
