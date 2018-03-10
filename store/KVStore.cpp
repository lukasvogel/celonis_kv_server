//
// Created by Lukas on 09.03.2018.
//

#include <iostream>
#include "KVStore.h"

using namespace std;

void KVStore::put(const string key, const string value) {
    //cout << "putting: " << value << " at key: " << key << endl;
    size_t h = hash<string>()(key);
    entries[0].put(h,key,value);
}

bool KVStore::get(const string key, string *result) {
    size_t h = hash<string>()(key);
    return entries[0].get(h,key, result);
}

void KVStore::del(string key) {
    size_t h = hash<string>()(key);
    entries[0].del(h,key);
}

Buffer *KVStore::get_page(string key) {
    size_t h = hash<string>()(key);
    return nullptr;
}
