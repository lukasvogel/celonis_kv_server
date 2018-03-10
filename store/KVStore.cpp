//
// Created by Lukas on 09.03.2018.
//

#include <iostream>
#include "KVStore.h"

using namespace std;

void KVStore::put(const string key, const string value) {
    cout << "putting: " << value << " at key: " << key << endl;
    entries.put(key,value);
}

string KVStore::get(const string key) {
    string result = entries.get(key);
    //TODO: handle not found case
    if (result != "\0") {
        return result;
    } else {
        cout << "didn't find key: " << key << endl;
        return nullptr;
    }
}

void KVStore::del(string key) {
    entries.del(key);
}
