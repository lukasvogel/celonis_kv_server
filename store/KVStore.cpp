//
// Created by Lukas on 09.03.2018.
//

#include <iostream>
#include "KVStore.h"

using namespace std;

void KVStore::put(const string key, const string value) {
    size_t h = hash<string>()(key);

    pthread_rwlock_wrlock(&hashtable_lock);
    Bucket &b = get_bucket(h, true);

    if (b.put(h, key, value)) {
        pthread_rwlock_unlock(&hashtable_lock);
        // Normal insert worked, we're done here!
        bm.release(b);
        return;
    } else {
        // insert failed, bucket is full, we have to split
        if (b.header.local_depth == global_depth) {

            // To increment global depth, we double the table and references
            unsigned long size = pages.size();
            for (int i = 0; i < size; i++) {
                pages.push_back(pages[i]);
            }
            global_depth++;
        }

        if (b.header.local_depth < global_depth) {

            Bucket &b2 = bm.get(++max_bucket_no, true);

            // Put new bucket into duplicated position in the index
            for (int i = 0; i < pages.size(); i++) {
                if (pages[i] == b.header.bucket_id) {
                    if (((i >> b.header.local_depth) & 1) == 1) {
                        pages[i] = b2.header.bucket_id;
                    }
                }
            }
            pthread_rwlock_unlock(&hashtable_lock);

            // split bucket into two
            b.split(global_depth, b2, h, key, value);

            bm.release(b2);
        } else {
            pthread_rwlock_unlock(&hashtable_lock);
        }
        bm.release(b);
    }
}

bool KVStore::get(const string key, string &result) {
    size_t h = hash<string>()(key);
    pthread_rwlock_rdlock(&hashtable_lock);
    Bucket &b = get_bucket(h, false);
    pthread_rwlock_unlock(&hashtable_lock);
    bool status = b.get(h, key, result);
    bm.release(b);
    return status;
}

void KVStore::del(string key) {
    size_t h = hash<string>()(key);
    pthread_rwlock_rdlock(&hashtable_lock);
    Bucket &b = get_bucket(h, true);
    pthread_rwlock_unlock(&hashtable_lock);
    b.del(h, key);
    bm.release(b);
}

Bucket &KVStore::get_bucket(size_t hash, bool exclusive) {
    size_t num = hash & ((1 << global_depth) - 1);

    return bm.get(pages[num], exclusive);
}

KVStore::KVStore() :
        bm() {
    pthread_rwlock_init(&hashtable_lock, nullptr);

    //start with the zero page
    pages.push_back(0);
}

void KVStore::print_table_layout() {
    cout << "| ";
    for (int i=0; i < pages.size(); i++) {
        cout << pages[i] << " | ";
    }
    cout << endl;
}

