//
// Created by Lukas on 09.03.2018.
//

#include <iostream>
#include "KVStore.h"

using namespace std;

void KVStore::put(const string key, const string value) {
    size_t h = hash<string>()(key);
    Bucket *b = get_bucket(h);

    if (b->put(h, key, value)) {
        return;
    } else {
        // insert failed, bucket is full, we have to split
        if (b->local_depth == global_depth) {
            global_depth += 1;

            //TODO duplicate num of buckets
        }

        if (b->local_depth < global_depth) {
            // split bucket into two
            //TODO: what's the number?
            Bucket b2 = bm.get(1337);

            b->split(global_depth, b2);

            bm.release(b2);

            //TODO put new buckets into index
        }

        //try insert again with new bucket
        bm.release(*b);
        put(key, value);
    }

    bm.release(*b);


}

bool KVStore::get(const string key, string *result) {
    size_t h = hash<string>()(key);
    Bucket b = *get_bucket(h);

    bool status = b.get(h, key, result);
    bm.release(b);
    return status;
}

void KVStore::del(string key) {
    size_t h = hash<string>()(key);

    Bucket b = *get_bucket(h);
    b.del(h, key);
    bm.release(b);
}

Bucket *KVStore::get_bucket(size_t hash) {
    size_t num = hash & ((1 << global_depth) - 1);
    return &bm.get(num);
}

KVStore::KVStore() {
    bm = BucketManager();
}

