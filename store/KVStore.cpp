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
        bm.release(*b);
        return;
    } else {
        // insert failed, bucket is full, we have to split

        if (b->header.local_depth == global_depth) {
            cout << "Incrementing global depth" << endl;

            // double the size of the table
            unsigned long size = pages.size();
            for (int i = 0; i < size; i++) {
                pages.push_back(pages[i]);
            }
            global_depth++;
        }

        if (b->header.local_depth < global_depth) {
            cout << "Incrementing local depth of: " << b->header.bucket_id << endl;
            cout << "Splitting because of: " << key << endl;

            // split bucket into two
            Bucket *b2 = &bm.get(++max_bucket_no);
            b->split(global_depth, *b2);
            bm.release(*b2);

            // Put new bucket into index
            for (int i = 0; i < pages.size(); i++) {
                if (pages[i] == b->header.bucket_id) {
                    if (((i >> b->header.local_depth-1) & 1) == 1) {
                        pages[i] = b2->header.bucket_id;
                    }
                }
            }
        }

        print_table_layout();

        bm.release(*b);
        put(key, value);
    }
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

    return &bm.get(pages[num]);
}

KVStore::KVStore() {
    bm = BucketManager();
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

