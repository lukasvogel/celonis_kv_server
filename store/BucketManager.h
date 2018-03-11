//
// Created by Lukas on 10.03.2018.
//

#ifndef CELONIS_KV_SERVER_BUFFERMANAGER_H
#define CELONIS_KV_SERVER_BUFFERMANAGER_H


#include <cstring>
#include <vector>
#include <unordered_map>
#include "Bucket.h"

class BucketManager {

public:


    BucketManager();
    Bucket& get(size_t bucket_id);
    void release(Bucket &bucket);

private:

    void flush(Bucket &bucket);
    void load(size_t bucket_id, Bucket &bucket);
    unsigned evict();

    int file;
    unsigned clock_hand = 0;
    unordered_map<size_t, unsigned> bucket_mapping;
    Bucket buckets[BUCKETS_IN_MEM];

};


#endif //CELONIS_KV_SERVER_BUFFERMANAGER_H
