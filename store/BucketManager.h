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

    static const size_t BUCKETS_IN_MEM = 10;

    Bucket& get(size_t bucket_id);


    void release(Bucket &bucket);

private:

    void flush(Bucket &bucket);

    void load(Bucket &bucket);

    unsigned evict();

    int file;
    unordered_map<size_t, unsigned> bucket_mapping;
    Bucket buckets[BUCKETS_IN_MEM];

};


#endif //CELONIS_KV_SERVER_BUFFERMANAGER_H
