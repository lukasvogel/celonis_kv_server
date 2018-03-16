//
// Created by Lukas on 10.03.2018.
//

#ifndef CELONIS_KV_SERVER_BUFFERMANAGER_H
#define CELONIS_KV_SERVER_BUFFERMANAGER_H


#include <vector>
#include <unordered_map>
#include <mutex>
#include "bucket/Bucket.h"

class BucketManager {

public:

    BucketManager();
    ~BucketManager();

    Bucket& get(size_t bucket_id, bool exclusive);
    void release(Bucket &bucket);
    void flush_all();

private:

    void flush(Bucket &bucket);
    void load(size_t bucket_id, Bucket &bucket);
    unsigned evict();

    int page_file_fd;
    unsigned clock_hand = 0;
    unordered_map<size_t, unsigned> bucket_mapping;
    mutex mapping_mutex;
    Bucket buckets[BUCKETS_IN_MEM];
};


#endif //CELONIS_KV_SERVER_BUFFERMANAGER_H