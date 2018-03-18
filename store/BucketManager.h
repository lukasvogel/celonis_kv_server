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

    /**
     * Returns a reference to the bucket with the given id, either locked exclusively or just with a read lock.
     * If the bucket is not in memory, it is loaded from disk. If the bucket is not on disk either, it is newly
     * created and initialized.
     */
    Bucket& get(size_t bucket_id, bool exclusive);

    /**
     * Releases the given bucket by removing the existing lock.
     * The caller is expected to not use this bucket anymore after calling release() as this bucket might be flushed
     * to disk and therefore might contain invalid data.
     */
    void release(Bucket &bucket);

    /**
     * Flushes all bucekts in memory to disk.
     * WARNING: this method is not thread-safe, it is only meant to be called during destruction of the server after
     * no requests can be made anymore
     */
    void flush_all();

private:

    /**
     * Flushes a single bucket to disk, if its dirty bit is set.
     */
    void flush(Bucket &bucket);

    /**
     * Loads a single bucket from disk, if it exists.
     * Otherwise creates a new bucket with the given id
     */
    void load(size_t bucket_id, Bucket &bucket);

    /**
     * Finds a bucket in memory that can be evicted and writes it to disk
     * Returns the index of the now evicted bucket in memory.
     * Expects the mapping mutex to be locked when called.
     */
    unsigned evict();

    int page_file_fd;
    unsigned clock_hand = 0;
    unordered_map<size_t, unsigned> bucket_mapping;
    mutex mapping_mutex;
    Bucket buckets[BUCKETS_IN_MEM];
};


#endif //CELONIS_KV_SERVER_BUFFERMANAGER_H