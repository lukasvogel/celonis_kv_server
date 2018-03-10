//
// Created by Lukas on 10.03.2018.
//

#include "BucketManager.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <iostream>
#include <bits/stat.h>
#include <cstdio>
#include <fcntl.h>


BucketManager::BucketManager() {
    file = open("storage.dat", O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
}

Bucket &BucketManager::get(size_t bucket_id) {

    auto it = bucket_mapping.find(bucket_id);

    if (it == bucket_mapping.end()) {

        unsigned free_slot = evict();
        Bucket &bucket = buckets[free_slot];
        load(bucket);
        bucket.status = 0x0;
        bucket.bucket_id = bucket_id;
        bucket_mapping.insert(pair<size_t, unsigned>(bucket_id, free_slot));
        bucket.ref_count++;
        return buckets[free_slot];
    } else {
        unsigned pos = it->second;
        Bucket &bucket = buckets[pos];
        bucket.ref_count++;
        //TODO: lock
        return buckets[pos];
    }
}

void BucketManager::release(Bucket &bucket) {
    bucket.ref_count--;
    flush(bucket);
}

unsigned BucketManager::evict() {
    //first look if we still have unclaimed pages
    if (bucket_mapping.size() < BUCKETS_IN_MEM) {
        for (unsigned i = 0; i < BUCKETS_IN_MEM; i++) {
            Bucket cur_bucket = buckets[i];

            if (cur_bucket.status & Bucket::NEWLY_CREATED_MASK) {
                return i;
            }
        }
    }
    //TODO: better eviction strategy
    for (unsigned i = 0; i < BUCKETS_IN_MEM; i++) {
        Bucket cur_bucket = buckets[i];

        if (cur_bucket.ref_count == 0) {
            flush(cur_bucket);
            return i;
        }
    }
}


void BucketManager::flush(Bucket &bucket) {

    if (pwrite(file, bucket.get_data(), Bucket::SIZE, bucket.bucket_id * Bucket::SIZE) == -1) {
        std::cerr << "cannot write bucket data: " << strerror(errno)
                  << " Bucket: " << bucket.bucket_id
                  << std::endl;
    }
}

void BucketManager::load(Bucket &bucket) {
    struct stat statbuf;
    if (stat("storage.dat", &statbuf) == -1) {
        std::cerr << "cannot check input file stat " << strerror(errno)
                  << std::endl;
    }
    size_t file_size = statbuf.st_size;

    bool is_in_range = file_size >= (bucket.bucket_id + 1) * (Bucket::SIZE) - 1;

    if (is_in_range) {
        if (pread(file, bucket.get_data(), Bucket::SIZE, bucket.bucket_id * (Bucket::SIZE)) == -1) {
            std::cerr << "cannot read bucket data: " << strerror(errno)
                      << " Bucket: " << bucket.bucket_id
                      << " contents: " << bucket.get_data() << std::endl;
        }
    } else {
        memset(bucket.get_data(), 0, Bucket::SIZE);
    }
}

