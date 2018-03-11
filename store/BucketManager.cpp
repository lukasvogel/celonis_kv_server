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
#include <cassert>


BucketManager::BucketManager() {
    file = open("storage.dat", O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
}

Bucket &BucketManager::get(size_t bucket_id) {

    auto it = bucket_mapping.find(bucket_id);

    if (it == bucket_mapping.end()) {

        unsigned free_slot = evict();
        load(bucket_id, buckets[free_slot]);
        bucket_mapping[bucket_id] = free_slot;
        buckets[free_slot].header.ref_count++;
        return buckets[free_slot];
    } else {
        unsigned pos = it->second;
        Bucket &bucket = buckets[pos];
        bucket.header.ref_count++;
        //TODO: lock
        return buckets[pos];
    }

}

void BucketManager::release(Bucket &bucket) {
    bucket.header.ref_count--;
}

unsigned BucketManager::evict() {
    //first look if we still have unclaimed space in memory
    if (bucket_mapping.size() < BUCKETS_IN_MEM) {

        //no need to evict, just grab the first free page
        for (unsigned i = 0; i < BUCKETS_IN_MEM; i++) {
            Bucket cur_bucket = buckets[i];

            if (cur_bucket.header.status & Bucket::NEWLY_CREATED_MASK) {
                return i;
            }
        }
    }
    //TODO: better eviction strategy
    for (unsigned i = 0; i < BUCKETS_IN_MEM; i++) {
        Bucket cur_bucket = buckets[i];

        if (cur_bucket.header.ref_count == 0) {
            bucket_mapping.erase(bucket_mapping.find(cur_bucket.header.bucket_id));
            flush(cur_bucket);
            return i;
        }
    }
    // all buckets claimed
    assert(false);
}


void BucketManager::flush(Bucket &bucket) {

    size_t bucket_size = BUCKET_SIZE + sizeof(Bucket::Header);
    size_t bucket_offset = (bucket.header.bucket_id * bucket_size);


    // write the header
    if (pwrite(file, &bucket.header, sizeof(Bucket::Header), bucket_offset) == -1) {
        std::cerr << "cannot write bucket header: " << strerror(errno)
                  << " Bucket: " << bucket.header.bucket_id
                  << std::endl;
    }


    // write the data
    if (pwrite(file, bucket.get_data(), BUCKET_SIZE, bucket_offset + sizeof(Bucket::Header)) == -1) {
        std::cerr << "cannot write bucket data: " << strerror(errno)
                  << " Bucket: " << bucket.header.bucket_id
                  << std::endl;
    }
}

void BucketManager::load(size_t bucket_id, Bucket &bucket) {
    struct stat statbuf;
    if (stat("storage.dat", &statbuf) == -1) {
        std::cerr << "cannot check input file stat " << strerror(errno)
                  << std::endl;
    }
    size_t file_size = statbuf.st_size;
    size_t bucket_size = BUCKET_SIZE + sizeof(Bucket::Header);
    size_t bucket_offset = (bucket_id * bucket_size);

    bool is_in_range = file_size >= (bucket_offset + bucket_size - 1);

    if (is_in_range) {
        // We already have this bucket saved in the file, load it

        // read the header
        if (pread(file, &bucket.header, sizeof(Bucket::Header), bucket_offset) == -1) {
            std::cerr << "cannot read bucket header: " << strerror(errno)
                      << " Bucket: " << bucket.header.bucket_id << std::endl;
        }

        // read the data
        if (pread(file, bucket.get_data(), BUCKET_SIZE, bucket_offset + sizeof(Bucket::Header)) == -1) {
            std::cerr << "cannot read bucket data: " << strerror(errno)
                      << " Bucket: " << bucket.header.bucket_id
                      << " contents: " << bucket.get_data() << std::endl;
        }
    } else {
        // This bucket has never been requested before, create it
        memset(bucket.get_data(), 0, BUCKET_SIZE);
        bucket.header.data_begin = BUCKET_SIZE;
        bucket.header.offset_end = 0;
        bucket.header.bucket_id = bucket_id;
        bucket.header.status = 0;
        bucket.header.ref_count = 0;
    }
}

