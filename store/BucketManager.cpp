//
// Created by Lukas on 10.03.2018.
//

#include "BucketManager.h"
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <fcntl.h>
#include <cstring>


BucketManager::BucketManager() {
    page_file_fd = open(PAGE_FILE, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
}

Bucket &BucketManager::get(size_t bucket_id, bool exclusive) {

    mapping_mutex.lock();
    auto it = bucket_mapping.find(bucket_id);

    if (it == bucket_mapping.end()) {

        unsigned free_slot = evict();
        bucket_mapping[bucket_id] = free_slot;
        Bucket &bucket = buckets[free_slot];

        bucket.ref_count++; //Allows us to unlock the mutex as it can't be evicted now. Will be overwritten later anyway
        mapping_mutex.unlock();

        load(bucket_id, bucket);

        if(exclusive)
            pthread_rwlock_wrlock(&bucket.rw_lock);
        else
            pthread_rwlock_rdlock(&bucket.rw_lock);

        bucket.recently_used = true;

        return bucket;
    } else {
        // found the bucket in memory!
        unsigned pos = it->second;
        Bucket &bucket = buckets[pos];
        bucket.ref_count++;
        bucket.recently_used = true;
        mapping_mutex.unlock();

        if(exclusive)
            pthread_rwlock_wrlock(&bucket.rw_lock);
        else
            pthread_rwlock_rdlock(&bucket.rw_lock);


        return bucket;
    }

}

void BucketManager::release(Bucket &bucket) {
    bucket.ref_count--;
    pthread_rwlock_unlock(&bucket.rw_lock);
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
    // clock algorithm
    while (true) {
        clock_hand = (clock_hand + 1) % BUCKETS_IN_MEM;

        if (buckets[clock_hand].recently_used) {
            buckets[clock_hand].recently_used = false;
        } else if (buckets[clock_hand].ref_count == 0) {
            bucket_mapping.erase(bucket_mapping.find(buckets[clock_hand].header.bucket_id));
            flush(buckets[clock_hand]);
            return clock_hand;
        }
    }
}


void BucketManager::flush(Bucket &bucket) {

    if ((bucket.header.status & Bucket::DIRTY_MASK) == 0) {
        // If the dirty mask is not set, we don't have to do anything
        return;
    }

    size_t bucket_size = BUCKET_SIZE + sizeof(Bucket::Header);
    size_t bucket_offset = (bucket.header.bucket_id * bucket_size);


    // write the header
    if (pwrite(page_file_fd, &bucket.header, sizeof(Bucket::Header), bucket_offset) == -1) {
        std::cerr << "cannot write bucket header: " << strerror(errno)
                  << " Bucket: " << bucket.header.bucket_id
                  << std::endl;
    }


    // write the data
    if (pwrite(page_file_fd, bucket.get_data(), BUCKET_SIZE, bucket_offset + sizeof(Bucket::Header)) == -1) {
        std::cerr << "cannot write bucket data: " << strerror(errno)
                  << " Bucket: " << bucket.header.bucket_id
                  << std::endl;
    }
}

void BucketManager::load(size_t bucket_id, Bucket &bucket) {
    struct stat statbuf;
    if (stat(PAGE_FILE, &statbuf) == -1) {
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
        if (pread(page_file_fd, &bucket.header, sizeof(Bucket::Header), bucket_offset) == -1) {
            std::cerr << "cannot read bucket header: " << strerror(errno)
                      << " Bucket: " << bucket.header.bucket_id << std::endl;
        }

        // read the data
        if (pread(page_file_fd, bucket.get_data(), BUCKET_SIZE, bucket_offset + sizeof(Bucket::Header)) == -1) {
            std::cerr << "cannot read bucket data: " << strerror(errno)
                      << " Bucket: " << bucket.header.bucket_id
                      << " contents: " << bucket.get_data() << std::endl;
        }

        bucket.ref_count = 1;
    } else {
        // This bucket has never been requested before, initialize it
        memset(bucket.get_data(), 0, BUCKET_SIZE);
        bucket.header.data_begin = BUCKET_SIZE;
        bucket.header.offset_end = 0;
        bucket.header.bucket_id = bucket_id;
        bucket.header.status = 0;
        bucket.ref_count = 1;
    }
}

BucketManager::~BucketManager() {
    flush_all();
    close(page_file_fd);
}

void BucketManager::flush_all() {
    for (auto &bucket : buckets) {
        if (!(bucket.header.status & Bucket::NEWLY_CREATED_MASK)) {
            flush(bucket);
        }
    }
}

