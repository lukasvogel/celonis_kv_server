//
// Created by Lukas on 09.03.2018.
//

#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include "KVStore.h"

using namespace std;

void KVStore::put(const string &key, const string &value) {
    size_t h = hash<string>()(key);

    pthread_rwlock_wrlock(&hashtable_lock);
    Bucket &b = get_bucket(h, true);

    if (b.put(h, key, value)) {
        // Normal insert worked, we're done here!
        bm.release(b);
        pthread_rwlock_unlock(&hashtable_lock);
        return;
    } else {
        // insert failed, bucket is full, we have to split
        if (b.header.local_depth == global_depth) {

            // To increment global depth, we double the table and references
            unsigned long size = buckets.size();
            for (int i = 0; i < size; i++) {
                buckets.push_back(buckets[i]);
            }
            global_depth++;
        }

        if (b.header.local_depth < global_depth) {

            Bucket &b2 = bm.get(++max_bucket_no, true);

            // Put new bucket into duplicated position in the index
            for (int i = 0; i < buckets.size(); i++) {
                if (buckets[i] == b.header.bucket_id) {
                    if (((i >> b.header.local_depth) & 1) == 1) {
                        buckets[i] = b2.header.bucket_id;
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

bool KVStore::get(const string &key, string &result) {
    size_t h = hash<string>()(key);
    pthread_rwlock_rdlock(&hashtable_lock);
    Bucket &b = get_bucket(h, false);
    pthread_rwlock_unlock(&hashtable_lock);
    bool status = b.get(h, key, result);
    bm.release(b);
    return status;
}

void KVStore::del(const string &key) {
    size_t h = hash<string>()(key);
    pthread_rwlock_rdlock(&hashtable_lock);
    Bucket &b = get_bucket(h, true);
    pthread_rwlock_unlock(&hashtable_lock);
    b.del(h, key);
    bm.release(b);
}

Bucket &KVStore::get_bucket(size_t hash, bool exclusive) {
    size_t num = hash & ((1 << global_depth) - 1);

    return bm.get(buckets[num], exclusive);
}

KVStore::KVStore() :
        bm() {
    pthread_rwlock_init(&hashtable_lock, nullptr);

    // try to restore the state from an index file if possible
    index_file_fd = open(INDEX_FILE, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

    if (pread(index_file_fd, &global_depth, sizeof(size_t), 0) == -1) {
        std::cerr << "cannot read global depth: " << strerror(errno) << ", using default: 0" << std::endl;
    }
    if (pread(index_file_fd, &max_bucket_no, sizeof(size_t), sizeof(size_t)) == -1) {
        std::cerr << "cannot read max bucket no: " << strerror(errno) << ", using default: 0" << std::endl;
    }

    size_t index_size = 0;
    if (pread(index_file_fd, &index_size, sizeof(size_t), 2 * sizeof(size_t)) == -1) {
        std::cerr << "cannot read index size: " << strerror(errno) << ", using default: 1" << std::endl;
    }

    // if we do not have an index, create a new one
    if (index_size == 0) {
        buckets.push_back(0);
        return;
    }

    // if we have an index, read it
    size_t offset = 3 * sizeof(size_t);
    for (size_t cur_offset = offset; cur_offset < offset + sizeof(size_t) * index_size; cur_offset+= sizeof(size_t)) {

        size_t page_no;

        pread(index_file_fd, &page_no, sizeof(size_t), offset);
        buckets.push_back(page_no);
    }


}

void KVStore::print_table_layout() {
    cout << "| ";
    for (int i = 0; i < buckets.size(); i++) {
        cout << buckets[i] << " | ";
    }
    cout << endl;
}

void KVStore::flush() {

    // Flush all pages in memory
    bm.flush_all();

    // HEADER: | GLOBAL_DEPTH | MAX_BUCKET_NO | INDEX_SIZE |
    // CONTENT: | INDEX .... |

    // serialize global depth and max bucket number
    size_t size = buckets.size();

    if (pwrite(index_file_fd, &global_depth, sizeof(size_t), 0) == -1) {
        std::cerr << "cannot write global depth: " << strerror(errno) << std::endl;
    }

    if (pwrite(index_file_fd, &max_bucket_no, sizeof(size_t), sizeof(size_t)) == -1) {
        std::cerr << "cannot write max bucket no: " << strerror(errno) << std::endl;
    }

    if (pwrite(index_file_fd, &size, sizeof(size_t), 2 * sizeof(size_t)) == -1) {
        std::cerr << "cannot write index header: " << strerror(errno) << std::endl;
    }

    // The index never shrinks so we can just overwrite the contents if there are any
    if (pwrite(index_file_fd, &buckets[0], buckets.size() * sizeof(size_t), 3 * sizeof(size_t)) == -1) {
        std::cerr << "cannot write index: " << strerror(errno) << std::endl;
    }

}

KVStore::~KVStore() {
    flush();
    close(index_file_fd);

}


