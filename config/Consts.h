//
// Created by Lukas on 11.03.2018.
//

#include <cstddef>

#ifndef CELONIS_KV_SERVER_CONSTS_H
#define CELONIS_KV_SERVER_CONSTS_H




static const size_t BUCKET_SIZE = 4 * 1024; // 4 kb
static const size_t MAX_MEMORY = 10 * 1024 * 1024; // 1 mb;
static constexpr size_t BUCKETS_IN_MEM = MAX_MEMORY / BUCKET_SIZE;

static const char* PAGE_FILE = "storage.dat";
static const char* INDEX_FILE = "index.dat";

#endif //CELONIS_KV_SERVER_CONSTS_H
