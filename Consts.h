//
// Created by Lukas on 11.03.2018.
//

#include <cstddef>

#ifndef CELONIS_KV_SERVER_CONSTS_H
#define CELONIS_KV_SERVER_CONSTS_H

const size_t BUCKET_SIZE = 4 * 1024; // 4 kb
const size_t MAX_MEMORY = 100 * 1024 * 1024; // 100 mb;
constexpr size_t BUCKETS_IN_MEM = MAX_MEMORY / BUCKET_SIZE;

#endif //CELONIS_KV_SERVER_CONSTS_H
