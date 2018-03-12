cmake_minimum_required(VERSION 3.5)
project(celonis_kv_server)

set(CMAKE_CXX_STANDARD 14)
SET(CMAKE_CXX_FLAGS -pthread)

add_subdirectory(googletest)
include_directories(googletest/googletest/include)

add_executable(run_tests
        test/tests.cpp store/KVStore.cpp store/KVStore.cpp store/KVStore.h store/bucket/EntryPosition.h store/bucket/Bucket.cpp store/bucket/Bucket.h store/bucket/EntryHeader.h store/BucketManager.cpp store/BucketManager.h)
target_link_libraries(run_tests gtest gtest_main)

add_executable(celonis_kv_server main.cpp Endpoint.cpp Endpoint.h store/KVStore.cpp store/KVStore.h store/bucket/EntryPosition.h store/bucket/Bucket.cpp store/bucket/Bucket.h store/bucket/EntryHeader.h store/BucketManager.cpp store/BucketManager.h config/Consts.h)
target_link_libraries(celonis_kv_server pistache)