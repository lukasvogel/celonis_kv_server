//
// Created by Lukas on 10.03.2018.
//
#include "../store/KVStore.h"
#include <climits>
#include "gtest/gtest.h"


TEST(SimpleOperations, Insert) {
    KVStore store;

    store.put("key", "value");
    string value = store.get("key");
    EXPECT_EQ(value, "value");
}
