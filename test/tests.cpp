//
// Created by Lukas on 10.03.2018.
//
#include "../store/KVStore.h"
#include <climits>
#include "gtest/gtest.h"


TEST(SimpleOperations, Insert) {
    KVStore store;
    // inserting and retrieving a key
    store.put("key", "value");
    string value;
    store.get("key", &value);
    EXPECT_EQ(value, "value");

    // variation with special characters
    store.put("KEY WITH SPACE ", "VALUE WITH SPACE");
    store.get("KEY WITH SPACE ", &value);
    EXPECT_EQ(value, "VALUE WITH SPACE");

}

TEST(SimpleOperations, Delete) {
    KVStore store;
    store.put("key", "value");
    string value;
    store.get("key", &value);
    EXPECT_EQ(value, "value");

    store.del("key");

    EXPECT_EQ(store.get("key", &value), false);
}

TEST(SimpleOperations, Update) {
    KVStore store;
    store.put("key", "value");
    store.put("key", "value2");
    string value;
    store.get("key", &value);
    EXPECT_EQ(value, "value2");

    store.put("key", "value");
    store.get("key", &value);
    EXPECT_EQ(value, "value");
}

TEST(SimpleOperations, DeleteThenReinsert) {
    KVStore store;
    store.put("key", "value");
    store.del("key");

    store.put("key", "value");
    string value;
    store.get("key", &value);
    EXPECT_EQ(value, "value");

    store.del("key");
    store.put("key", "value2");
    store.get("key", &value);
    EXPECT_EQ(value, "value2");
}

TEST(Packing, FillingBufferPromptsPacking) {


    KVStore store;

    size_t BUFFER_SIZE = Bucket::SIZE;
    size_t ENTRY_SIZE = 2 * (sizeof(char) * 6) + sizeof(EntryPosition) + sizeof(EntryHeader);

    std::cout << "Entries: " << BUFFER_SIZE / ENTRY_SIZE << endl;

    for (int i = 10000; i < 10000 + BUFFER_SIZE / ENTRY_SIZE; i++) {
        store.put(to_string(i), to_string(i));
    }

    string value;
    for (int i = 10000; i < 10000 + BUFFER_SIZE / ENTRY_SIZE; i++) {
        store.get(to_string(i), &value);
        EXPECT_EQ(value, to_string(i));
    }


    // delete the first five elements to make space for compaction
    for (int i = 10000; i < 10005; i++) {
        store.del(to_string(i));
        EXPECT_FALSE(store.get(to_string(i), &value));
    }

    // add another five elements - should trigger compaction but fit
    for (int i = 10000; i < 10005; i++) {
        store.put(to_string(i), to_string(i));
    }

    for (int i = 10000; i < 10005; i++) {
        value = "0";
        store.get(to_string(i), &value);
        EXPECT_EQ(value, to_string(i));
    }


}


TEST(HashTable, FillingTablePromptsExtension) {
    KVStore store;

    size_t BUFFER_SIZE = Bucket::SIZE;
    size_t ENTRY_SIZE = 2 * (sizeof(char) * 6) + sizeof(EntryPosition) + sizeof(EntryHeader);

    size_t NUM_ENTRIES = (BUFFER_SIZE / ENTRY_SIZE) * 2; //fills two pages
    int start_val = 10000;
    std::cout << "Entries: " << NUM_ENTRIES << endl;

    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        store.put(to_string(i), to_string(i));
    }

    string value;
    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        EXPECT_TRUE(store.get(to_string(i), &value));
        EXPECT_EQ(value, to_string(i));
    }
}

TEST(HashTable, LotsOfExtensions) {
    KVStore store;

    size_t BUFFER_SIZE = Bucket::SIZE;
    size_t ENTRY_SIZE = 2 * (sizeof(char) * 6) + sizeof(EntryPosition) + sizeof(EntryHeader);

    size_t NUM_ENTRIES = (BUFFER_SIZE / ENTRY_SIZE) * 4; //fills two pages
    int start_val = 10000;
    std::cout << "Entries: " << NUM_ENTRIES << endl;

    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        store.put(to_string(i), to_string(i));
    }

    string value;
    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        EXPECT_TRUE(store.get(to_string(i), &value));
        EXPECT_EQ(value, to_string(i));
    }
}


