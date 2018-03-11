//
// Created by Lukas on 10.03.2018.
//
#include "../store/KVStore.h"
#include <climits>
#include <thread>
#include "gtest/gtest.h"


TEST(SimpleOperations, Insert) {
    KVStore store;
    // inserting and retrieving a key
    store.put("key", "value");
    string value;
    store.get("key", value);
    EXPECT_EQ(value, "value");

    // variation with special characters
    store.put("KEY WITH SPACE ", "VALUE WITH SPACE");
    store.get("KEY WITH SPACE ", value);
    EXPECT_EQ(value, "VALUE WITH SPACE");

}

TEST(SimpleOperations, Delete) {
    KVStore store;
    store.put("key", "value");
    string value;
    store.get("key", value);
    EXPECT_EQ(value, "value");

    store.del("key");

    EXPECT_EQ(store.get("key", value), false);
}

TEST(SimpleOperations, Update) {
    KVStore store;
    store.put("key", "value");
    store.put("key", "value2");
    string value;
    store.get("key", value);
    EXPECT_EQ(value, "value2");

    store.put("key", "value");
    store.get("key", value);
    EXPECT_EQ(value, "value");
}

TEST(SimpleOperations, DeleteThenReinsert) {
    KVStore store;
    store.put("key", "value");
    store.del("key");

    store.put("key", "value");
    string value;
    store.get("key", value);
    EXPECT_EQ(value, "value");

    store.del("key");
    store.put("key", "value2");
    store.get("key", value);
    EXPECT_EQ(value, "value2");
}

TEST(Packing, FillingBufferPromptsPacking) {


    KVStore store;

    size_t BUFFER_SIZE = BUCKET_SIZE;
    size_t ENTRY_SIZE = 2 * (sizeof(char) * 6) + sizeof(EntryPosition) + sizeof(EntryHeader);

    std::cout << "Entries: " << BUFFER_SIZE / ENTRY_SIZE << endl;

    for (int i = 10000; i < 10000 + BUFFER_SIZE / ENTRY_SIZE; i++) {
        store.put(to_string(i), to_string(i));
    }

    string value;
    for (int i = 10000; i < 10000 + BUFFER_SIZE / ENTRY_SIZE; i++) {
        store.get(to_string(i), value);
        EXPECT_EQ(value, to_string(i));
    }


    // delete the first five elements to make space for compaction
    for (int i = 10000; i < 10005; i++) {
        store.del(to_string(i));
        EXPECT_FALSE(store.get(to_string(i), value));
    }

    // add another five elements - should trigger compaction but fit
    for (int i = 10000; i < 10005; i++) {
        store.put(to_string(i), to_string(i));
    }

    for (int i = 10000; i < 10005; i++) {
        value = "0";
        store.get(to_string(i), value);
        EXPECT_EQ(value, to_string(i));
    }


}


TEST(HashTable, FillingTablePromptsExtension) {
    KVStore store;

    size_t BUFFER_SIZE = BUCKET_SIZE;
    size_t ENTRY_SIZE = 2 * (sizeof(char) * 6) + sizeof(EntryPosition) + sizeof(EntryHeader);

    size_t NUM_ENTRIES = (BUFFER_SIZE / ENTRY_SIZE) * 2; //fills two pages
    int start_val = 10000;
    std::cout << "Entries: " << NUM_ENTRIES << endl;

    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        store.put(to_string(i), to_string(i));
    }

    string value;
    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        EXPECT_TRUE(store.get(to_string(i), value));
        EXPECT_EQ(value, to_string(i));
    }
}

TEST(HashTable, LotsOfExtensions) {
    KVStore store;

    size_t NUM_ENTRIES = 100000;
    int start_val = 100000;
    std::cout << "Entries: " << NUM_ENTRIES << endl;

    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        store.put(to_string(i), to_string(i));
    }

    string value;
    cout << "----DONE INSERTING, READING----" << endl;
    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        value = "";
        EXPECT_TRUE(store.get(to_string(i), value));
        EXPECT_EQ(value, to_string(i));
    }
}

TEST(HashTable, LotsOfExtensionsWithDelete) {
    KVStore store;

    size_t NUM_ENTRIES = 200000;
    int start_val = 100000;
    std::cout << "Entries: " << NUM_ENTRIES << endl;

    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        store.put(to_string(i), to_string(i));
    }
    cout << "----DONE INSERTING, DELETING----" << endl;
    for (int i = start_val; i < start_val + NUM_ENTRIES; i += 2) {
        store.del(to_string(i));
    }

    string value;
    cout << "----DONE DELETING, READING----" << endl;
    for (int i = start_val; i < start_val + NUM_ENTRIES; i++) {
        if (i % 2 == 0) {
            EXPECT_FALSE(store.get(to_string(i), value));
        } else {
            EXPECT_TRUE(store.get(to_string(i), value));
            EXPECT_EQ(value, to_string(i));
        }
    }
}


void *read_the_key(void *store_ptr) {
    KVStore *store = (KVStore*)store_ptr;
    string value;

    for (int i = 0; i < 100000; i++) {
        store->get("the key",value);
        EXPECT_EQ("the value", value);
    }
}

TEST(Multithreading, MultipleThreadsReading) {

    int NUM_THREADS = 100;

    KVStore store;

    store.put("the key", "the value");

    pthread_t threads[NUM_THREADS];


    pthread_attr_t pattr;
    pthread_attr_init(&pattr);

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&threads[i], &pattr, read_the_key, &store);
    }

    void *ret;
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], &ret);
    }
    string value;
    store.get("the key",value);
    EXPECT_EQ("the value", value);
}

void *write_stuff(void *store_ptr) {
    KVStore *store = (KVStore*)store_ptr;
    string value;

    for (int i = 0; i < 100000; i++) {
        store->put("some rubbish key", "some rubbish key");
    }
}


TEST(Multithreading, MultipleReadingMultipleWriting) {

    int NUM_WRITERS = 100;
    int NUM_READERS = 100;

    KVStore store;

    store.put("the key", "the value");

    pthread_t writers[NUM_WRITERS];
    pthread_t readers[NUM_READERS];


    pthread_attr_t pattr;
    pthread_attr_init(&pattr);

    for (int i = 0; i < NUM_WRITERS; i++) {
        pthread_create(&writers[i], &pattr, write_stuff, &store);
    }

    for (int i = 0; i < NUM_READERS; i++) {
        pthread_create(&readers[i], &pattr, read_the_key, &store);
    }


    void *ret;
    for (int i = 0; i < NUM_WRITERS; i++) {
        pthread_join(writers[i], &ret);
    }

    for (int i = 0; i < NUM_READERS; i++) {
        pthread_join(readers[i], &ret);
    }

    string value;
    store.get("the key",value);
    EXPECT_EQ("the value", value);

}

void *write_sequence(KVStore &store, int start, int num_elems ) {

    for (int i = start; i < start + num_elems; i++) {
        store.put(to_string(i), to_string(i));
    }
}

TEST(Multithreading, MultipleWritingSequence) {

    int NUM_WRITERS = 100;

    int WRITES_PER_THREAD = 10000;


    KVStore store;

    thread writers[NUM_WRITERS];


    for (int i = 0; i < NUM_WRITERS; i++) {
        writers[i] = std::thread (write_sequence, ref(store), i * WRITES_PER_THREAD, WRITES_PER_THREAD);
    }


    for (int i = 0; i < NUM_WRITERS; i++) {
        writers[i].join();
    }

    for (int i = 0; i < NUM_WRITERS * WRITES_PER_THREAD; i++) {
        string value;
        EXPECT_TRUE(store.get(to_string(i),value));
        EXPECT_EQ(to_string(i),value);

    }
}




int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}