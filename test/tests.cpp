//
// Created by Lukas on 10.03.2018.
//
#include "../store/KVStore.h"
#include <climits>
#include <thread>
#include "gtest/gtest.h"
#include <chrono>
typedef std::chrono::high_resolution_clock Clock;

void cleanup() {
    remove(PAGE_FILE);
    remove(INDEX_FILE);
}

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
    cleanup();
}

TEST(SimpleOperations, Delete) {

    KVStore store;
    store.put("key", "value");
    string value;
    store.get("key", value);
    EXPECT_EQ(value, "value");

    store.del("key");

    EXPECT_EQ(store.get("key", value), false);
    cleanup();
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
    cleanup();
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
    cleanup();
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
        value = "";
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
    cleanup();
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
    cleanup();
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
    cleanup();
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
    cleanup();
}


void *read_the_key(void *store_ptr) {
    KVStore *store = (KVStore *) store_ptr;
    string value;

    for (int i = 0; i < 100000; i++) {
        store->get("the key", value);
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
    store.get("the key", value);
    EXPECT_EQ("the value", value);
    cleanup();
}

void *write_stuff(void *store_ptr) {
    KVStore *store = (KVStore *) store_ptr;
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
    store.get("the key", value);
    EXPECT_EQ("the value", value);
    cleanup();
}

void *write_sequence(KVStore &store, int start, int num_elems ) {

    for (int i = start; i < start + num_elems; i++) {
        store.put(to_string(i), to_string(i));
    }
}

void *del_sequence(KVStore &store, int start, int num_elems, int step) {
    for (int i = start; i < start + step * num_elems; i += step) {
        store.del(to_string(i));
    }
}

void *get_sequence(KVStore &store, int start, int num_elems) {
    string result;
    for (int i = start; i < start + num_elems; i++) {
        store.get(to_string(i),result);
    }
}


TEST(Multithreading, MultipleWritingSequence) {

    int NUM_WRITERS = 100;

    int WRITES_PER_THREAD = 1000;
    KVStore store;

    thread writers[NUM_WRITERS];

    for (int i = 0; i < NUM_WRITERS; i++) {
        writers[i] = std::thread(write_sequence, ref(store), i * WRITES_PER_THREAD, WRITES_PER_THREAD);
    }
    for (int i = 0; i < NUM_WRITERS; i++) {
        writers[i].join();
    }
    for (int i = 0; i < NUM_WRITERS * WRITES_PER_THREAD; i++) {
        string value;
        EXPECT_TRUE(store.get(to_string(i), value));
        EXPECT_EQ(to_string(i), value);
    }
    cleanup();
}


TEST(Multithreading, PerformanceTest) {
    KVStore store;

    int NUM_THREADS = 1;
    int NUM_ENTRIES = 200000;
    int WRITES_PER_THREAD = NUM_ENTRIES / NUM_THREADS;

    thread threads[NUM_THREADS];

    std::cout << "Entries: " << NUM_ENTRIES << endl;

    auto t1 = Clock::now();
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i] = std::thread(write_sequence, ref(store), i * WRITES_PER_THREAD, WRITES_PER_THREAD);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }
    auto t2 = Clock::now();
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
    std::cout << "Inserted " << NUM_ENTRIES << " entries in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
              << " milliseconds -> " << (NUM_ENTRIES / milliseconds) * 1000 << " entries per second." << std::endl;



    cout << "----DONE INSERTING, DELETING----" << endl;
    t1 = Clock::now();
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i] = std::thread(del_sequence, ref(store), i * WRITES_PER_THREAD, WRITES_PER_THREAD/2, 2);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }
    t2 = Clock::now();
    milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
    std::cout << "Deleted " << NUM_ENTRIES/2 << " entries in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
              << " milliseconds -> " << (NUM_ENTRIES / milliseconds) * 1000 << " entries per second." << std::endl;

    string value;
    cout << "----DONE DELETING, READING----" << endl;
    t1 = Clock::now();
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i] = std::thread(get_sequence, ref(store), i * WRITES_PER_THREAD, WRITES_PER_THREAD);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }
    t2 = Clock::now();
    milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
    std::cout << "Read " << NUM_ENTRIES << " entries in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
              << " milliseconds -> " << (NUM_ENTRIES / milliseconds) * 1000 << " entries per second." << std::endl;
    cleanup();

}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}