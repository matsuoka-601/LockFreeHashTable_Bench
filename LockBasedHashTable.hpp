#pragma once
#include "common/common.hpp"
#include <shared_mutex>
#include <vector>
#include <mutex>

class HashTable {
    static constexpr float LOAD_FACTOR = 0.5;
    static constexpr u32 INIT_CAPACITY = 64;
    u64 EMPTY = 0xffffffffffffffff;
    u32 capacity;
    u32 size;
    u64 *data;

    inline u32 CalcSlotIdx(u64 hash) {
        return (hash >> 32) % capacity;
    }

    inline bool InsertImpl(u64 hash) {
        u32 i = CalcSlotIdx(hash);
        for (; ; i = (i + 1) % capacity) {
            if (data[i] == EMPTY) { 
                data[i] = hash;
                return true;
            } else if (data[i] == hash) {
                return false;
            }
        }
    }

    void resize() {
        u64* oldData = data;
        data = (u64*) malloc(2 * capacity * sizeof(u64));
        for (u32 i = 0; i < 2 * capacity; i++) {
            data[i] = EMPTY;
        }
        capacity *= 2;

        for (u32 i = 0; i < capacity / 2; i++) {
            if (oldData[i] != EMPTY) {
                InsertImpl(oldData[i]);
            }
        }

        free(oldData);
    }
public:
    HashTable() {
        data = (u64*)malloc(INIT_CAPACITY * sizeof(u64));
        capacity = INIT_CAPACITY;
        size = 0;
        for (u64 i = 0; i < capacity; i++)
            data[i] = EMPTY;
    }

    ~HashTable() {
        free(data); 
    }

    bool Find(u64 hash) {
        u32 i = CalcSlotIdx(hash);
        for (; ; i = (i + 1) % capacity) {
            if (data[i] == EMPTY) { 
                return false;
            } else if (data[i] == hash) {
                return true;
            }
        }
    }

    bool Insert(u64 hash) {
        while (size > capacity * LOAD_FACTOR) {
            resize();
        }

        bool insertResult = InsertImpl(hash);
        size += insertResult;
        return insertResult;
    }

    u32 Size() {
        return size;
    }

    inline void Prefetch(u64 hash) {
        u32 i = CalcSlotIdx(hash);
        __builtin_prefetch(data + i);
    }
};

class ParallelHashTable {
    struct Bucket {
        std::shared_mutex mtx;
        HashTable table;

        Bucket() : table() {} 

        Bucket(Bucket&& other) noexcept : table(std::move(other.table)) {}
    };
    std::vector<Bucket> buckets;

    static constexpr u32 BUCKETS_THREADS_FACTOR = 64;

    inline u32 CalcBucketIdx(u64 hash) {
        return (hash & ((1LL << 32) - 1)) % buckets.size();
    }
public:
    ParallelHashTable(u32 num_threads) {
        buckets.resize(num_threads * BUCKETS_THREADS_FACTOR);
    }

    bool Insert(u64 hash) {
        u32 bucketIdx = CalcBucketIdx(hash);
        Bucket &bucket = buckets[bucketIdx];

        std::shared_lock<std::shared_mutex> readLock(bucket.mtx);
        if (bucket.table.Find(hash)) {
            return false;
        }
        readLock.unlock();
        std::unique_lock<std::shared_mutex> writeLock(bucket.mtx);
        return bucket.table.Insert(hash);
    }

    void ShowBucketsSize() {
        for (auto &bucket: buckets) {
            std::cerr << bucket.table.Size() << "\n";
        }
    }

    inline void Prefetch(u64 hash) {
        u32 bucketIdx = CalcBucketIdx(hash);
        Bucket &bucket = buckets[bucketIdx];
        bucket.table.Prefetch(hash);
    }

    u32 Size() {
        u32 ret = 0;
        for (auto &bucket: buckets) {
            ret += bucket.table.Size();
        }
        return ret;
    }
};