#pragma once
#include "common/common.hpp"
#include <atomic>
#include <cassert>

class LockFreeHashTable {
    using Entry = std::atomic<u64>;

    Entry* entries;
    u32 capacity;
    std::atomic<u32> size;

    inline u32 CalcSlotIdx(u64 hash) {
        return (hash >> 32) % capacity;
    }
public:
    LockFreeHashTable (u32 capacityLowerBound) {
        size = 0;
        capacity = 1;
        capacityLowerBound *= 1.5;
        while (capacity < capacityLowerBound) capacity *= 2;
        entries = (Entry*)malloc(capacity * sizeof(Entry));
        for (unsigned i = 0; i < capacity; i++)
            entries[i] = 0;
    }

    ~LockFreeHashTable() {
        free(entries);
    }

    bool Insert(u64 hash) {
        u32 idx = CalcSlotIdx(hash);
        for (; ; idx = (idx + 1) % capacity) {
            u64 probedHash = entries[idx].load(std::memory_order_relaxed);
            if (probedHash != hash) {
                if (probedHash != 0) // Another value was already inserted
                    continue;

                u64 expected = 0;
                bool result = entries[idx].compare_exchange_strong(expected, hash, std::memory_order_relaxed);
                if (result) {
                    // Insert succeeded
                    size++;
                    return true;
                }
                if (expected == hash) { // Another thread inserted "hash"
                    return false;
                }
            } else { 
                // Already inserted
                return false;
            }
        }
        assert(false);
    }

    u32 Size() {
        return size;
    }

    void Prefetch(u64 hash) {
        u32 idx = CalcSlotIdx(hash);
        __builtin_prefetch(entries + idx);
    }
};