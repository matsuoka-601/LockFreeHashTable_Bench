#include <random>
#include <climits>
#include <unordered_set>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <filesystem>
#include <cassert>
#include <omp.h>
#include <functional>
#include "ParallelHashTable.hpp"
#include "LockFreeHashTable.hpp"
#include "common/cmdline.h"

constexpr unsigned PREFETCH_STRIDE = 16;

template<class T> void ProcessChunk(
    std::vector<uint64_t> &hashes, 
    std::pair<unsigned, unsigned> &chunk, 
    T &hashTable
) {
    unsigned beg = chunk.first;
    unsigned end = chunk.second;
    for (unsigned i = beg; i < end; i++) {
        if (i + PREFETCH_STRIDE < end) 
            hashTable.Prefetch(hashes[i + PREFETCH_STRIDE]);
        hashTable.Insert(hashes[i]);
    }
}

template<class T> double BenchIter(
    std::vector<uint64_t> &hashes,
    std::vector<std::pair<unsigned, unsigned>> &chunks, 
    T &hashTable,
    unsigned correctAns, 
    unsigned threadCount
) 
{
    auto start = std::chrono::high_resolution_clock::now();
    omp_set_num_threads(threadCount);
    #pragma omp parallel
    {
        int threadNum = omp_get_thread_num();
        ProcessChunk(hashes, chunks[threadNum], hashTable);
    }
    auto end = std::chrono::high_resolution_clock::now();
    double runTimeSum = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    if (hashTable.Size() != correctAns) {
        std::cerr << "Error: The number of unique hashes is incorrect: ";
        std::cerr << "Correct: " << correctAns << " Returned answer: " << hashTable.Size() << "\n";
        exit(1);
    }

    return runTimeSum;
}

// Bench the scalability changing the number of threads

int main(int argc, char** argv) {
    cmdline::parser p;
    p.add<unsigned>("hashes", 'n', "Number of hashes", false, 30000000, cmdline::range(1, INT_MAX));
    p.add<unsigned>("unique-hashes", 'u', "Number of unique hashes", false, 1000000, cmdline::range(1, INT_MAX));
    p.add("help", 'h', "print help");

    if (!p.parse(argc, argv) || p.exist("help")) {
        std::cerr << p.error_full() << p.usage();
        return 1;
    }

    unsigned l = p.get<unsigned>("hashes");
    unsigned u = p.get<unsigned>("unique-hashes");

    if (l < u) {
        std::cerr << "Error: Invalid input. The number of unique strings (-u) should be equal to or less than the number of lines (-l)\n";
        return 1;
    }

    std::cerr << "Generating hashes...\n";

    std::unordered_set<uint64_t> uniqueHashesSet;

    std::random_device seed;
    std::mt19937_64 rng(seed());
    for (unsigned i = 0; i < u; i++) {
        unsigned counter = 0;
        while (true) {
            uint64_t hash = rng();

            if (uniqueHashesSet.count(hash)) {
                continue;
            } else {
                uniqueHashesSet.insert(hash);
                break;
            }
        }
    }

    std::vector<uint64_t> uniqueHashes(uniqueHashesSet.begin(), uniqueHashesSet.end());
    std::vector<uint64_t> hashes(l);

    for (unsigned i = 0; i < l; i++) {
        if (i < u) hashes[i] = uniqueHashes[i];
        else hashes[i] = uniqueHashes[rng() % u];
    }


    constexpr unsigned BENCH_REPEAT = 3;
    for (unsigned threadCount = 1; threadCount <= omp_get_num_procs(); threadCount++) {
        std::cerr << threadCount << ((threadCount == 1) ? " thread\n" : " threads\n");

        std::vector<std::pair<unsigned, unsigned>> chunks;
        unsigned perChunkLen = l / threadCount;
        for (unsigned t = 0; t < threadCount; t++) {
            unsigned beg = perChunkLen * t;
            unsigned end = (t == threadCount - 1) ? l : ((t + 1) * perChunkLen);
            chunks.push_back({ beg, end });
        }

        double runTimeSum1 = 0;
        double runTimeSum2 = 0;
        // Take average of BENCH_REPEAT times
        for (unsigned i = 0; i < BENCH_REPEAT; i++) {
            ParallelHashTable pht(threadCount);
            LockFreeHashTable lfht(u);
            runTimeSum1 += BenchIter(hashes, chunks, pht, u, threadCount);
            runTimeSum2 += BenchIter(hashes, chunks, lfht, u, threadCount);
        }
        double throughput1 = l / runTimeSum1 / 1e3;
        double throughput2 = l / runTimeSum2 / 1e3;
        std::cerr << "Parallel Hash Table   : " << throughput1 << " million insertion/sec\n";
        std::cerr << "Lock Free Hash Table  : " << throughput2 << " million insertion/sec\n";
    }
}