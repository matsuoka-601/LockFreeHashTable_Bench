# Benchmark for lock free hashtable
In this repository, performance of lock based hash table and lock-free hash table is compared.

The lock based hash table is the one used in [FastUniq](https://github.com/matsuoka-601/FastUniq/tree/main). The table is partitioned into multiple (e.g. 512) buckets and each bucket has its own lock. If the number of buckets is much larger than the number of threads, this hash table is expected to exhibit good scalability.


The lock-free hash table is based on [The World's Simplest Lock-Free Hash Table](https://preshing.com/20130605/the-worlds-simplest-lock-free-hash-table/). In the article, a very simple lock-free hash table is implemented with only insert (and get) function using CAS (compare and swap) instruction. Implementing a resize function is a much more challenging task, so the table size is fixed in this repository.

