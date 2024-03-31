AtomDB Release 1.0

# Objective
1. Redesign WAL, for faster read and write, if required.
2. Memtable, storing of one memtable extra, stating that latest written is query for frequently.
3. 1 disk seek per request.
   1. With 10% rule we can get done with one disk seek and load of reads, PER SST. 
   2. To improve on for 1 disk seek per request.
      1. Bloom
         1. Global Bloom filter to discard any non-existent key.
         2. Level wise Bloom filter.
         3. File wise bloom filter. (here we are good with binary search on 10% keys).
      2. Upper and lower bound check (SK and LK)
      3. Then binary search from the latest file to the older files
      4. Can same the crc32c of keys in memory (Future)
4. Cache all the open file descriptors.
5. Minimize the number of files being accessed, need a better predictor and cache.
6. improvement of Read, write
7. improvement of compaction
8. Making two sst in memory to write as one.
9. Let's implement the WAL, MEM, SST, TABLE, COMPACTION, CACHE correctly and then add on with multithreading, lock free, virtual thread, SIMD, etc.
10. can use treeset to find the overlapping intervals. and use posix_fadvise for compaction.
11. can use posix_fadvise for writing sst.
12. can use posix_fadvise for sequential reads.
13. for random reads use mmap.
14. Implement Manifest file https://github.com/google/leveldb/blob/main/doc/impl.md#manifest
15. We will be having 3 caches.
    1. first the primary cache which will cache k-v directly.
    2. File blocks
    3. File descriptor, which will be evicted based on the avaliable descriptors.
16. Reading of sst metadata when db restarts. Thinking needed.
17. Batch writing and reading.
18. MMap the whole file for compaction.
19. we mandatorily not store the 10% keys in memory, we can flush based on memory available and reread when required. but this will have some performance impact.
20. finalise how big can be the key and value.
21. idea is to make files obsolete and then delete them periodically.
22. benchmark between partial mapping and full mapping of file.
23. https://github.com/google/jimfs can be used for testing, its a in-memory files.
24. Performances Tuning can be done with
    1. using of better hash class for table in the sst chunks
    2. use of full mmap of reading files.
25. remove of contributors list.
26. wherever possible convert to the latest java features. makes class and record and using sealed. 
27. can we utilize the async file io to read the block and keep while searching. so while searching we might encounter 2-3 files to search, here we can load in a async was. and the reading wont go waste as they will be cache and will be used by someone else.
28. before release do benchmark based on the different writers and readers, see if the bufferedWriter is of any help when we have mmapped the file. since mmap does the same thing.
29. can we use cleaner and cleanable apis for unmapping ? and also does unmap calls the gc ? and also can't we just learn all the maping and unmapping be done and call System.gc() when we close the class ?
30. is common prefix adding any value ? need to do benchmark.
31. Observations
    1. for entries of 50000, and k and v size 1024 both.
    2. common saved ~50000 bytes and compression ate 700000 bytes.
    3. common prefix saved but compression didnt, (mind random class was used to generate byte data)


# Bottlenecks
1. multiple sst reads. (Partially solved by Objective->3)
2. heavy compaction.
3. No proper caching for recently retrieved block, k-v,
4. multiple disk seeks and reads for single request. **(Many sst)**
5. multiple jumps in a **single SST**
6. N sst -> N bloom filter reads. in worst case N SST binary search. ( can't we unify things)
7. Compacting random files with any patterns.
8. Do not compress the plain primitives, they are not compressible.




### TODO:
- [ ] **IMP** we can implement the shared keys in sst block down the list but need to understand the cost to write sst and also compaction cost and also does it help in reading.
  - basically when we have n keys in a sorted order, some keys will have thier prefix similar which can taken common out.
  - we need not store the prefix since we already have the first key in the memory, we just need to prefix length.
  - this will help in comparing as well as storing, since we will store less now.
  - before we implement we need to find the cost.
  - writing computation cost.
  - compaction reading and writing cost. (Very important)
  - reading cost, how many disk seeks and reads.
  - Put this in chatGPT "i have a list of strings i want to take its prefix common out."
- [ ] Thoughts on this,
  - If a file doesn't generate positive results for reads then it can be taken for compaction.
    this file has very sparse data, for reading efficiency we need concentrated data.
- [ ] Read, Write improvement.
- [ ] Compaction Improvement.
- [ ] Cache.
- [ ] Use of LevelDB interfaces.
- [ ] Faster Log writing and reader, is there a way were we convert the log into sst ?
- [ ] Table, basically all the meta data storage. 
- [ ] Only block or big chunks compression.
- [x] LZ4 at use.
- [ ] recently read blog cache.
- [ ] have a look at design and patterns.
- [ ] data intensive book read.
- [ ] how to write efficient wal.
- [ ] multithreading, lock free database.
- [ ] separate thread for compaction.
- [ ] latest java we will use.
- [ ] learn new features of java.
- [x] Crc32c comparison, java's builtIn is super fast.
  - benchmarked, java's implementation uses some low level stuff for computing.
- [x] Use of 64bit checksum, to find key present in the 10% block.
  - xxhash 64bit hash, and a bit slower than crc32c
  - use of crc32c seems viable. as the error is less than compared to the bloom filter.
  - bloom filter is not native for our system, where we dont have much flexibility
  - don't know the creation and restoration cost of the bloom filter.
- [x] Check what works best, the hashes or bloom filter.
  - hash seems good. and that took crc32c seems perfect.
- [ ] compaction, the sst file which close to each other based on the overlaps. here the idea is to find a sst which has a big range and then compact it with other sst which has small ranges together.
- [ ] Providing ACID
- [ ] OPTIMIZATION THINK: creating of tree of the spare keys to find the correct file.
- [ ] OPTIMIZATION THINK: in a block we can store partial keys, for example maybe the first key defines the commons prefix and further keys has suffix.
- [ ] can we make atomDB into sqlDB ?
- [ ] Need to have a look on rocksDB and levelDB docs.
  - https://github.com/facebook/rocksdb
  - https://www.youtube.com/watch?v=jvmvezq8Twg
- [ ] Read Virtual threads.
- [ ] Guava's bloom filter is just not it, need to find another one.
- [ ] benchmark our system with plane filechannel, mmap, and posix_fadvise. write the ChannelBackedWriter in a very abstract way.
- [ ] before writing have a look at leveldb and the other project, for unit and integration test cases. as well as how they used the option class.
- [ ] what if we manage the block of pages by our own ? just like wiredTiger (mongodb)
  - https://source.wiredtiger.com/develop/arch-index.html
- [ ] posix_fadvise seems viable option for compaction, where we give hint to the OS that we will read the ssts in sequential manner.
  - search on chatgpt, database basic channel "posix_fadvise vs mmap" 
- [ ] before writing the new sst, just check if there is a need for compaction and see if the file overlaps with other. so that directly compacted these files.
- [ ] need to add magic number at the end of the sst. to mark the end of sst.
- [ ] Think on what we can cache.
- [ ] what really is cache obvious data structure.
- cant we apply common prefix to values.


#### Information links
* LZ4
  * https://github.com/lz4/lz4-java/blob/master/src/java/net/jpountz/lz4/LZ4DecompressorWithLength.java
* Deign patterns
  * https://medium.com/@apusingh1967/every-senior-java-developer-must-know-how-and-when-to-use-generics-lower-and-upper-bound-89372c10d282
* Different Architectures.
* Bloom filter
  * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/BloomFilter.java
* Vector in java
  * https://jbaker.io/2022/06/09/vectors-in-java/
  * https://www.youtube.com/watch?v=ZQFzMfHIxng
* JMH Java Microbenchmark Harnees & other benchmark
  * https://jenkov.com/tutorials/java-performance/jmh.html
  * https://github.com/OpenHFT/JLBH?tab=readme-ov-file
* Lock-free Programming
  * https://preshing.com/20120612/an-introduction-to-lock-free-programming/
  * https://www.linkedin.com/advice/0/what-some-examples-lock-free-wait-free-data-structures
  * https://www.cs.cmu.edu/~410-s05/lectures/L31_LockFree.pdf
  * https://medium.com/@tylerneely/fear-and-loathing-in-lock-free-programming-7158b1cdd50c
  * http://15418.courses.cs.cmu.edu/spring2013/article/46
  * https://www.youtube.com/watch?v=c1gO9aB9nbs
  * https://www.youtube.com/watch?v=lVBvHbJsg5Y
  * https://www.1024cores.net/home/lock-free-algorithms/introduction
  * https://lwn.net/Articles/844224/
* Risc vs Cisc
  * https://cs.stanford.edu/people/eroberts/courses/soco/projects/risc/risccisc/
* MMap
  * https://news.ycombinator.com/item?id=25701959
  * https://sasha-f.medium.com/why-mmap-is-faster-than-system-calls-24718e75ab37
  * https://news.ycombinator.com/item?id=29936104
  * https://www.youtube.com/watch?v=1BRGU_AS25c
* Posix_fadvise
  * https://github.com/hopshadoop/hops/blob/master/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/nativeio/NativeIO.java
* LevelDB explanation
  * https://segmentfault.com/a/1190000040286395/en (Very well written)
  * https://docs.riak.com/riak/kv/latest/setup/planning/backend/leveldb/index.html#:~:text=Comparison%20of%20eLevelDB%20and%20Bitcask&text=Bitcask%20stores%20keys%20in%20memory,LevelDB%20will%20need%20two%20seeks. (Compaction)
  * https://axlgrep.github.io/tech/leveldb-sst-file.html
  * https://chenju2k6.github.io/blog/2018/11/leveldb
  * https://rocksdb.org/blog/
* Compaction
  * https://smalldatum.blogspot.com/2018/08/name-that-compaction-algorithm.html
  * https://github.com/facebook/rocksdb/wiki/Compaction
* Sysetm programming 
  * https://www.cs.cornell.edu/courses/cs4414/2023sp/
* Write optimization
  * https://www.reddit.com/r/cpp_questions/comments/18g513s/resources_for_learning_writing_optimized_code/
* Cache obvious
  * https://www.reddit.com/r/programming/comments/90o1g/cache_oblivious_algorithms/
  * https://www.reddit.com/r/programming/comments/8smna/algorithmconscious_cacheoblivious/
  * https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.44.5650
  * https://www.reddit.com/r/programming/comments/fvqz4/cacheoblivious_algorithms/
  * http://blog.netduma.com/2013/01/cache-money-hoes-attached-code.html
  * https://rcoh.me/posts/cache-oblivious-datastructures/
* Page cache
  * https://manybutfinite.com/post/page-cache-the-affair-between-memory-and-files/
* Testing learning
  * https://junit.org/junit5/docs/current/user-guide/
  * https://javadoc.io/doc/org.mockito/mockito-core/latest/org/mockito/Mockito.html