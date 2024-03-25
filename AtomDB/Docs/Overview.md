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

# Bottlenecks
1. multiple sst reads.
2. No compaction or heavy compaction.
3. No proper caching for recently retrieved block, k-v,
4. multiple disk seeks and reads for single request. **(Many sst)**
5. multiple jumps in a **single SST**
6. N sst -> N bloom filter reads. in worst case N SST binary search. ( can't we unify things)
7. Compacting random files with any patterns.
8. Do not compress the plain primitives, they are not compressible.




### TODO:
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
- [ ] benchmark our system with plane filechannel, mmap, and posix_fadvise. write the writer in a very abstract way.
- [ ] before writing have a look at leveldb and the other project, for unit and integration test cases. as well as how they used the option class.
- [ ] what if we manage the block of pages by our own ? just like wiredTiger (mongodb)
  - https://source.wiredtiger.com/develop/arch-index.html
- [ ] posix_fadvise seems viable option for compaction, where we give hint to the OS that we will read the ssts in sequential manner.
  - search on chatgpt, database basic channel "posix_fadvise vs mmap" 

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
* JMH Java Microbenchmark Harnees
  * https://jenkov.com/tutorials/java-performance/jmh.html
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