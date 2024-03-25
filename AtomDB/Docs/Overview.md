AtomDB Release 1.0

# Objective
1. 1 disk seek per SST.
2. Minimize the number of files being accessed, need a better predictor and cache.
3. improvement of Read, write
4. improvement of compaction
5. Making two sst in memory to write as one.
6. Lets implement the WAL, MEM, SST, TABLE, COMPACTION, CACHE correctly and then add on with multithreading, lock free, virtual thread, SIMD, etc.

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
  - hash seems good.
- [ ] compaction, the sst file which close to each other based on the overlaps. here the idea is to find a sst which has a big range and then compact it with other sst which has small ranges together.
- [ ] Providing ACID
- [ ] OPTIMIZATION THINK: creating of tree of the spare keys to find the correct file.
- [ ] OPTIMIZATION THINK: in a block we can store partial keys, for example maybe the first key defines the commons prefix and further keys has suffix.
- [ ] can we make atomDB into sqlDB ?
- [ ] Need to have a look on rocksDB and levelDB docs.
  - https://github.com/facebook/rocksdb
  - https://www.youtube.com/watch?v=jvmvezq8Twg
- [ ] Read Virtual threads.
- Guava's bloom filter is just not it, need to find another one.



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
* JMH Java Microbenchmark Harnees
  * https://jenkov.com/tutorials/java-performance/jmh.html
  * 