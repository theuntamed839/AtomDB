AtomDB Release 1.0

# Objective
1. 1 disk seek per SST.
2. Minimize the number of files being accessed, need a better predictor and cache.
3. improvement of Read, write
4. improvement of compaction
5. Making two sst in memory to write as one.

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
- [ ] Use of 64bit checksum, to find key present in the 10% block.


#### Information links
* LZ4
  * https://github.com/lz4/lz4-java/blob/master/src/java/net/jpountz/lz4/LZ4DecompressorWithLength.java
* Deign patterns
  * https://medium.com/@apusingh1967/every-senior-java-developer-must-know-how-and-when-to-use-generics-lower-and-upper-bound-89372c10d282
* Different Architectures.
* Bloom filter
  * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/BloomFilter.java