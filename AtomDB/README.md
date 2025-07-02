# AtomDB

### TODO
- [ ] Value updating and delete.
- [ ] Improved threading in compaction and maybe try virtual threads
- [ ] Crash recovery
  - [ ] From WAL
  - [ ] During SST persisting
  - [ ] During compaction
- [ ] Understand the effect of compaction on read correctness and performance
- [ ] Improve the read performance, maybe caching.
- [ ] is common prefix adding any value ? need to do benchmark.
- [ ] Let's fix all the warnings in the code.
- [ ] Let's look at all the TODOs in the code.
- [ ] Unit tests
- [ ] Benchmark tests
- [ ] Documentation
- [ ] Maven release
- [ ] GitHub readme page explaining 
  - How to install, use, benchmarks, limitations, ideas, motivation and future work, Pictorial representation of architecture and sstPath


## SST file layout
* BS-> Binary Search position pointer
* CH-> checksum
* EN-> number of entries in the org.g2n.atomdb.sstPath
* VID-> version id of that org.g2n.atomdb.sstPath
* LEN-> length
* SK-> smallest Key
* LK-> largest Key
* LEV-> level of that org.g2n.atomdb.sstPath
* K-> key
* V-> value
* MAR-> MARKER
* P->pointer to key

``` 
[
 {VID | LEV | BS | EN | SK_LEN | SK | SK_CH | LK_LEN | LK | LK_CH} = header
 {K_LEN | K | MAR | V_LEN | V | CH} = middleBlock, multiple such block
 {K_LEN | K | MAR | CH} = middleBlock, when key is deleted
 {P1,P2,P3....Pn} = Key positions for binary org.g2n.atomdb.search
 {Bloom filter} = Bloom filter
]
```