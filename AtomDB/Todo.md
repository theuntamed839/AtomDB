# Todo


# current sst architecture
* current SST Structure
* BS-> Binary Search position pointer
* CH-> checksum
* EN-> number of entries in the sst
* VID-> version id of that sst
* LEN-> length
* SK-> smallest Key
* LK-> largest Key
* LEV-> level of that sst
* K-> key
* V-> value
* MAR-> MARKER
* P->pointer to key

``` 
[
 {VID | LEV | BS | EN | SK_LEN | SK | SK_CH | LK_LEN | LK | LK_CH} = header
 {K_LEN | K | MAR | V_LEN | V | CH} = middleBlock, multiple such block
 {K_LEN | K | MAR | CH} = middleBlock, when key is deleted
 {P1,P2,P3....Pn} = Key positions for binary search
 {Bloom filter} = Bloom filter
]
```

# Branch Optimization Roadmap
1. Batch write
2. Batch Read of header (2 times read will retrieve the whole header). Adding of a long 
3. Replace Long with Int
4. Performing partial binary search
5. Increase ByteBuffer size (400KB is max item size in dynamoDB)

## Elaboration of features used in Optimization Roadmap
### 1. Performing partial binary search
First we calculate how many reads will be required to fully retrieve the pointers.
now do NumberOfPointers/(TotalNumberOfReadsToReadWholePointerList) = partials
Now get the keys at every partials and store them together.(Compress them together)
While reading we read all the keys which we stored together and find the given key in it.
once we know what range is it, we can calculate which section of pointers we need to retrieve,
and now it's a one read to get the required pointers and from here its usual stuff.
With this we reduced the need to get all the pointers in memory and save multiple reads.
Catch is that the keys present at the partials shouldn't be too long as this will increase the read cost.

Overheads for a 50000 entries

Current architecture
97ConstantPointerRead + 16ConstantKSizeRead  + 16XKeyRead = 129

New
97XKeyWrite
97XKeyRead + 1ConstantRead + 9ConstantKSizeRead  + 9XKeyRead = 116

97XKeyRead will be batch read and also can be compressed.

# Enhancement
1. Allow Key value to be of any variable size or at least some big number like 100mb.
2. Find bloom filter whose storage and retrieval is in our hand
3. Value and Key together overflowing from 4096bytes byteBuffer.
4. More information(Metadata) should be included in the SST.

# Think tank
1. we have wal, and then we have level one sst. Basically writing of same data twice. can we improve
2. Storing all the values first then keys, basically when binary search done we can improve the disk needle movement
since now the needle need not move to a huge gap as we reduced the values inbetween its jumps.
3. We need now get the whole byte array key in to the memory, look at how the comparison is done in the code.
basically it compares each bytes so partial key can be retrieved and checked.
4. Map and unMap as required. MappedByteBuffer https://github.com/dain/leveldb/blob/130db6965ebba2c19106c5355bee0c8dc59f57db/leveldb/src/main/java/org/iq80/leveldb/impl/MMapLogWriter.java#L214
   invokeCleaner to unmap the map. https://github.com/dain/leveldb/blob/130db6965ebba2c19106c5355bee0c8dc59f57db/leveldb/src/main/java/org/iq80/leveldb/util/ByteBufferSupport.java
