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


# Enhancement
1. Allow Key value to be of any variable size or at least some big number like 100mb.
2. Find bloom filter whose storage and retrieval is in our hand
3. Value and Key together overflowing from 4096bytes byteBuffer.
