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

[ {VID | LEV | BS | EN | SK_LEN | SK | SK_CH | LK_LEN | LK | LK_CH}=header
{K_LEN | K | MAR | V_LEN | V | CH}=middleBlock ideally, btw this will be multiple
{K_LEN | K | MAR | CH}=middleBlock when key is deleted
{P1,P2,P3.....}=End


