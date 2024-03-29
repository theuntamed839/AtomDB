package sstIo;

import Constants.DBConstant;
import Level.Level;
import Mem.ImmutableMem;
import db.KVUnit;

public class SSTReader {
    public SSTReader(ImmutableMem<byte[], KVUnit> memtable) {
        new SSTHeader(
                DBConstant.SST_VERSION,
                Level.LEVEL_ZERO,
                DBConstant.CRC32C_CHECKSUM_TYPE,
                DBConstant.LZ4_COMPRESSION_TYPE,
                DBConstant.NUMBER_OF_KEYS_IN_CHUNK,
                DBConstant.SHORTEST_COMMON_PREFIX_USED,
                memtable.getNumberOfEntries(),
                new SSTKeyRange(memtable.getFirstKey(), memtable.getLastKey()));
    }
}
