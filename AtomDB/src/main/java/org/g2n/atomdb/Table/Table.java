package org.g2n.atomdb.Table;

import org.g2n.atomdb.Level.Level;
import com.google.common.base.Preconditions;
import org.g2n.atomdb.db.DbComponentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.g2n.atomdb.search.Search;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * TODO:
 * We need to have a shared lock between the table and the search, as the file can change underneath while we are searching.
 */

public class Table {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private final Search search;
    private final Map<Level, Integer> tableSize;
    private final Map<Level, SortedSet<SSTInfo>> levelToFilesMap;
    private final SSTFileNamer sstFileNamer;
    private final DbComponentProvider dbComponentProvider;

    public Table(Path dbPath, Search search, DbComponentProvider dbComponentProvider) {
        this.search = search;
        this.sstFileNamer = new SSTFileNamer(dbPath);
        this.dbComponentProvider = dbComponentProvider;
        levelToFilesMap = new ConcurrentHashMap<>();
        tableSize = new ConcurrentHashMap<>() ;

        for (Level value : Level.values()) {
            levelToFilesMap.put(value, new TreeSet<>());
        }
        for (Level value : Level.values()) {
            tableSize.put(value, 0);
        }
    }

    public void fillLevels() {
        Set<SSTFileNameMeta> validSSTFiles = sstFileNamer.getValidSSTFiles();
        for (SSTFileNameMeta sstMeta : validSSTFiles) {
            var sstInfo = SSTFileHelper.getSSTInfo(sstMeta, dbComponentProvider);
            addSST(sstMeta.level(), sstInfo);
        }
    }

    public synchronized SSTFileNameMeta getNewSST(Level level) {
        Preconditions.checkNotNull(level);
        return sstFileNamer.nextSst(level);
    }

    public synchronized void addSST(Level level, SSTInfo sstInfo) {
        requireNonNull(sstInfo.getSstPath(), "SST file cannot be null");
        requireNonNull(level, "Level cannot be null");
        if (!levelToFilesMap.get(level).add(sstInfo)) { // todo this if can be removed.
            throw new IllegalStateException("Adding of the same file");
        }
        tableSize.put(level, tableSize.get(level) + sstInfo.getFileTorsoSize());
        search.addSSTInfo(sstInfo);
    }

    public synchronized void removeSST(SSTInfo sstInfo) throws Exception {
        requireNonNull(sstInfo.getSstPath(), "SST file cannot be null");
        if (!levelToFilesMap.get(sstInfo.getLevel()).contains(sstInfo)) {
            throw new IllegalStateException("Trying to remove SST that is not in the table: " + sstInfo.getSstPath().toAbsolutePath());
        }
        levelToFilesMap.get(sstInfo.getLevel()).remove(sstInfo);
        tableSize.put(sstInfo.getLevel(), tableSize.get(sstInfo.getLevel()) - sstInfo.getFileTorsoSize());
        search.removeSSTInfo(sstInfo);
        Files.delete(sstInfo.getSstPath());
    }

    public SortedSet<SSTInfo> getSSTInfoSet(Level level) {
        return levelToFilesMap.get(level);
    }

    public int getCurrentLevelSize(Level level) {
        return tableSize.get(level);
    }

}
