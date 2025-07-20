package org.g2n.atomdb.Table;

import org.g2n.atomdb.Level.Level;
import com.google.common.base.Preconditions;
import org.g2n.atomdb.SSTIO.Intermediate;
import org.g2n.atomdb.SSTIO.SSTHeader;
import org.g2n.atomdb.db.DbComponentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * TODO:
 * We need to have a shared lock between the table and the search, as the file can change underneath while we are searching.
 */

public class Table {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private final Map<Level, Integer> tableSize = new ConcurrentHashMap<>();
    private final Map<Level, SortedSet<SSTInfo>> levelToFilesMap = new ConcurrentHashMap<>();
    private final SortedSet<SSTInfo> allFilesSet = new ConcurrentSkipListSet<>();
    private final SortedSet<SSTInfo> fileListView = Collections.unmodifiableSortedSet(allFilesSet);
    private final SSTFileNamer sstFileNamer;
    private final DbComponentProvider dbComponentProvider;

    public Table(Path dbPath, DbComponentProvider dbComponentProvider) {
        this.sstFileNamer = new SSTFileNamer(dbPath);
        this.dbComponentProvider = dbComponentProvider;
        for (Level value : Level.values()) {
            levelToFilesMap.put(value, new TreeSet<>());
        }
        for (Level value : Level.values()) {
            tableSize.put(value, 0);
        }
        fillLevels();
    }

    private void fillLevels() {
        Set<SSTFileNameMeta> validSSTFiles = sstFileNamer.getValidSSTFiles();
        for (SSTFileNameMeta sstMeta : validSSTFiles) {
            var sstInfo = SSTFileHelper.getSSTInfo(sstMeta, dbComponentProvider);
            Level level = sstInfo.getLevel();
            levelToFilesMap.get(level).add(sstInfo);
            allFilesSet.add(sstInfo);
            tableSize.put(level, tableSize.get(level) + sstInfo.getFileTorsoSize());
        }
    }

    public synchronized SSTFileNameMeta getNewSST(Level level) {
        Preconditions.checkNotNull(level);
        return sstFileNamer.nextSst(level);
    }

    public synchronized void addToTheTable(List<Intermediate> intermediates) throws IOException {
        Preconditions.checkArgument(intermediates.stream().map(Intermediate::sstHeader).map(SSTHeader::getLevel).distinct().count() == 1,
                "All intermediates must be of the same level");
        Level level = intermediates.getFirst().sstHeader().getLevel();
        int torsoSize = 0;
        SortedSet<SSTInfo> levelSSTInfos = levelToFilesMap.get(level);

        for (Intermediate inter : intermediates) {
            SSTFileNameMeta meta = getNewSST(level);
            Files.move(inter.path(), meta.path(), StandardCopyOption.ATOMIC_MOVE);
            SSTInfo info = new SSTInfo(
                    meta.path(),
                    inter.sstHeader(),
                    inter.pointers(),
                    inter.filter(),
                    meta
            );
            torsoSize += info.getFileTorsoSize();
            levelSSTInfos.add(info);
            allFilesSet.add(info);
        }
        tableSize.put(level, tableSize.get(level) + torsoSize);
    }

    public synchronized void removeSST(Collection<SSTInfo> ssts) {
        for (SSTInfo info : ssts) {
            Level level = info.getLevel();
            if (!levelToFilesMap.get(level).contains(info)) {
                throw new IllegalStateException("Trying to remove SST that is not in the table: " + info.getSstPath().toAbsolutePath()
                        + " all files: " + levelToFilesMap.get(level).stream().map(SSTInfo::getSstPath).toList());
            }
            levelToFilesMap.get(level).remove(info);
            allFilesSet.remove(info);
            tableSize.put(level, tableSize.get(level) - info.getFileTorsoSize());
            try {
                Files.deleteIfExists(info.getSstPath());
            } catch (Exception e) {
                logger.error("Failed to delete SST file: {}", info.getSstPath().toAbsolutePath(), e);
            }
        }
    }

    public SortedSet<SSTInfo> getFileListView() {
        return fileListView;
    }

    public SortedSet<SSTInfo> getSSTInfoSet(Level level) {
        return levelToFilesMap.get(level);
    }

    public int getCurrentLevelSize(Level level) {
        return tableSize.get(level);
    }
}
