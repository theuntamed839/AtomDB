package org.g2n.atomdb.table;

import org.g2n.atomdb.level.Level;
import com.google.common.base.Preconditions;
import org.g2n.atomdb.search.Search;
import org.g2n.atomdb.sstIO.Intermediate;
import org.g2n.atomdb.sstIO.SSTFileHelper;
import org.g2n.atomdb.sstIO.SSTHeader;
import org.g2n.atomdb.db.DbComponentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/*
TODO: we should find a strategy which avoids creating too many SST files in the same folder, surely there would be some limit how many files can a directory hold.
 */

public class Table {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private final Map<Level, Long> tableSize = new HashMap<>();
    private final Map<Level, SortedSet<SSTInfo>> levelToFilesMap = new HashMap<>();
    private final SSTFileNamer sstFileNamer;
    private final Search search;
    private final DbComponentProvider dbComponentProvider;

    public Table(Path dbPath, Search search, DbComponentProvider dbComponentProvider) {
        this.sstFileNamer = new SSTFileNamer(dbPath);
        this.search = search;
        this.dbComponentProvider = dbComponentProvider;
        for (Level value : Level.values()) {
            levelToFilesMap.put(value, new ConcurrentSkipListSet<>());
        }
        for (Level value : Level.values()) {
            tableSize.put(value, 0L);
        }
        fillLevels();
    }

    private void fillLevels() {
        Set<SSTFileNameMeta> validSSTFiles = sstFileNamer.getValidSSTFiles();
        for (SSTFileNameMeta sstMeta : validSSTFiles) {
            var sstInfo = SSTFileHelper.getSSTInfo(sstMeta, dbComponentProvider);
            Level level = sstInfo.getLevel();
            levelToFilesMap.get(level).add(sstInfo);
        }
        for (Map.Entry<Level, SortedSet<SSTInfo>> entry : levelToFilesMap.entrySet()) {
            tableSize.put(entry.getKey(), (long) entry.getValue().size());
        }
        search.addAndRemoveSST(
                levelToFilesMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet()),
                Collections.emptyList()
        );
    }

    public synchronized void addToTheTableAndDelete(List<Intermediate> intermediatesToAdd, Collection<SSTInfo> toRemove) throws IOException {
        Preconditions.checkArgument(intermediatesToAdd.stream().map(Intermediate::sstHeader).map(SSTHeader::getLevel).distinct().count() == 1,
                "All intermediates must be of the same level");
        toRemove.stream()
                .filter(info -> !levelToFilesMap.get(info.getLevel()).contains(info))
                .findFirst()
                .ifPresent(info -> {
                    throw new IllegalArgumentException("Trying to remove SST that is not in the table: " +
                            info.getSstPath().toAbsolutePath());
                });

        SortedSet<SSTInfo> added = addToTheTable(intermediatesToAdd);
        search.addAndRemoveSST(added, toRemove);
        removeSST(toRemove);
    }

    private SortedSet<SSTInfo> addToTheTable(List<Intermediate> intermediates) throws IOException {
        Level level = intermediates.getFirst().sstHeader().getLevel();
        SortedSet<SSTInfo> ssts = new TreeSet<>();
        for (Intermediate inter : intermediates) {
            SSTFileNameMeta meta = sstFileNamer.nextSst(level);
            Files.move(inter.path(), meta.path(), StandardCopyOption.ATOMIC_MOVE);
            SSTInfo info = new SSTInfo(
                    inter.sstHeader(),
                    inter.pointers(),
                    inter.filter(),
                    meta
            );
            ssts.add(info);
        }
        levelToFilesMap.get(level).addAll(ssts);
        tableSize.put(level, tableSize.get(level) + intermediates.size());
        return ssts;
    }

    public void removeSST(Collection<SSTInfo> ssts) {
        for (SSTInfo info : ssts) {
            Level level = info.getLevel();
            levelToFilesMap.get(level).remove(info);
            tableSize.put(level,tableSize.get(level) - 1);
            try {
                Files.deleteIfExists(info.getSstPath());
            } catch (Exception e) {
                logger.error("Failed to delete SST file: {}", info.getSstPath().toAbsolutePath(), e);
            }
        }
    }

    public SortedSet<SSTInfo> getSSTInfoSet(Level level) {
        return Collections.unmodifiableSortedSet(levelToFilesMap.get(level));
    }

    public Long getCurrentLevelSize(Level level) {
        return tableSize.get(level);
    }
}
