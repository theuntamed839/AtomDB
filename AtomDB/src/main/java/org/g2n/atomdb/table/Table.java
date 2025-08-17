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
    private final Map<Level, SortedSet<SSTInfo>> levelToFilesMap = new HashMap<>();
    private final SSTFileNamer sstFileNamer;
    private final Search search;
    private final DbComponentProvider dbComponentProvider;

    public Table(Path dbPath, Search search, DbComponentProvider dbComponentProvider) {
        this.sstFileNamer = new SSTFileNamer(dbPath);
        this.search = search;
        this.dbComponentProvider = dbComponentProvider;
        fillLevels();
    }

    private void fillLevels() {
        Set<SSTFileNameMeta> validSSTFiles = sstFileNamer.getValidSSTFiles();
        for (SSTFileNameMeta sstMeta : validSSTFiles) {
            var sstInfo = SSTFileHelper.getSSTInfo(sstMeta, dbComponentProvider);
            Level level = sstInfo.getLevel();
            levelToFilesMap.computeIfAbsent(level, k -> new ConcurrentSkipListSet<>()).add(sstInfo);
        }
        search.addAndRemoveSST(
                levelToFilesMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet()),
                Collections.emptyList()
        );
    }

    public synchronized void addToTheTableAndDelete(List<Intermediate> intermediatesToAdd, Collection<SSTInfo> toRemove) throws IOException {
        Preconditions.checkArgument(intermediatesToAdd.stream().map(Intermediate::sstHeader).map(SSTHeader::getLevel).distinct().count() <= 1,
                "All intermediates must be of the same level");
        toRemove.stream()
                .filter(info -> !levelToFilesMap.getOrDefault(info.getLevel(), Collections.emptySortedSet()).contains(info))
                .findFirst()
                .ifPresent(info -> {
                    throw new IllegalArgumentException("Trying to remove SST that is not in the table: " +
                            info.getSstPath().toAbsolutePath());
                });

        SortedSet<SSTInfo> added = Collections.emptySortedSet();
        if (!intermediatesToAdd.isEmpty()) {
            // when all the files had only deleted entries, which also didn't appear in the further levels.
            // that's why result is empty. but we still need to remove the old ssts.
            added = addToTheTable(intermediatesToAdd);
        }
        search.addAndRemoveSST(added, toRemove);
        removeSST(toRemove);
    }

    public SortedSet<SSTInfo> getSSTInfoSet(Level level) {
        return Collections.unmodifiableSortedSet(levelToFilesMap.getOrDefault(level, Collections.emptySortedSet()));
    }

    public int getNumberOfFilesInLevel(Level level) {
        return levelToFilesMap.getOrDefault(level, Collections.emptySortedSet()).size();
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
        levelToFilesMap.computeIfAbsent(level, k -> new ConcurrentSkipListSet<>()).addAll(ssts);
        return ssts;
    }

    private void removeSST(Collection<SSTInfo> ssts) {
        for (SSTInfo info : ssts) {
            Level level = info.getLevel();
            levelToFilesMap.get(level).remove(info);
            try {
                Files.deleteIfExists(info.getSstPath());
            } catch (Exception e) {
                logger.error("Failed to delete SST file: {}", info.getSstPath().toAbsolutePath(), e);
            }
        }
    }
}
