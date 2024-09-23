package Table;

import Constants.DBConstant;
import Level.Level;
import com.google.common.base.Preconditions;
import db.KVUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import search.Search;
import util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class Table implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private final Search search;
    private final Map<Level, Integer> tableSize;
    private final Map<Level, byte[]> lastCompactedKV;
    private Map<Level, SortedSet<SSTInfo>>  table;
    // todo this should be set at start
    private AtomicLong currentFileName = new AtomicLong(0);
    private final File dbFolder;
    private final String fileSeparatorForSplit =  Pattern.quote(File.separator);
    public Table(File dbFolder, Search search) {
        Preconditions.checkArgument(dbFolder.exists());
        this.dbFolder = dbFolder;
        this.search = search;
        table = Map.of(Level.LEVEL_ZERO, new TreeSet<SSTInfo>(),
                Level.LEVEL_ONE,         new TreeSet<SSTInfo>(),
                Level.LEVEL_TWO,         new TreeSet<SSTInfo>(),
                Level.LEVEL_THREE,       new TreeSet<SSTInfo>(),
                Level.LEVEL_FOUR,        new TreeSet<SSTInfo>(),
                Level.LEVEL_FIVE,        new TreeSet<SSTInfo>(),
                Level.LEVEL_SIX,         new TreeSet<SSTInfo>(),
                Level.LEVEL_SEVEN,       new TreeSet<SSTInfo>());
        tableSize = new HashMap<>() ;
        for (Level value : Level.values()) {
            tableSize.put(value, 0);
        }
        fillLevels();
        lastCompactedKV = new HashMap<>();
    }

    private void fillLevels() {
        long max = Long.MIN_VALUE;
        for (File file : dbFolder.listFiles()) {
            if (!file.getName().contains(".sst") || file.getName().contains(DBConstant.OBSOLETE)) {
                continue;
            }
            var split = file.getName().strip().replace(".sst", "").split("_");

            max = Math.max(Long.parseLong(split[1]), max);
            Level level = Level.fromID(split[0].charAt(0) - 48);

            var sstInfo = SSTFileHelper.getSSTInfo(file);
            addSST(level, sstInfo);
        }
        currentFileName.set(max != Long.MIN_VALUE ? max : 0);
    }

    public File getNewSST(Level level) throws IOException {
        Preconditions.checkNotNull(level);
        File file = SSTInfo.newFile(dbFolder.getAbsolutePath(), level, currentFileName.incrementAndGet());
        if (!file.createNewFile()) {
            throw new RuntimeException("Unable to create file");
        }
        return file;
    }

    public synchronized void addSST(Level level, SSTInfo sstInfo) {
        Preconditions.checkNotNull(level);
        Preconditions.checkNotNull(sstInfo);
        table.get(level).add(sstInfo);
        tableSize.put(level, tableSize.get(level) + sstInfo.getFileTorsoSize());
        search.addSSTInfo(sstInfo);
    }

    public synchronized void removeSST(SSTInfo sstInfo)  {
        Preconditions.checkNotNull(sstInfo.getLevel());
        Preconditions.checkNotNull(sstInfo);
        table.get(sstInfo.getLevel()).remove(sstInfo);
        tableSize.put(sstInfo.getLevel(), tableSize.get(sstInfo.getLevel()) - sstInfo.getFileTorsoSize());
        try {
            search.removeSSTInfo(sstInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // todo
//        File obsolete = FileUtil.makeFileObsolete(sstInfo.getSst());
//        if (obsolete == null) {
//            throw new RuntimeException("unable to rename");
//        }
//        if (!obsolete.delete()) {
//            throw new RuntimeException("Unable to delete files");
//        }
    }

//    private List<String> createList() {
//        // todo improve this
//        return new ArrayList<>() {
//            public boolean add(String mt) {
//                int index = Collections.binarySearch(this, mt, (s1, s2) -> {
//                    String[] pi = s1.trim().split(fileSeparatorForSplit);
//                    var thisPi = pi[pi.length - 1].trim().split("_");
//
//                    pi = s2.trim().split(fileSeparatorForSplit);
//                    var providedPi = pi[pi.length - 1].trim().split("_");
//
//                    if (!thisPi[0].equals(providedPi[0])) throw new RuntimeException("level mismatch");
//                    long a = Long.parseLong(providedPi[1].trim().replace(".sst", ""));
//                    long b = Long.parseLong(thisPi[1].trim().replace(".sst", ""));
//                    return Long.compare(a, b);
//                });
//                if (index < 0) index = ~index;
//                super.add(index, mt);
//                return true;
//            }
//        };
//    }


    @Override
    public void close() throws Exception {
        table.values().stream().flatMap(Collection::stream).forEach(each -> {
            try {
                each.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public SortedSet<SSTInfo> getLevelFileList(Level level) {
        return table.get(level);
    }

    public int getCurrentLevelSize(Level level) {
        return tableSize.get(level);
    }

    public byte[] getLastCompactedKey(Level level) {
        if (lastCompactedKV.containsKey(level)) {
            return lastCompactedKV.get(level);
        }
        SortedSet<SSTInfo> sstofLevel = table.get(level);
        if (sstofLevel.isEmpty()) {
            return null;
        }
        return sstofLevel.getLast().getSstKeyRange().getGreatest();
    }

    public synchronized void  saveLastCompactedKey(byte[] last, Level level) {
        lastCompactedKV.put(level, last);
    }
}
