package Table;

import Level.Level;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import search.Search;
import util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class Table implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private final Search search;
    private final Map<Level, Integer> tableSize;
    private Map<Level, SortedSet<SSTInfo>>  table;
    private int currentFileName = 0;
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
//        fillLevels();
    }

//    private void fillLevels() {
//        String[] fileNames = dbFolder.list();
//
//        if (fileNames.length == 0) return; // new db
//
//        int max = Integer.MIN_VALUE;
//        for (String fileName : fileNames) {
//            if (!fileName.contains(".sst") || fileName.contains(DBConstant.OBSOLETE)) continue;
//
//            // todo make it neat
//            int got = Integer.parseInt(fileName.trim().split("_")[1].trim().replace(".sst", ""));
//            max = Math.max(got, max);
//
//            Level level = Level.fromID(fileName.charAt(0) - 48);
//            String file = dbFolder + File.separator + fileName;
//            SSTInfo sstInfo = SSTFileHelper.getSSTInfo(file);
//            table.get(level).add(sstInfo);
//        }
//        currentFileName = max;
//    }

    public synchronized File getNewSST(Level level) throws IOException {
        Preconditions.checkNotNull(level);
        File file = SSTInfo.newFile(dbFolder.getAbsolutePath(), level, ++currentFileName);
        if (!file.createNewFile()) {
            throw new RuntimeException("Unable to create file");
        }
        return file;
    }

    public synchronized void addSST(Level level, SSTInfo sstInfo) {
        Preconditions.checkNotNull(level);
        Preconditions.checkNotNull(sstInfo);
        System.out.println("adding="+sstInfo.getSst().getName());
        table.get(level).add(sstInfo);
        tableSize.put(level, tableSize.get(level) + sstInfo.getFileTorsoSize());
        search.addSSTInfo(sstInfo);
    }

    public synchronized void removeSST(SSTInfo sstInfo)  {
        Preconditions.checkNotNull(sstInfo.getLevel());
        Preconditions.checkNotNull(sstInfo);
        System.out.println("removing="+sstInfo.getSst().getName());
        table.get(sstInfo.getLevel()).remove(sstInfo);
        tableSize.put(sstInfo.getLevel(), tableSize.get(sstInfo.getLevel()) - sstInfo.getFileTorsoSize());
        try {
            search.removeSSTInfo(sstInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (FileUtil.makeFileObsolete(sstInfo.getSst()) == null) {
            throw new RuntimeException("unable to rename");
        }
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
        SortedSet<SSTInfo> sstofLevel = table.get(level);
        if (sstofLevel.isEmpty()) {
            return null;
        }
        return sstofLevel.getLast().getSstKeyRange().getLast();
    }
}
