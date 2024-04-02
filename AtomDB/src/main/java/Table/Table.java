package Table;

import Constants.DBConstant;
import Level.Level;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class Table implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private Map<Level, SortedSet<SSTInfo>>  table;
    private int currentFileName = 0;
    private final File dbFolder;
    private final String fileSeparatorForSplit =  Pattern.quote(File.separator);
    private Map<String, BloomFilter<byte[]>> bloomMap;
    public Table(File dbFolder) {
        Preconditions.checkArgument(dbFolder.exists());
        this.dbFolder = dbFolder;
        table = Map.of(Level.LEVEL_ZERO, new TreeSet<SSTInfo>(),
                Level.LEVEL_ONE,         new TreeSet<SSTInfo>(),
                Level.LEVEL_TWO,         new TreeSet<SSTInfo>(),
                Level.LEVEL_THREE,       new TreeSet<SSTInfo>(),
                Level.LEVEL_FOUR,        new TreeSet<SSTInfo>(),
                Level.LEVEL_FIVE,        new TreeSet<SSTInfo>(),
                Level.LEVEL_SIX,         new TreeSet<SSTInfo>(),
                Level.LEVEL_SEVEN,       new TreeSet<SSTInfo>());
        bloomMap = new HashMap<>();
        fillLevels();
    }

    private void fillLevels() {
        String[] fileNames = dbFolder.list();

        if (fileNames.length == 0) return; // new db

        int max = Integer.MIN_VALUE;
        for (String fileName : fileNames) {
            if (!fileName.contains(".sst") || fileName.contains(DBConstant.OBSOLETE)) continue;

            // todo make it neat
            int got = Integer.parseInt(fileName.trim().split("_")[1].trim().replace(".sst", ""));
            max = Math.max(got, max);

            Level level = Level.fromID(fileName.charAt(0) - 48);
            String file = dbFolder + File.separator + fileName;
            SSTInfo sstInfo = SSTFileHelper.getSSTInfo(file);
            table.get(level).add(sstInfo);
            bloomMap.put(file, sstInfo.getBloomFilter());
        }
        currentFileName = max;
    }

    public File getNewSST(Level level) throws IOException {
        Preconditions.checkNotNull(level);

        File file = new File(dbFolder.getAbsolutePath() + File.separator +
                level.value() + "_" + (++currentFileName) + ".sst");
        if (!file.createNewFile()) {
            throw new RuntimeException("Unable to create file");
        }
        return file;
    }

    public void addSST(Level level, SSTInfo sstInfo) {
        Preconditions.checkNotNull(level);
        Preconditions.checkNotNull(sstInfo);
        table.get(level).add(sstInfo);
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

    public void discardFiles(List<String> filesToCompact) {
        table.get(level).removeAll(filesToCompact);
        for (String s : filesToCompact) {
            bloomMap.remove(s);
        }
    }

    public BloomFilter<byte[]> getBloom(String file){
        return bloomMap.get(file);
    }

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
}
