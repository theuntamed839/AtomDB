package Table;

import Level.Level;
import com.google.common.hash.BloomFilter;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

public class Table {
    private Map<Level, List<SSTInfo>>  table;
    private int currentFileName = 0;
    private final File dbFolder;
    private final String fileSeparatorForSplit =  Pattern.quote(File.separator);
    private Map<String, BloomFilter<byte[]>> bloomMap;
    public Table(File dbFolder) {
        this.dbFolder = dbFolder;
        table = Map.of(Level.LEVEL_ZERO, new ArrayList<SSTInfo>(),
                Level.LEVEL_ONE,         new ArrayList<SSTInfo>(),
                Level.LEVEL_TWO,         new ArrayList<SSTInfo>(),
                Level.LEVEL_THREE,       new ArrayList<SSTInfo>(),
                Level.LEVEL_FOUR,        new ArrayList<SSTInfo>(),
                Level.LEVEL_FIVE,        new ArrayList<SSTInfo>(),
                Level.LEVEL_SIX,         new ArrayList<SSTInfo>(),
                Level.LEVEL_SEVEN,       new ArrayList<SSTInfo>());
        bloomMap = new HashMap<>();
        // todo why to fill everything at start
        fillLevels();
    }

    private void fillLevels() {
        String[] fileNames = dbFolder.list();

        if (fileNames.length == 0) return; // new db

        int max = Integer.MIN_VALUE;
        for (String fileName : fileNames) {
            if (!fileName.contains(".sst")) continue;

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

    public String getNewSST(Level level) {
        return dbFolder.getAbsolutePath() + File.separator +
                level.value() + "_" + (++currentFileName) + ".sst";
    }

    public void addSST(Level level, SSTInfo sstInfo) {
        table.get(level).add(sstInfo);
        bloomMap.put(sstInfo.getFileName(), sstInfo.getBloomFilter());
    }

    public List<SSTInfo> getLevelFileList(Level value) {
        return List.copyOf(table.get(value));
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

    public void removeFiles(Level level, List<String> filesToCompact) {
        table.get(level).removeAll(filesToCompact);
        for (String s : filesToCompact) {
            bloomMap.remove(s);
        }
    }

    public BloomFilter<byte[]> getBloom(String file){
        return bloomMap.get(file);
    }
}
