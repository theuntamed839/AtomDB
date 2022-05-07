package Compaction;

/**
 *
 * level 0 -> 4, 4, 4 around 12 size
 *  0 * 2 + 2 = 2 = 2 * 4 = 8
 * level 1 -> 8 * 10 around 100
 *  1 * 2 + 2 = 4 = 4 * 8 = 32
 *  level 2 -> 32 * 32 around 1000
 *  2 * 2 + 2 = 6 = 6 * 32 = 192
 *  level 3 -> 192 * 52 around 10000
 *
 *
 *
 */

import Printer.Checker;
import Table.Table;
import Table.FileNameSizeHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.util.*;
import static Compaction.Level.*;
import static Constants.DBConstants.EOF;

public class Compaction {
    private final String folder;
//    private Map<Integer, SortedMap<String, String>> levels;
//    private Comparator<String> treeComparator = (s1, s2) -> {
//        long s1L = Long.parseLong(s1.substring(0, s1.length() - 26));
//        long s2L = Long.parseLong(s2.substring(0, s1.length() - 26));
//        int compare = Long.compare(s1L, s2L);
//        if (compare == 0) {
//            return s1.compareTo(s2);
//        } else return compare;
//    };

    private Table table;

    public Compaction(Table table) {
        this.table = table;
        this.folder = table.getFolder();
//        this.levels = new HashMap<>();
//        levels.put(0, new TreeMap<>(treeComparator));
//        levels.put(1, new TreeMap<>(treeComparator));
//        levels.put(2, new TreeMap<>(treeComparator));
//        levels.put(3, new TreeMap<>(treeComparator));
//        levels.put(4, new TreeMap<>(treeComparator));
//        levels.put(5, new TreeMap<>(treeComparator));
//        levels.put(6, new TreeMap<>(treeComparator));
//        levels.put(7, new TreeMap<>(treeComparator));
    }

    public void compactionMaybe(Level level) throws Exception {
        SortedSet<FileNameSizeHelper> levelFiles = table.getLevel(level);
        switch (level) {
            case LEVEL_ZERO -> {
                if (levelFiles.size() >= 10) { // normally 10
                    doCompaction(level, levelFiles);
                    compactionMaybe(level.next(level));
                }
            }
            case LEVEL_ONE -> {
//                int count = 0;
//                for (FileNameSizeHelper levelFile : levelFiles) {
//                    if (levelFile.getFileSize() < ) {
//                        count++;
//                    }
//                }
                if (levelFiles.size() >= 10/*count >= 10*/) { // normally 10
                    doCompaction(level, levelFiles);
                    compactionMaybe(level.next(level));
                }
            }
            case LEVEL_TWO -> {
//                int count = 0;
//                for (FileNameSizeHelper levelFile : levelFiles) {
//                    if (levelFile.getFileSize() < 60000) {
//                        count++;
//                    }
//                }
                if (levelFiles.size() >= 32/*count >= 10*/) {
                    doCompaction(level, levelFiles);
                }
            }
            case LEVEL_THREE -> {
//                int count = 0;
//                for (FileNameSizeHelper levelFile : levelFiles) {
//                    if (levelFile.getFileSize() < 60000) {
//                        count++;
//                    }
//                }
                if (levelFiles.size() >= 52/*count >= 10*/) {
                    doCompaction(level, levelFiles);
                }
            }
        }
    }

    public void compactionMaybe() throws Exception {
        compactionMaybe(LEVEL_ZERO);
    }

    private String doCompaction(Level level, SortedSet<FileNameSizeHelper> levelFiles) throws Exception {
        List<String> filesToCompact = getFilesToCompact(levelFiles, level.value() * 2 + 2);
        String createdFileName = performCompaction(filesToCompact, level);
        deleteCompactedFiles(levelFiles, level.value() * 2 + 2);
        return createdFileName;
    }

    private List<String> getFilesToCompact(SortedSet<FileNameSizeHelper> levelFiles,
                                           int fileCount) {
        List<String> list = new ArrayList<>(fileCount);
        for (FileNameSizeHelper levelFile : levelFiles) {
            list.add(levelFile.getFileName());
        }
        return list;
    }

    private void deleteCompactedFiles(SortedSet<FileNameSizeHelper> levelFiles,
                                           int fileCount) {
        List<FileNameSizeHelper> list = new ArrayList<>(fileCount);
        list.addAll(levelFiles);
        for (FileNameSizeHelper fileNameSizeHelper : list) {
            boolean delete = new File(folder +
                    System.getProperty("file.separator") + fileNameSizeHelper.getFileName()).delete();
            if (delete) {
                System.out.println("deleted " + fileNameSizeHelper.getFileName());
                levelFiles.remove(fileNameSizeHelper);
            } else  {
                System.out.println("not deleted " + fileNameSizeHelper.getFileName());
            }
        }
    }

//    private void addToFirstLevel(String fileName, long fileSize) {
//        fileCount += 1;
//        String size = fileSize + LocalDateTime.now().toString();
//        System.out.println(levels.getOrDefault(LEVEL_ZERO, new TreeMap<>()).keySet());
//        levels.get(LEVEL_ZERO.value()).put(size, fileName);
//        System.out.println(size);
//    }

    private String performCompaction(List<String> files,
                                   Level level) throws Exception {
        var compactor = new Compactor(folder , files);
        var file = table.getNewFile(level.next(level));
        System.out.println("running compaction and will be written on " + file.getName());
        long createdFileSize = compactor.compact(
                new FileOutputStream(file).getChannel());
        System.out.println(file.getName() + " created " + createdFileSize);
        table.put(level.next(level), createdFileSize, file.getName());

//        FileChannel channel = new FileInputStream(file).getChannel();
//        new Checker(channel);
//        channel.close();

        return file.getName();
    }

    private void deleteCompactedFiles(List<String> files) {
        for (String file : files) {
            boolean delete = new File(folder +
                    System.getProperty("file.separator") + file).delete();
            if (delete) {
                System.out.println("deleted " + file);
            } else  {
                System.out.println("not deleted " + file);
            }
        }
    }
}
