package Table;

import Compaction.Level;

import java.io.File;
import java.util.*;

public class Table {
    private Map<Integer, SortedSet<FileNameSizeHelper>> levels;
    private int fileCount = 0;
    private String folder;
    private Comparator<String> treeComparator = (s1, s2) -> {
        long s1L = Long.parseLong(s1.substring(0, s1.length() - 26));
        long s2L = Long.parseLong(s2.substring(0, s1.length() - 26));
        int compare = Long.compare(s1L, s2L);
        if (compare == 0) {
            return s1.compareTo(s2);
        } else return compare;
    };

    public int getFileCount() {
        return fileCount;
    }

    private static Table table = null;

    public static Table getTable(String folder) {
        if (table == null) {
            table = new Table(folder);
        }
        return table;
    }

    private Table(String folder) {
        this.folder = folder;
        this.levels = new HashMap<>();
        levels.put(0, new TreeSet<>());
        levels.put(1, new TreeSet<>());
        levels.put(2, new TreeSet<>());
        levels.put(3, new TreeSet<>());
        levels.put(4, new TreeSet<>());
        levels.put(5, new TreeSet<>());
        levels.put(6, new TreeSet<>());
        levels.put(7, new TreeSet<>());
    }

    public void put(Level level, long fileSize, String fileName) {
        levels.get(level.value())
                .add(new FileNameSizeHelper(fileName, fileSize));
    }

    public SortedSet<FileNameSizeHelper> getLevel(Level level) {
        return levels.get(level.value());
    }

    public void putInInitialLevel(long fileSize, String fileName) {
        levels.get(Level.LEVEL_ZERO.value())
                .add(new FileNameSizeHelper(fileName, fileSize));
    }

    public void removeFile(Level level, FileNameSizeHelper obj) {
        levels.get(level.value()).remove(obj);
    }

    public String getFolder() {
        return folder;
    }

    public void removeFile(Level level, long fileSize, String fileName) {
        levels.get(level.value())
                .remove(new FileNameSizeHelper(fileName, fileSize));
    }

    public File getNewFile(Level level) {
        fileCount++;
        return new File(folder
                + System.getProperty("file.separator") +
                ("LEVEL-"+ level.value() +"-" + fileCount + ".sst"));
    }

    public Iterator<String> getFileIterator() {
        return levels.values().stream()
                .flatMap(Collection::stream)
                .map(FileNameSizeHelper::getFileName)
                .iterator();
    }
}
