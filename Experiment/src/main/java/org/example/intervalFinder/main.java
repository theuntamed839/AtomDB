package org.example.intervalFinder;

import java.util.*;

public class main {
    static int fileCount = 0;
    public static void main(String[] args) {
        var intervalTable = new TreeSet<Point>((a, b) -> Arrays.compare(a.arr, b.arr));
        var batch1 = new SST[] {fileGenerator(), fileGenerator(), fileGenerator(), fileGenerator()};
        var compaction1 = fileCompaction(batch1);
        var batch2 = new SST[] {fileGenerator(), fileGenerator(), fileGenerator(), fileGenerator()};
        var compaction2 = fileCompaction(batch2);
        var batch3 = new SST[] {fileGenerator(), fileGenerator(), fileGenerator(), fileGenerator()};
        var compaction3 = fileCompaction(batch3);

        List<SST> currentFileList = new ArrayList<>();
        Cycle(intervalTable, batch1, compaction1, currentFileList);
        Cycle(intervalTable, batch2, compaction2, currentFileList);
        Cycle(intervalTable, batch3, compaction3, currentFileList);
    }

    private static void Cycle(TreeSet<Point> intervalTable, SST[] batch, SST compaction, List<SST> currentFileList) {
        currentFileList.add(batch[0]);
        addToIntervalTable(currentFileList, batch[0], intervalTable);

        currentFileList.add(batch[1]);
        addToIntervalTable(currentFileList, batch[1], intervalTable);

        currentFileList.add(batch[2]);
        addToIntervalTable(currentFileList, batch[2], intervalTable);

        currentFileList.add(batch[3]);
        addToIntervalTable(currentFileList, batch[3], intervalTable);

        currentFileList.removeAll(List.of(batch));
        currentFileList.add(compaction);
        addToIntervalTable(currentFileList, compaction, intervalTable);
    }

    static void addToIntervalTable(List<SST> currentFileList, SST FileToBeAdded, TreeSet<Point> intervalTable) {
        for (byte[] key : FileToBeAdded.keys) {
            Point point = new Point(key);
            for (SST sst : currentFileList) {
                byte[] first = sst.keys.first();
                byte[] last = sst.keys.last();
                if (Arrays.compare(key, first) >= 0 && Arrays.compare(key, last) <= 0) {
                    point.listOfFilesOccurrence.add(sst.file);
                }
            }
            intervalTable.add(point);
        }
    }

    static SST fileGenerator() {
        var rand = new Random();
        int numberOfEntries = rand.nextInt(20_000, 50_000);
        var file = new SST("file_" + fileCount++, new TreeSet<>(Arrays::compare));
        var list = file.keys;
        for (int i = 0; i < numberOfEntries; i++) {
            var arr = new byte[256];
            rand.nextBytes(arr);
            list.add(arr);
        }
        return file;
    }

    static SST fileCompaction(SST[] files) {
        var set = new TreeSet<byte[]>(Arrays::compare);
        for (SST stringSetEntry : files) {
            set.addAll(stringSetEntry.keys);
        }
        return new SST("file_" + fileCount++, set);
    }
}

class Point {
    byte[] arr;
    List<String> listOfFilesOccurrence;

    public Point(byte[] arr) {
        this.arr = arr;
        this.listOfFilesOccurrence = new ArrayList<>();
    }
}

class SST {
    String file;
    TreeSet<byte[]> keys;

    public SST(String file, TreeSet<byte[]> keys) {
        this.file = file;
        this.keys = keys;
    }
}