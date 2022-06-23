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


import Table.Table;
import Tools.Validate;
import db.DBOptions;

import java.io.File;
import java.time.Instant;
import java.util.List;

import Level.Level;
import util.Util;

/**
 *  todo
 *  need to get the lowest rank sst file from the level to compact
 *  otherwise some files doesnt get compacted
 *
 */

public class Compaction {
    // todo find a optimized solution
    private final static int[] LEVEL_FILES_TO_COMPACT = {
            (int) (2),
            (int) (3),
            (int) (3),
            (int) (3),
            (int) (3),
            (int) (3),
            (int) (3),
            (int) (3)
    };

    private Table table;
    private DBOptions dbOptions;
    public Compaction(DBOptions dbOptions, Table table) {
        this.table = table;
        this.dbOptions = dbOptions;
    }

    private void compactionMaybe0(Level level) throws Exception {
        List<String> levelFiles = table.getLevelList(level);
        switch (level) {
            case LEVEL_ZERO -> {
                if (levelFiles.size() > 3) {
                    doCompaction(level, levelFiles);
                    compactionMaybe0(level.next());
                }
            }
            case LEVEL_ONE -> {
                if (levelFiles.size() >= 10/*count >= 10*/) { // normally 10
                    doCompaction(level, levelFiles);
                    compactionMaybe0(level.next());
                }
            }
            case LEVEL_TWO -> {
                if (levelFiles.size() >= 32/*count >= 10*/) {
                    doCompaction(level, levelFiles);
                    compactionMaybe0(level.next());
                }
            }
            case LEVEL_THREE -> {
                if (levelFiles.size() >= 52/*count >= 10*/) {
                    doCompaction(level, levelFiles);
                    compactionMaybe0(level.next());
                }
            }
        }
    }

    public void compactionMaybe() throws Exception {
        compactionMaybe0(Level.LEVEL_ZERO);
    }

    // todo can be made good
    private String doCompaction(Level level, List<String> levelFiles) throws Exception {
        int numberOfFiles = LEVEL_FILES_TO_COMPACT[(int) Level.toID(level)];

        // important because the list is in decreasing order
        // eg 10, 7, 5, 4, 2, 1
        // you need to compact the oldest to newest, so u need maybe 1, 2
        // thats why this
        List<String> filesToCompact = levelFiles.subList(
                levelFiles.size() - numberOfFiles, levelFiles.size());

        String createdFileName = performCompaction(filesToCompact, level);



        table.removeFiles(level, filesToCompact);
        deleteCompactedFiles(filesToCompact);
        return createdFileName;
    }

    private void deleteCompactedFiles(List<String> levelFiles) {
        for (String file : levelFiles) {
            boolean delete = new File(file).delete();
            if (delete) {
                System.out.println("deleted " + file);
            } else  {
                System.out.println("not deleted " + file);
            }
        }
    }


    private String performCompaction(List<String> files,
                                   Level level) throws Exception {

        // need to be in sync with table creating file path
        var file = new File(dbOptions.getDBfolder() + File.separator +
                level.value() + "_" + (Instant.now().toString().replace(':', '_'))
                + ".inMaking");

        Util.requireTrue(file.createNewFile(), "unable to create file");

        var compactor = new Compactor(files, file, level);
//        System.out.println("running compaction and will be written on " + file);
        compactor.compact();

//        // debug
//        var vali = new Validate(file);
//        vali.isValid();

        String newSST = table.getNewSST(level.next());
        Util.requireTrue(file.renameTo(new File(newSST)), "unable to rename file");

        table.addSST(level.next(), newSST);
        return newSST;
    }
}
