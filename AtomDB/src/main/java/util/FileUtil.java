package util;

import Constants.DBConstant;

import java.io.File;

public class FileUtil {
    public static File createFileAtDirectory(String directoryPath, String fileName) {
        return new File(directoryPath + File.separator + fileName);
    }

    public static File makeFileObsolete(File currentFile) {
        if (!currentFile.exists()) {
            return null;
        }
        File newFile = new File(currentFile.getAbsolutePath() + "_" + DBConstant.OBSOLETE);
        if (currentFile.renameTo(newFile)) {
           return newFile;
        }
        return null;
    }
}
