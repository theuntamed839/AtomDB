package util;

import Constants.DBConstant;

import java.io.File;

public class FileUtil {
    public static File createFileAtDirectory(String directoryPath, String fileName) {
        return new File(directoryPath + File.separator + fileName);
    }

    public static boolean makeFileObsolete(File currentFile) {
        if (!currentFile.exists()) {
            return false;
        }
        return currentFile.renameTo(new File(currentFile.getAbsolutePath() + "_" + DBConstant.OBSOLETE));
    }
}
