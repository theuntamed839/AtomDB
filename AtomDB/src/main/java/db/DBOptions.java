package db;

import java.io.File;

public final class DBOptions {
    private final String DBfolder;
    private boolean verifyChecksum;
    private final boolean isDBNew;
    public DBOptions(String DBfolder) {
        this.DBfolder = DBfolder;
        this.isDBNew = !new File(DBfolder).isDirectory();
    }

    public String getDBfolder() {
        return DBfolder;
    }

    public boolean isNew() {
        return isDBNew;
    }
}
