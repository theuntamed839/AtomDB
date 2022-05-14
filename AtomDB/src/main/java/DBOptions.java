public final class DBOptions {
    private final String DBfolder;
    private boolean verifyChecksum;

    public DBOptions(String DBfolder) {
        this.DBfolder = DBfolder;
    }

    public String getDBfolder() {
        return DBfolder;
    }
}
