package Table;

public class FileNameSizeHelper implements Comparable<FileNameSizeHelper> {
    private String fileName;
    private long fileSize;

    public FileNameSizeHelper(String fileName, long fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    @Override
    public int compareTo(FileNameSizeHelper obj) {
        int sizeComparison = Long.compare(fileSize, obj.fileSize);
        if (sizeComparison == 0) {
            return fileName.compareTo(obj.fileName);
        }
        return sizeComparison;
    }
}
