public class FileProgressManager {
    /* Variables */
    long chunk_Size;
    long remainingFileSize;

    // Getters & Setters
    public long getChunk_Size() {
        return chunk_Size;
    }

    public void setChunk_Size(long chunk_Size) {
        this.chunk_Size = chunk_Size;
    }

    public long getRemainingFileSize() {
        return remainingFileSize;
    }

    public void setRemainingFileSize(long remainingFileSize) {
        this.remainingFileSize = remainingFileSize;
    }
}
