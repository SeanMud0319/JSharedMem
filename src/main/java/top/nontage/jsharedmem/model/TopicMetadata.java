package top.nontage.jsharedmem.model;

public class TopicMetadata {
    public final int topicId;
    public final int regionSize;
    public final long writeSequence;
    public final long readSequence;
    public final long createdAt;
    public final long lastAccessAt;

    public TopicMetadata(int topicId, int regionSize, long writeSeq, long readSeq, long createdAt, long lastAccessAt) {
        this.topicId = topicId;
        this.regionSize = regionSize;
        this.writeSequence = writeSeq;
        this.readSequence = readSeq;
        this.createdAt = createdAt;
        this.lastAccessAt = lastAccessAt;
    }

    public long getPendingCount() {
        return writeSequence - readSequence;
    }

    public double getUsagePercent() {
        long total = writeSequence + readSequence;
        if (total == 0) return 0;
        return (double) (writeSequence - readSequence) / total * 100;
    }

    @Override
    public String toString() {
        return String.format(
                "TopicMetadata{id=%d, size=%d bytes, pending=%d, usage=%.1f%%, created=%s}",
                topicId, regionSize, getPendingCount(), getUsagePercent(),
                new java.util.Date(createdAt)
        );
    }
}