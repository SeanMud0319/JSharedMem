package top.nontage.jsharedmem.model;

public class MemoryStats {
    public final long totalSize;
    public final long usedSize;
    public final int topicCount;
    public final long createdAt;

    public MemoryStats(long totalSize, long usedSize, int topicCount, long createdAt) {
        this.totalSize = totalSize;
        this.usedSize = usedSize;
        this.topicCount = topicCount;
        this.createdAt = createdAt;
    }

    public double getUsagePercent() {
        return (double) usedSize / totalSize * 100;
    }

    public long getAvailableSize() {
        return totalSize - usedSize;
    }

    @Override
    public String toString() {
        return String.format(
                "MemoryStats{total=%.2f MB, used=%.2f MB (%.1f%%), available=%.2f MB, topics=%d, created=%s}",
                totalSize / (1024.0 * 1024.0),
                usedSize / (1024.0 * 1024.0),
                getUsagePercent(),
                getAvailableSize() / (1024.0 * 1024.0),
                topicCount,
                new java.util.Date(createdAt)
        );
    }
}