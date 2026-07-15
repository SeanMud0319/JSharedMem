package top.nontage.jsharedmem.model;

import java.util.Date;

public class TopicMetadata {
    public final int topicId;
    public final int regionSize;
    public final long writeOffset;
    public final long readOffset;
    public final long createdAt;
    public final long lastAccessAt;
    public final long pendingBytes;
    public final long pendingCount;
    public final long regionUsed;
    public final double regionUsedPercent;

    public TopicMetadata(int topicId, int regionSize, long writeOffset, long readOffset, long createdAt, long lastAccessAt) {
        this.topicId = topicId;
        this.regionSize = regionSize;
        this.writeOffset = writeOffset;
        this.readOffset = readOffset;
        this.createdAt = createdAt;
        this.lastAccessAt = lastAccessAt;
        this.pendingBytes = Math.max(0, writeOffset - readOffset);
        this.pendingCount = 0;
        this.regionUsed = Math.min(pendingBytes, regionSize);
        this.regionUsedPercent = regionSize > 0 ? (double) regionUsed / regionSize * 100 : 0;
    }

    public TopicMetadata(int topicId, int regionSize, long writeOffset, long readOffset, long createdAt, long lastAccessAt, long pendingCount) {
        this.topicId = topicId;
        this.regionSize = regionSize;
        this.writeOffset = writeOffset;
        this.readOffset = readOffset;
        this.createdAt = createdAt;
        this.lastAccessAt = lastAccessAt;
        this.pendingBytes = Math.max(0, writeOffset - readOffset);
        this.pendingCount = pendingCount;
        this.regionUsed = Math.min(pendingBytes, regionSize);
        this.regionUsedPercent = regionSize > 0 ? (double) regionUsed / regionSize * 100 : 0;
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " bytes";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    @Override
    public String toString() {
        return String.format(
                "TopicMetadata{topicId=%d, regionSize=%s, regionUsed=%s (%.1f%%), pendingMsgs=%d, pendingBytes=%s, writeOffset=%d, readOffset=%d, created=%s}",
                topicId,
                formatBytes(regionSize),
                formatBytes(regionUsed),
                regionUsedPercent,
                pendingCount,
                formatBytes(pendingBytes),
                writeOffset,
                readOffset,
                new Date(createdAt)
        );
    }
}