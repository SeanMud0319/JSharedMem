package top.nontage.jsharedmem.model;

import java.util.Date;

public class MemoryStats {
    public final long arenaSize;
    public final long arenaUsed;
    public final long regionSize;
    public final long regionUsed;
    public final long topicDataBytes;
    public final long topicDataCount;
    public final int topicCount;
    public final long createdAt;

    public MemoryStats(long arenaSize, long arenaUsed, long regionSize, long regionUsed, long topicDataBytes, long topicDataCount, int topicCount, long createdAt) {
        this.arenaSize = arenaSize;
        this.arenaUsed = arenaUsed;
        this.regionSize = regionSize;
        this.regionUsed = regionUsed;
        this.topicDataBytes = topicDataBytes;
        this.topicDataCount = topicDataCount;
        this.topicCount = topicCount;
        this.createdAt = createdAt;
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

    public double getArenaUsagePercent() {
        return arenaSize > 0 ? (double) arenaUsed / arenaSize * 100 : 0;
    }

    public double getRegionUsagePercent() {
        return regionSize > 0 ? (double) regionUsed / regionSize * 100 : 0;
    }

    public long getArenaAvailable() {
        return arenaSize - arenaUsed;
    }

    @Override
    public String toString() {
        return String.format(
                "MemoryStats{arena=%s, arenaUsed=%s (%.1f%%), region=%s, regionUsed=%s (%.1f%%), topicData=%s (%d msgs), topics=%d, created=%s}",
                formatBytes(arenaSize),
                formatBytes(arenaUsed),
                getArenaUsagePercent(),
                formatBytes(regionSize),
                formatBytes(regionUsed),
                getRegionUsagePercent(),
                formatBytes(topicDataBytes),
                topicDataCount,
                topicCount,
                new Date(createdAt)
        );
    }
}