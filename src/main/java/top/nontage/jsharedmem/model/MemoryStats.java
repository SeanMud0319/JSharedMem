package top.nontage.jsharedmem.model;

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

    public double getArenaUsagePercent() {
        return (double) arenaUsed / arenaSize * 100;
    }

    public double getRegionUsagePercent() {
        return (double) regionUsed / regionSize * 100;
    }

    public long getArenaAvailable() {
        return arenaSize - arenaUsed;
    }

    @Override
    public String toString() {
        return String.format(
                "MemoryStats{arena=%.2f MB, arenaUsed=%.2f MB (%.1f%%), region=%.2f MB, regionUsed=%.2f MB (%.1f%%), topicData=%.2f MB (%d msgs), topics=%d, created=%s}",
                arenaSize / (1024.0 * 1024.0),
                arenaUsed / (1024.0 * 1024.0),
                getArenaUsagePercent(),
                regionSize / (1024.0 * 1024.0),
                regionUsed / (1024.0 * 1024.0),
                getRegionUsagePercent(),
                topicDataBytes / (1024.0 * 1024.0),
                topicDataCount,
                topicCount,
                new java.util.Date(createdAt)
        );
    }
}