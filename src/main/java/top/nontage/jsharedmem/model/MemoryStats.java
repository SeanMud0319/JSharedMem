package top.nontage.jsharedmem.model;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStats {
    public final long arenaSize;
    public final long arenaUsed;
    public final long regionSize;
    public final long regionUsed;
    public final long topicDataBytes;
    public final long topicDataCount;
    public final int topicCount;
    public final long createdAt;
    public final Map<Integer, TopicMetadata> topicDetails;
    public final Map<String, Integer> stringTopicMap;

    private static final long GLOBAL_METADATA_SIZE = 1024;
    private static final long STRING_MAP_SIZE = 32 * 64;
    private static final long PER_REGION_OVERHEAD = 40 + 32 * 64;

    public MemoryStats(long arenaSize, long arenaUsed, long regionSize, long regionUsed,
                       long topicDataBytes, long topicDataCount, int topicCount, long createdAt,
                       Map<Integer, TopicMetadata> topicDetails, Map<String, Integer> stringTopicMap) {
        this.arenaSize = arenaSize;
        this.arenaUsed = arenaUsed;
        this.regionSize = regionSize;
        this.regionUsed = regionUsed;
        this.topicDataBytes = topicDataBytes;
        this.topicDataCount = topicDataCount;
        this.topicCount = topicCount;
        this.createdAt = createdAt;
        this.topicDetails = topicDetails != null ? topicDetails : new ConcurrentHashMap<>();
        this.stringTopicMap = stringTopicMap != null ? stringTopicMap : new ConcurrentHashMap<>();
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

    private String formatBytesSimple(long bytes) {
        if (bytes < 1024) {
            return bytes + "bytes";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1fKB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1fMB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    private String repeat(String str, int count) {
        if (count <= 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    private String padRight(String str, int length) {
        if (str.length() >= length) return str;
        return str + repeat(" ", length - str.length());
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

    private String getProgressBar(long used, long total) {
        if (total <= 0) {
            return "[" + repeat(" ", 35) + "]";
        }
        int filled = (int) ((double) used / total * 35);
        filled = Math.min(filled, 35);
        return "[" + repeat("█", filled) + repeat("░", 35 - filled) + "]";
    }

    public String getMemoryLayout() {
        StringBuilder sb = new StringBuilder();
        int width = 74;
        sb.append("\n");
        sb.append("Memory Layout (").append(formatBytes(arenaSize)).append("):\n");
        sb.append("┌").append(repeat("─", width)).append("┐\n");

        long remaining = arenaSize;

        long metaSize = Math.min(GLOBAL_METADATA_SIZE, remaining);
        if (metaSize > 0) {
            String label = " Global Metadata";
            String sizeStr = formatBytesSimple(metaSize);
            sb.append("│").append(label);
            sb.append(repeat(" ", width - label.length() - sizeStr.length()));
            sb.append(sizeStr).append(" │\n");
            sb.append("│   Magic: 4 bytes");
            sb.append(repeat(" ", width - 21)).append("│\n");
            sb.append("│   Version: 4 bytes");
            sb.append(repeat(" ", width - 23)).append("│\n");
            sb.append("│   TotalSize: 8 bytes");
            sb.append(repeat(" ", width - 24)).append("│\n");
            sb.append("│   UsedSize: 8 bytes");
            sb.append(repeat(" ", width - 23)).append("│\n");
            sb.append("│   TopicCount: 4 bytes");
            sb.append(repeat(" ", width - 25)).append("│\n");
            sb.append("│   CreatedAt: 8 bytes");
            sb.append(repeat(" ", width - 24)).append("│\n");
            sb.append("│   Reserved: 56 bytes");
            sb.append(repeat(" ", width - 23)).append("│\n");
            remaining -= metaSize;
        }

        long stringMapSize = Math.min(STRING_MAP_SIZE, remaining);
        if (stringMapSize > 0) {
            sb.append("├").append(repeat("─", width)).append("┤\n");
            String label = " String Map (" + stringTopicMap.size() + " entries)";
            String sizeStr = formatBytesSimple(stringMapSize);
            sb.append("│").append(label);
            sb.append(repeat(" ", width - label.length() - sizeStr.length()));
            sb.append(sizeStr).append(" │\n");

            if (stringTopicMap.isEmpty()) {
                sb.append("│   (empty)");
                sb.append(repeat(" ", width - 11)).append("│\n");
            } else {
                int count = 0;
                for (Map.Entry<String, Integer> entry : stringTopicMap.entrySet()) {
                    if (count >= 5) break;
                    String displayName = entry.getKey().length() > 20 ? entry.getKey().substring(0, 17) + "..." : entry.getKey();
                    String entryStr = "   " + displayName + " -> " + entry.getValue();
                    sb.append("│").append(entryStr);
                    sb.append(repeat(" ", width - entryStr.length()));
                    sb.append("│\n");
                    count++;
                }
                if (stringTopicMap.size() > 5) {
                    String moreStr = "   ... " + (stringTopicMap.size() - 5) + " more entries";
                    sb.append("│").append(moreStr);
                    sb.append(repeat(" ", width - moreStr.length()));
                    sb.append("│\n");
                }
            }
            remaining -= stringMapSize;
        }

        if (topicCount > 0) {
            long regionTotal = 0;
            long regionDataUsed = 0;
            long regionOverhead = 0;
            long regionDataCapacity = 0;

            for (TopicMetadata meta : topicDetails.values()) {
                regionTotal += meta.regionSize;
                regionDataUsed += meta.regionUsed;
                regionOverhead += PER_REGION_OVERHEAD;
                regionDataCapacity += meta.regionSize - PER_REGION_OVERHEAD;
            }

            sb.append("├").append(repeat("─", width)).append("┤\n");
            String label = " Regions (" + topicCount + " topics)";
            String sizeStr = formatBytesSimple(regionTotal);
            sb.append("│").append(label);
            sb.append(repeat(" ", width - label.length() - sizeStr.length()));
            sb.append(sizeStr).append(" │\n");

            String capStr = "   Data Capacity: " + formatBytes(regionDataCapacity);
            sb.append("│").append(capStr);
            sb.append(repeat(" ", width - capStr.length())).append("│\n");

            String usedStr = "   Data Used: " + formatBytes(regionDataUsed);
            sb.append("│").append(usedStr);
            sb.append(repeat(" ", width - usedStr.length())).append("│\n");

            String barStr = "   Data: " + getProgressBar(regionDataUsed, regionDataCapacity) + " " + String.format("%.1f%%", (double) regionDataUsed / regionDataCapacity * 100);
            sb.append("│").append(barStr);
            sb.append(repeat(" ", width - barStr.length())).append("│\n");

            String overheadStr = "   Overhead: " + formatBytes(regionOverhead) + " (Header: 40 bytes + SubscriberTable: " + (topicCount * 32 * 64) + " bytes)";
            sb.append("│").append(overheadStr);
            sb.append(repeat(" ", width - overheadStr.length())).append("│\n");

            if (!topicDetails.isEmpty()) {
                for (TopicMetadata meta : topicDetails.values()) {
                    long topicDataCapacity = meta.regionSize - PER_REGION_OVERHEAD;
                    String topicLabel = "   Topic (Id) " + meta.topicId + ":";
                    sb.append("│").append(topicLabel);
                    sb.append(repeat(" ", width - topicLabel.length())).append("│\n");

                    String capStr2 = "     Region Size: " + formatBytes(meta.regionSize);
                    sb.append("│").append(capStr2);
                    sb.append(repeat(" ", width - capStr2.length())).append("│\n");

                    String capStr3 = "     Data Capacity: " + formatBytes(topicDataCapacity);
                    sb.append("│").append(capStr3);
                    sb.append(repeat(" ", width - capStr3.length())).append("│\n");

                    String usedStr2 = "     Data Used: " + formatBytes(meta.regionUsed);
                    sb.append("│").append(usedStr2);
                    sb.append(repeat(" ", width - usedStr2.length())).append("│\n");

                    String msgStr = "     Messages: " + meta.pendingCount;
                    sb.append("│").append(msgStr);
                    sb.append(repeat(" ", width - msgStr.length())).append("│\n");

                    String barStr2 = "     " + getProgressBar(meta.regionUsed, topicDataCapacity) + " " + String.format("%.1f%%", meta.regionUsedPercent);
                    sb.append("│").append(barStr2);
                    sb.append(repeat(" ", width - barStr2.length())).append("│\n");
                }
            }

            remaining -= regionTotal;
        }

        if (remaining > 0) {
            sb.append("├").append(repeat("─", width)).append("┤\n");
            String label = " Free Space";
            String sizeStr = formatBytesSimple(remaining);
            sb.append("│").append(label);
            sb.append(repeat(" ", width - label.length() - sizeStr.length()));
            sb.append(sizeStr).append(" │\n");
        }

        sb.append("└").append(repeat("─", width)).append("┘\n");

        sb.append("\n");
        sb.append("┌").append(repeat("─", width)).append("┐\n");
        sb.append("│ Detailed Breakdown");
        sb.append(repeat(" ", width - 20)).append("│\n");
        sb.append("├").append(repeat("─", width)).append("┤\n");

        long globalMeta = Math.min(GLOBAL_METADATA_SIZE, arenaSize);
        long stringMap = Math.min(STRING_MAP_SIZE, Math.max(0, arenaSize - GLOBAL_METADATA_SIZE));
        long regionTotal = 0;
        for (TopicMetadata meta : topicDetails.values()) {
            regionTotal += meta.regionSize;
        }
        long freeSpace = Math.max(0, arenaSize - arenaUsed);
        long overheadTotal = topicCount * PER_REGION_OVERHEAD;
        long regionDataTotal = regionTotal - overheadTotal;

        String h1 = " Component";
        String h2 = " Size";
        String h3 = " Percentage";
        String h4 = " Status";
        sb.append("│").append(padRight(h1, 26));
        sb.append("│").append(padRight(h2, 14));
        sb.append("│").append(padRight(h3, 12));
        sb.append("│").append(padRight(h4, 10));
        sb.append("│\n");
        sb.append("├").append(repeat("─", width)).append("┤\n");

        String r1 = " Global Metadata";
        sb.append("│").append(padRight(r1, 26));
        sb.append("│").append(padRight(formatBytesSimple(globalMeta), 14));
        sb.append("│").append(padRight(String.format("%.1f%%", (double) globalMeta / arenaSize * 100), 12));
        sb.append("│").append(padRight("Fixed", 10));
        sb.append("│\n");

        String r2 = " String Map";
        sb.append("│").append(padRight(r2, 26));
        sb.append("│").append(padRight(formatBytesSimple(stringMap), 14));
        sb.append("│").append(padRight(String.format("%.1f%%", (double) stringMap / arenaSize * 100), 12));
        sb.append("│").append(padRight("Fixed", 10));
        sb.append("│\n");

        String r3 = " Region Data";
        sb.append("│").append(padRight(r3, 26));
        sb.append("│").append(padRight(formatBytesSimple(regionDataTotal), 14));
        sb.append("│").append(padRight(String.format("%.1f%%", (double) regionDataTotal / arenaSize * 100), 12));
        sb.append("│").append(padRight(regionDataTotal > 0 ? "Usable" : "N/A", 10));
        sb.append("│\n");

        String r4 = " Region Overhead";
        sb.append("│").append(padRight(r4, 26));
        sb.append("│").append(padRight(formatBytesSimple(overheadTotal), 14));
        sb.append("│").append(padRight(String.format("%.1f%%", (double) overheadTotal / arenaSize * 100), 12));
        sb.append("│").append(padRight("Overhead", 10));
        sb.append("│\n");

        String r5 = " Free Space";
        sb.append("│").append(padRight(r5, 26));
        sb.append("│").append(padRight(formatBytesSimple(freeSpace), 14));
        sb.append("│").append(padRight(String.format("%.1f%%", (double) freeSpace / arenaSize * 100), 12));
        sb.append("│").append(padRight("Available", 10));
        sb.append("│\n");
        sb.append("└").append(repeat("─", width)).append("┘\n");

        sb.append("\n");
        sb.append("Summary:\n");
        sb.append("  Arena: ").append(formatBytes(arenaSize));
        sb.append(" (Used: ").append(formatBytes(arenaUsed));
        sb.append(", ").append(String.format("%.1f%%", getArenaUsagePercent()));
        sb.append(", Free: ").append(formatBytes(getArenaAvailable())).append(")\n");
        sb.append("  Topics: ").append(topicCount).append(" (max 32)\n");
        sb.append("  Messages: ").append(topicDataCount);
        sb.append(" (").append(formatBytes(topicDataBytes)).append(")\n");
        sb.append("  Avg Message Size: ").append(topicDataCount > 0 ?
                formatBytes(topicDataBytes / topicDataCount) : "0 bytes").append("\n");
        sb.append("  Created: ").append(new Date(createdAt)).append("\n");

        return sb.toString();
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