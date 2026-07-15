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
    public final long dataStartAddress;
    public final long dataCapacity;

    private static final int MSG_HEADER_SIZE = 20;

    public TopicMetadata(int topicId, int regionSize, long writeOffset, long readOffset, long createdAt, long lastAccessAt, long dataStartAddress, long dataCapacity) {
        this.topicId = topicId;
        this.regionSize = regionSize;
        this.writeOffset = writeOffset;
        this.readOffset = readOffset;
        this.createdAt = createdAt;
        this.lastAccessAt = lastAccessAt;
        this.dataStartAddress = dataStartAddress;
        this.dataCapacity = dataCapacity;
        this.pendingBytes = Math.max(0, writeOffset - readOffset);
        this.pendingCount = 0;
        this.regionUsed = Math.min(pendingBytes, regionSize);
        this.regionUsedPercent = regionSize > 0 ? (double) regionUsed / regionSize * 100 : 0;
    }

    public TopicMetadata(int topicId, int regionSize, long writeOffset, long readOffset, long createdAt, long lastAccessAt, long pendingCount, long dataStartAddress, long dataCapacity) {
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
        this.dataStartAddress = dataStartAddress;
        this.dataCapacity = dataCapacity;
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

    private String repeat(String str, int count) {
        if (count <= 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    private String padRight(String str) {
        if (str.length() >= 30) return str;
        return str + repeat(" ", 30 - str.length());
    }

    private String getProgressBar(long used, long total) {
        if (total <= 0) {
            return "[" + repeat(" ", 40) + "]";
        }
        int filled = (int) ((double) used / total * 40);
        filled = Math.min(filled, 40);
        return "[" + repeat("█", filled) + repeat("░", 40 - filled) + "]";
    }

    public String getRingBufferLayout() {
        StringBuilder sb = new StringBuilder();
        int width = 70;

        sb.append("\n");
        sb.append("Ring Buffer Layout - Topic ").append(topicId).append(":\n");
        sb.append("┌").append(repeat("─", width)).append("┐\n");

        String line1 = " Region Size: " + formatBytes(regionSize);
        sb.append("│").append(line1);
        sb.append(repeat(" ", width - line1.length())).append("│\n");

        String line2 = " Data Capacity: " + formatBytes(dataCapacity);
        sb.append("│").append(line2);
        sb.append(repeat(" ", width - line2.length())).append("│\n");

        String line3 = " Write Offset: " + writeOffset;
        sb.append("│").append(line3);
        sb.append(repeat(" ", width - line3.length())).append("│\n");

        String line4 = " Read Offset: " + readOffset;
        sb.append("│").append(line4);
        sb.append(repeat(" ", width - line4.length())).append("│\n");

        String line5 = " Pending Bytes: " + formatBytes(pendingBytes);
        sb.append("│").append(line5);
        sb.append(repeat(" ", width - line5.length())).append("│\n");

        String line6 = " Pending Messages: " + pendingCount;
        sb.append("│").append(line6);
        sb.append(repeat(" ", width - line6.length())).append("│\n");

        String line7 = " Usage: " + String.format("%.1f%%", regionUsedPercent);
        sb.append("│").append(line7);
        sb.append(repeat(" ", width - line7.length())).append("│\n");

        sb.append("├").append(repeat("─", width)).append("┤\n");

        String bar = getProgressBar(regionUsed, dataCapacity) + " " + String.format("%.1f%%", regionUsedPercent);
        sb.append("│ Data: ").append(bar);
        sb.append(repeat(" ", width - 7 - bar.length())).append("│\n");

        sb.append("└").append(repeat("─", width)).append("┘\n");

        return sb.toString();
    }

    public String getRingBufferData() {
        StringBuilder sb = new StringBuilder();
        int width = 70;

        sb.append("\n");
        sb.append("Ring Buffer Data - Topic ").append(topicId).append(":\n");
        sb.append("┌").append(repeat("─", width)).append("┐\n");

        if (pendingCount == 0) {
            sb.append("│ (no messages pending)");
            sb.append(repeat(" ", width - 23)).append("│\n");
        } else {
            sb.append("│ Pending Messages:");
            sb.append(repeat(" ", width - 19)).append("│\n");
            sb.append("├").append(repeat("─", width)).append("┤\n");

            long scanOffset = readOffset;
            int msgIndex = 0;
            int maxDisplay = Math.min((int) pendingCount, 20);

            while (scanOffset < writeOffset && msgIndex < maxDisplay) {
                long pos = dataStartAddress + (scanOffset % dataCapacity);
                int length = top.nontage.jsharedmem.core.UnsafeUtil.getInt(pos);
                long timestamp = top.nontage.jsharedmem.core.UnsafeUtil.getLong(pos + 4);
                long ttl = top.nontage.jsharedmem.core.UnsafeUtil.getLong(pos + 12);

                if (length <= 0 || length > 1024 * 1024) {
                    break;
                }

                long now = System.currentTimeMillis();
                boolean expired = (now - timestamp) > ttl;

                int totalSize = MSG_HEADER_SIZE + length;
                totalSize = (totalSize + 7) & ~7;

                String status = expired ? " [EXPIRED]" : "";
                String msgStr = " #" + msgIndex + " len=" + length + "B" + status;
                sb.append("│").append(padRight(msgStr));
                sb.append("│ timestamp=").append(new Date(timestamp));
                sb.append(" ttl=").append(ttl).append("ms");
                int padding = width - 45 - String.valueOf(new Date(timestamp)).length() - 12 - String.valueOf(ttl).length();
                sb.append(repeat(" ", Math.max(0, padding)));
                sb.append("│\n");

                scanOffset += totalSize;
                msgIndex++;
            }

            if (pendingCount > maxDisplay) {
                sb.append("│ ... and ").append(pendingCount - maxDisplay).append(" more messages");
                int padding = width - 19 - String.valueOf(pendingCount - maxDisplay).length();
                sb.append(repeat(" ", padding));
                sb.append("│\n");
            }
        }

        sb.append("└").append(repeat("─", width)).append("┘\n");

        return sb.toString();
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