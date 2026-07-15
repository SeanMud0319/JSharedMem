package top.nontage.jsharedmem.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.nontage.jsharedmem.exception.MemoryFullException;
import top.nontage.jsharedmem.exception.SharedMemoryException;
import top.nontage.jsharedmem.model.MemoryStats;
import top.nontage.jsharedmem.model.TopicMetadata;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static top.nontage.jsharedmem.core.UnsafeUtil.*;

public class MemoryManager {

    private static final Logger logger = LoggerFactory.getLogger(MemoryManager.class);

    private static final int GLOBAL_METADATA_SIZE = 1024;
    private static final int MAGIC_OFFSET = 0;
    private static final int VERSION_OFFSET = 4;
    private static final int TOTAL_SIZE_OFFSET = 8;
    private static final int USED_SIZE_OFFSET = 16;
    private static final int TOPIC_COUNT_OFFSET = 24;
    private static final int CREATED_AT_OFFSET = 32;

    private static final int STRING_MAP_START = 128;
    private static final int MAX_STRING_TOPICS = 32;
    private static final int STRING_ENTRY_SIZE = 64;
    private static final int STRING_ENTRY_ID_OFFSET = 0;
    private static final int STRING_ENTRY_NAME_OFFSET = 4;
    private static final int STRING_ENTRY_NAME_LENGTH = 56;

    private static final int REGION_TOPIC_ID_OFFSET = 0;
    private static final int REGION_SIZE_OFFSET = 4;
    private static final int REGION_WRITE_SEQ_OFFSET = 8;
    private static final int REGION_READ_SEQ_OFFSET = 16;
    private static final int REGION_CREATED_AT_OFFSET = 24;
    private static final int REGION_LAST_ACCESS_OFFSET = 32;

    private final ConcurrentHashMap<Integer, TopicRingBuffer> topicCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> stringTopicCache = new ConcurrentHashMap<>();

    private final long baseAddress;
    private final long totalSize;
    private final long dataStart;

    private volatile boolean closed = false;

    private long defaultRegionSize = 64 * 1024;

    public MemoryManager(long baseAddress, long totalSize) {
        if (baseAddress == 0L) {
            throw new IllegalStateException("baseAddress is 0 - shared memory not initialized");
        }
        this.baseAddress = baseAddress;
        this.totalSize = totalSize;
        this.dataStart = baseAddress + 1024L;

        int magic = UnsafeUtil.getInt(baseAddress);
        if (magic != 4870989) {
            this.initGlobalMetadata();
            logger.info("Initialized new shared memory");
        } else {
            logger.info("Connected to existing shared memory (version: {})", UnsafeUtil.getInt(baseAddress + 4L));
            this.loadStringTopicCache();

            long usedSize = UnsafeUtil.getLong(this.baseAddress + 16L);
            if (usedSize < 0 || usedSize > this.totalSize) {
                logger.warn("Invalid usedSize: {}, resetting to 0", usedSize);
                UnsafeUtil.putLong(this.baseAddress + 16L, 0L);
            }
        }
    }

    public void setDefaultRegionSize(long size) {
        this.defaultRegionSize = size;
    }

    private void initGlobalMetadata() {
        putInt(baseAddress + MAGIC_OFFSET, 0x4A534D);
        putInt(baseAddress + VERSION_OFFSET, 1);
        putLong(baseAddress + TOTAL_SIZE_OFFSET, totalSize);
        putLong(baseAddress + USED_SIZE_OFFSET, 0);
        putInt(baseAddress + TOPIC_COUNT_OFFSET, 0);
        putLong(baseAddress + CREATED_AT_OFFSET, System.currentTimeMillis());
        setMemory(baseAddress + 64, GLOBAL_METADATA_SIZE - 64, (byte) 0);
        setMemory(baseAddress + STRING_MAP_START, MAX_STRING_TOPICS * STRING_ENTRY_SIZE, (byte) 0);
    }

    private void loadStringTopicCache() {
        stringTopicCache.clear();
        for (int i = 0; i < MAX_STRING_TOPICS; i++) {
            long entryAddr = baseAddress + STRING_MAP_START + (i * STRING_ENTRY_SIZE);
            int topicId = getInt(entryAddr + STRING_ENTRY_ID_OFFSET);
            if (topicId == 0) continue;

            byte[] nameBytes = new byte[STRING_ENTRY_NAME_LENGTH];
            for (int j = 0; j < STRING_ENTRY_NAME_LENGTH; j++) {
                nameBytes[j] = getByte(entryAddr + STRING_ENTRY_NAME_OFFSET + j);
            }
            String name = new String(nameBytes, StandardCharsets.UTF_8).trim();
            if (!name.isEmpty()) {
                stringTopicCache.put(name, topicId);
                logger.debug("Loaded String topic mapping: {} -> {}", name, topicId);
            }
        }
    }

    private synchronized int getOrCreateStringTopicId(String topic) {
        Integer cached = stringTopicCache.get(topic);
        if (cached != null) {
            return cached;
        }

        for (int i = 0; i < MAX_STRING_TOPICS; i++) {
            long entryAddr = baseAddress + STRING_MAP_START + (i * STRING_ENTRY_SIZE);
            int topicId = getInt(entryAddr + STRING_ENTRY_ID_OFFSET);
            if (topicId == 0) continue;

            byte[] nameBytes = new byte[STRING_ENTRY_NAME_LENGTH];
            for (int j = 0; j < STRING_ENTRY_NAME_LENGTH; j++) {
                nameBytes[j] = getByte(entryAddr + STRING_ENTRY_NAME_OFFSET + j);
            }
            String existing = new String(nameBytes, StandardCharsets.UTF_8).trim();
            if (existing.equals(topic)) {
                stringTopicCache.put(topic, topicId);
                return topicId;
            }
        }

        int newId = getInt(baseAddress + TOPIC_COUNT_OFFSET) + 1;
        if (newId > 32) {
            throw new MemoryFullException("Maximum topics exceeded: " + newId);
        }

        for (int i = 0; i < MAX_STRING_TOPICS; i++) {
            long entryAddr = baseAddress + STRING_MAP_START + (i * STRING_ENTRY_SIZE);
            if (getInt(entryAddr + STRING_ENTRY_ID_OFFSET) == 0) {
                putInt(entryAddr + STRING_ENTRY_ID_OFFSET, newId);

                byte[] nameBytes = topic.getBytes(StandardCharsets.UTF_8);
                int nameLen = Math.min(nameBytes.length, STRING_ENTRY_NAME_LENGTH);
                for (int j = 0; j < nameLen; j++) {
                    putByte(entryAddr + STRING_ENTRY_NAME_OFFSET + j, nameBytes[j]);
                }

                for (int j = nameLen; j < STRING_ENTRY_NAME_LENGTH; j++) {
                    putByte(entryAddr + STRING_ENTRY_NAME_OFFSET + j, (byte) 0);
                }

                stringTopicCache.put(topic, newId);
                logger.info("Created String topic mapping: {} -> {}", topic, newId);
                return newId;
            }
        }

        throw new MemoryFullException("String topic map is full (max " + MAX_STRING_TOPICS + ")");
    }

    public int getTopicId(String topic) {
        Integer cached = stringTopicCache.get(topic);
        if (cached != null) {
            return cached;
        }

        for (int i = 0; i < MAX_STRING_TOPICS; i++) {
            long entryAddr = baseAddress + STRING_MAP_START + (i * STRING_ENTRY_SIZE);
            int topicId = getInt(entryAddr + STRING_ENTRY_ID_OFFSET);
            if (topicId == 0) continue;

            byte[] nameBytes = new byte[STRING_ENTRY_NAME_LENGTH];
            for (int j = 0; j < STRING_ENTRY_NAME_LENGTH; j++) {
                nameBytes[j] = getByte(entryAddr + STRING_ENTRY_NAME_OFFSET + j);
            }
            String existing = new String(nameBytes, StandardCharsets.UTF_8).trim();
            if (existing.equals(topic)) {
                stringTopicCache.put(topic, topicId);
                return topicId;
            }
        }
        return -1;
    }

    public String getTopicName(int topicId) {
        for (int i = 0; i < MAX_STRING_TOPICS; i++) {
            long entryAddr = baseAddress + STRING_MAP_START + (i * STRING_ENTRY_SIZE);
            int existingId = getInt(entryAddr + STRING_ENTRY_ID_OFFSET);
            if (existingId == topicId) {
                byte[] nameBytes = new byte[STRING_ENTRY_NAME_LENGTH];
                for (int j = 0; j < STRING_ENTRY_NAME_LENGTH; j++) {
                    nameBytes[j] = getByte(entryAddr + STRING_ENTRY_NAME_OFFSET + j);
                }
                String name = new String(nameBytes, StandardCharsets.UTF_8).trim();
                return name.isEmpty() ? null : name;
            }
        }
        return null;
    }

    public List<String> getStringTopicList() {
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < MAX_STRING_TOPICS; i++) {
            long entryAddr = baseAddress + STRING_MAP_START + (i * STRING_ENTRY_SIZE);
            int topicId = getInt(entryAddr + STRING_ENTRY_ID_OFFSET);
            if (topicId == 0) continue;

            byte[] nameBytes = new byte[STRING_ENTRY_NAME_LENGTH];
            for (int j = 0; j < STRING_ENTRY_NAME_LENGTH; j++) {
                nameBytes[j] = getByte(entryAddr + STRING_ENTRY_NAME_OFFSET + j);
            }
            String name = new String(nameBytes, StandardCharsets.UTF_8).trim();
            if (!name.isEmpty()) {
                topics.add(name);
            }
        }
        return topics;
    }

    public TopicRingBuffer getOrCreateTopic(int topicId) {
        return getOrCreateTopic(topicId, defaultRegionSize);
    }

    public TopicRingBuffer getOrCreateTopic(int topicId, long regionSize) {
        TopicRingBuffer cached = topicCache.get(topicId);
        if (cached != null && !cached.isClosed()) {
            return cached;
        }

        synchronized (this) {
            cached = topicCache.get(topicId);
            if (cached != null && !cached.isClosed()) {
                return cached;
            }

            long regionStart = findRegion(topicId);

            if (regionStart != 0) {
                TopicRingBuffer buffer = new TopicRingBuffer(topicId, regionStart);
                topicCache.put(topicId, buffer);
                logger.debug("Reusing existing topic {} at address 0x{}",
                        topicId, Long.toHexString(regionStart));
                return buffer;
            }

            long usedSize = getLong(baseAddress + USED_SIZE_OFFSET);
            long available = totalSize - GLOBAL_METADATA_SIZE - usedSize;

            long alignedSize = ((regionSize + 4095) / 4096) * 4096;

            if (available < alignedSize) {
                throw new MemoryFullException(
                        String.format("Not enough memory to create topic: available=%d bytes, needed=%d bytes (aligned), totalUsed=%d bytes, total=%d bytes",
                                available, alignedSize, usedSize, totalSize
                        )
                );
            }

            TopicRingBuffer newBuffer = createRegion(topicId, regionSize);
            topicCache.put(topicId, newBuffer);
            logger.info("Created new topic {} with region size {} bytes",
                    topicId, newBuffer.getCapacity());
            return newBuffer;
        }
    }

    public TopicRingBuffer getOrCreateTopic(String topic) {
        return getOrCreateTopic(topic, defaultRegionSize);
    }

    public TopicRingBuffer getOrCreateTopic(String topic, long regionSize) {
        int topicId = getOrCreateStringTopicId(topic);
        return getOrCreateTopic(topicId, regionSize);
    }

    public TopicRingBuffer getTopic(int topicId) {
        return topicCache.get(topicId);
    }

    public TopicRingBuffer getTopic(String topic) {
        int topicId = getTopicId(topic);
        if (topicId == -1) {
            return null;
        }
        return topicCache.get(topicId);
    }

    public long getTopicPendingCount(int topicId, String subscriberId) {
        TopicRingBuffer buffer = getTopic(topicId);
        if (buffer == null) {
            return 0;
        }
        return buffer.getSubscriberPendingCount(subscriberId);
    }

    public long getTopicPendingCount(String topic, String subscriberId) {
        TopicRingBuffer buffer = getTopic(topic);
        if (buffer == null) {
            return 0;
        }
        return buffer.getSubscriberPendingCount(subscriberId);
    }

    public synchronized void removeTopic(int topicId) {
        TopicRingBuffer buffer = topicCache.remove(topicId);
        if (buffer == null) {
            logger.warn("Topic {} not found", topicId);
            return;
        }

        long regionStart = findRegion(topicId);
        if (regionStart == 0) {
            logger.warn("Region for topic {} not found", topicId);
            return;
        }

        int regionSize = getInt(regionStart + REGION_SIZE_OFFSET);
        long usedSize = getLong(baseAddress + USED_SIZE_OFFSET);
        putLong(baseAddress + USED_SIZE_OFFSET, usedSize - regionSize);

        int topicCount = getInt(baseAddress + TOPIC_COUNT_OFFSET);
        putInt(baseAddress + TOPIC_COUNT_OFFSET, topicCount - 1);

        setMemory(regionStart, regionSize, (byte) 0);

        for (Map.Entry<String, Integer> entry : stringTopicCache.entrySet()) {
            if (entry.getValue() == topicId) {
                stringTopicCache.remove(entry.getKey());
                break;
            }
        }

        logger.info("Removed topic {}: freed {} bytes", topicId, regionSize);
    }

    public synchronized void removeTopic(String topic) {
        int topicId = getTopicId(topic);
        if (topicId == -1) {
            logger.warn("Topic {} not found", topic);
            return;
        }
        removeTopic(topicId);
    }

    private TopicRingBuffer createRegion(int topicId, long requestedSize) {
        if (baseAddress == 0) {
            throw new SharedMemoryException("baseAddress is 0");
        }

        int topicCount = getInt(baseAddress + TOPIC_COUNT_OFFSET);
        if (topicCount >= 32) {
            throw new MemoryFullException("Maximum topics exceeded: " + topicCount);
        }

        long usedSize = getLong(baseAddress + USED_SIZE_OFFSET);

        if (usedSize < 0 || usedSize > totalSize - GLOBAL_METADATA_SIZE) {
            logger.warn("Invalid usedSize: {}, resetting to 0", usedSize);
            putLong(baseAddress + USED_SIZE_OFFSET, 0);
            usedSize = 0;
        }

        long available = totalSize - GLOBAL_METADATA_SIZE - usedSize;

        long MIN_REGION_SIZE = 8 * 1024;
        if (available < MIN_REGION_SIZE) {
            throw new MemoryFullException(
                    String.format("Not enough memory: available=%d bytes, min region=%d bytes",
                            available, MIN_REGION_SIZE)
            );
        }
        long regionSize = requestedSize;

        regionSize = ((regionSize + 4095) / 4096) * 4096;

        if (regionSize > available) {
            throw new MemoryFullException(
                    String.format("Requested region size %d bytes (aligned) exceeds available memory %d bytes",
                            regionSize, available)
            );
        }

        if (regionSize < MIN_REGION_SIZE) {
            throw new MemoryFullException(
                    String.format("Requested region size %d bytes is less than minimum %d bytes",
                            regionSize, MIN_REGION_SIZE)
            );
        }

        long regionStart = dataStart + usedSize;

        if (regionStart < baseAddress || regionStart + regionSize > baseAddress + totalSize) {
            throw new SharedMemoryException(
                    String.format("Invalid region range: start=0x%x, size=%d, base=0x%x, total=%d",
                            regionStart, regionSize, baseAddress, totalSize)
            );
        }

        UnsafeUtil.setMemory(regionStart, regionSize, (byte) 0);

        putInt(regionStart + REGION_TOPIC_ID_OFFSET, topicId);
        putInt(regionStart + REGION_SIZE_OFFSET, (int) regionSize);
        putLong(regionStart + REGION_WRITE_SEQ_OFFSET, 0);
        putLong(regionStart + REGION_READ_SEQ_OFFSET, 0);
        putLong(regionStart + REGION_CREATED_AT_OFFSET, System.currentTimeMillis());
        putLong(regionStart + REGION_LAST_ACCESS_OFFSET, System.currentTimeMillis());

        putLong(baseAddress + USED_SIZE_OFFSET, usedSize + regionSize);
        putInt(baseAddress + TOPIC_COUNT_OFFSET, topicCount + 1);

        logger.info("Created region for topic {}: size={} bytes, start=0x{}",
                topicId, regionSize, Long.toHexString(regionStart));

        return new TopicRingBuffer(topicId, regionStart);
    }

    private long findRegion(int topicId) {
        if (this.baseAddress == 0L) {
            logger.error("findRegion called with baseAddress=0");
            return 0L;
        }

        long usedSize = UnsafeUtil.getLong(this.baseAddress + 16L);

        if (usedSize < 0 || usedSize > this.totalSize - 1024L) {
            logger.warn("Invalid usedSize: {}, resetting to 0", usedSize);
            UnsafeUtil.putLong(this.baseAddress + 16L, 0L);
            return 0L;
        }

        if (usedSize == 0) {
            return 0L;
        }

        if (usedSize % 4096 != 0) {
            logger.warn("usedSize not aligned: {}, resetting to 0", usedSize);
            UnsafeUtil.putLong(this.baseAddress + 16L, 0L);
            return 0L;
        }

        for (long pos = this.dataStart; pos < this.dataStart + usedSize; ) {
            if (pos < this.baseAddress || pos > this.baseAddress + this.totalSize) {
                logger.error("pos out of range: pos=0x{}, base=0x{}, total={}", Long.toHexString(pos), Long.toHexString(this.baseAddress), this.totalSize);
                resetSharedMemory();
                return 0L;
            }

            int existingTopic = UnsafeUtil.getInt(pos);
            int regionSize = UnsafeUtil.getInt(pos + 4L);

            if (regionSize <= 0 || regionSize > this.totalSize) {
                logger.error("Invalid region size: {} at pos=0x{}, resetting shared memory", regionSize, Long.toHexString(pos));
                resetSharedMemory();
                return 0L;
            }

            if (regionSize % 4096 != 0) {
                logger.error("regionSize not aligned: {} at pos=0x{}, resetting shared memory", regionSize, Long.toHexString(pos));
                resetSharedMemory();
                return 0L;
            }

            if (existingTopic == topicId) {
                long writeAddr = pos + 32L;
                if (writeAddr >= this.baseAddress + this.totalSize) {
                    logger.error("write address out of range: 0x{}", Long.toHexString(writeAddr));
                    resetSharedMemory();
                    return 0L;
                }
                UnsafeUtil.putLong(writeAddr, System.currentTimeMillis());
                return pos;
            }
            pos += regionSize;
        }

        return 0L;
    }

    private void resetSharedMemory() {
        logger.warn("Resetting shared memory due to corruption");
        UnsafeUtil.putLong(this.baseAddress + 16L, 0L);
        UnsafeUtil.putInt(this.baseAddress + 24L, 0);
        UnsafeUtil.setMemory(this.dataStart, this.totalSize - 1024L, (byte) 0);
        UnsafeUtil.setMemory(this.baseAddress + 128L, 2048L, (byte) 0);
        this.topicCache.clear();
        this.stringTopicCache.clear();
    }

    public boolean topicExists(int topicId) {
        return findRegion(topicId) != 0;
    }

    public boolean topicExists(String topic) {
        return getTopicId(topic) != -1;
    }

    public List<Integer> getTopicList() {
        List<Integer> topics = new ArrayList<>();
        long usedSize = getLong(baseAddress + USED_SIZE_OFFSET);
        long pos = dataStart;

        while (pos < dataStart + usedSize) {
            int topicId = getInt(pos + REGION_TOPIC_ID_OFFSET);
            topics.add(topicId);
            int regionSize = getInt(pos + REGION_SIZE_OFFSET);
            pos += regionSize;
        }

        return topics;
    }

    public TopicMetadata getTopicMetadata(int topicId) {
        long regionStart = findRegion(topicId);
        if (regionStart == 0) {
            return null;
        }

        int regionSize = getInt(regionStart + REGION_SIZE_OFFSET);
        long writeOffset = getLong(regionStart + REGION_WRITE_SEQ_OFFSET);
        long readOffset = getLong(regionStart + REGION_READ_SEQ_OFFSET);
        long createdAt = getLong(regionStart + REGION_CREATED_AT_OFFSET);
        long lastAccess = getLong(regionStart + REGION_LAST_ACCESS_OFFSET);

        TopicRingBuffer buffer = topicCache.get(topicId);
        long pendingCount = 0;
        if (buffer != null) {
            pendingCount = buffer.getPendingCount();
        }

        return new TopicMetadata(topicId, regionSize, writeOffset, readOffset,
                createdAt, lastAccess, pendingCount);
    }

    public TopicMetadata getTopicMetadata(String topic) {
        int topicId = getTopicId(topic);
        if (topicId == -1) {
            return null;
        }
        return getTopicMetadata(topicId);
    }

    public MemoryStats getStats() {
        long arenaUsed = getLong(baseAddress + USED_SIZE_OFFSET);
        int topicCount = getInt(baseAddress + TOPIC_COUNT_OFFSET);
        long createdAt = getLong(baseAddress + CREATED_AT_OFFSET);

        long totalRegionSize = 0;
        long totalRegionUsed = 0;
        long totalDataBytes = 0;
        long totalDataCount = 0;

        for (TopicRingBuffer buffer : topicCache.values()) {
            totalRegionSize += buffer.getCapacity();
            totalRegionUsed += buffer.getCapacity() - (buffer.getCapacity() - buffer.getPendingBytes());
            totalDataBytes += buffer.getPendingBytes();
            totalDataCount += buffer.getPendingCount();
        }

        return new MemoryStats(totalSize, arenaUsed, totalRegionSize, totalRegionUsed, totalDataBytes, totalDataCount, topicCount, createdAt);
    }

    public long getTopicPendingCount(int topicId) {
        TopicRingBuffer buffer = getTopic(topicId);
        if (buffer == null) {
            return 0;
        }
        return buffer.getPendingCount();
    }

    public long getTopicPendingCount(String topic) {
        TopicRingBuffer buffer = getTopic(topic);
        if (buffer == null) {
            return 0;
        }
        return buffer.getPendingCount();
    }

    public long getTopicPendingBytes(int topicId) {
        TopicRingBuffer buffer = getTopic(topicId);
        if (buffer == null) {
            return 0;
        }
        return buffer.getPendingBytes();
    }

    public long getTopicPendingBytes(String topic) {
        TopicRingBuffer buffer = getTopic(topic);
        if (buffer == null) {
            return 0;
        }
        return buffer.getPendingBytes();
    }

    public void close() {
        this.closed = true;
        topicCache.clear();
        stringTopicCache.clear();
    }
}