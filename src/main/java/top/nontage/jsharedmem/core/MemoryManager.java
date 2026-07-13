package top.nontage.jsharedmem.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.nontage.jsharedmem.exception.MemoryFullException;
import top.nontage.jsharedmem.model.MemoryStats;
import top.nontage.jsharedmem.model.TopicMetadata;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

    public MemoryManager(long baseAddress, long totalSize) {
        this.baseAddress = baseAddress;
        this.totalSize = totalSize;
        this.dataStart = baseAddress + GLOBAL_METADATA_SIZE;

        int magic = getInt(baseAddress + MAGIC_OFFSET);
        if (magic != 0x4A534D) {
            initGlobalMetadata();
            logger.info("Initialized new shared memory");
        } else {
            logger.info("Connected to existing shared memory (version: {})",
                    getInt(baseAddress + VERSION_OFFSET));
            loadStringTopicCache();
        }
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

            TopicRingBuffer newBuffer = createRegion(topicId);
            topicCache.put(topicId, newBuffer);
            logger.info("Created new topic {} with region size {} bytes",
                    topicId, newBuffer.getCapacity());
            return newBuffer;
        }
    }

    public TopicRingBuffer getOrCreateTopic(String topic) {
        int topicId = getOrCreateStringTopicId(topic);
        return getOrCreateTopic(topicId);
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

    private TopicRingBuffer createRegion(int topicId) {
        int topicCount = getInt(baseAddress + TOPIC_COUNT_OFFSET);
        if (topicCount >= 32) {
            throw new MemoryFullException("Maximum topics exceeded: " + topicCount);
        }

        long usedSize = getLong(baseAddress + USED_SIZE_OFFSET);
        long available = totalSize - GLOBAL_METADATA_SIZE - usedSize;

        long regionSize = Math.min(available, 64 * 1024);

        if (regionSize < 4096) {
            throw new MemoryFullException(
                    String.format("Not enough memory: available=%d bytes, min region=%d bytes",
                            available, 4096)
            );
        }

        regionSize = ((regionSize + 4095) / 4096) * 4096;

        long regionStart = dataStart + usedSize;

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
        long usedSize = getLong(baseAddress + USED_SIZE_OFFSET);
        long pos = dataStart;

        while (pos < dataStart + usedSize) {
            int existingTopic = getInt(pos + REGION_TOPIC_ID_OFFSET);
            if (existingTopic == topicId) {
                putLong(pos + REGION_LAST_ACCESS_OFFSET, System.currentTimeMillis());
                return pos;
            }
            int regionSize = getInt(pos + REGION_SIZE_OFFSET);
            pos += regionSize;
        }

        return 0;
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

        return new TopicMetadata(topicId, regionSize, writeOffset, readOffset, createdAt, lastAccess);
    }

    public TopicMetadata getTopicMetadata(String topic) {
        int topicId = getTopicId(topic);
        if (topicId == -1) {
            return null;
        }
        return getTopicMetadata(topicId);
    }

    public MemoryStats getStats() {
        long usedSize = getLong(baseAddress + USED_SIZE_OFFSET);
        int topicCount = getInt(baseAddress + TOPIC_COUNT_OFFSET);
        long createdAt = getLong(baseAddress + CREATED_AT_OFFSET);

        return new MemoryStats(totalSize, usedSize, topicCount, createdAt);
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