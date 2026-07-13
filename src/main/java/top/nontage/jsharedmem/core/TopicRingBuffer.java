package top.nontage.jsharedmem.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

import static top.nontage.jsharedmem.core.UnsafeUtil.*;

public class TopicRingBuffer {

    private static final Logger logger = LoggerFactory.getLogger(TopicRingBuffer.class);

    private static final int MSG_HEADER_SIZE = 20;
    private static final int MSG_LENGTH_OFFSET = 0;
    private static final int MSG_TIMESTAMP_OFFSET = 4;
    private static final int MSG_TTL_OFFSET = 12;

    private static final int SUBSCRIBER_TABLE_START = 64;
    private static final int MAX_SUBSCRIBERS = 32;
    private static final int SUBSCRIBER_ENTRY_SIZE = 64;
    private static final int SUBSCRIBER_ID_OFFSET = 0;
    private static final int SUBSCRIBER_READ_OFFSET_OFFSET = 32;
    private static final int SUBSCRIBER_LAST_ACTIVE_OFFSET = 40;
    private static final int SUBSCRIBER_ID_LENGTH = 32;

    private final int topicId;
    private final long regionStart;
    private final long dataStart;
    private final int dataCapacity;

    private final long writeOffsetAddress;
    private final long readOffsetAddress;

    private volatile boolean closed = false;

    private final ConcurrentHashMap<String, Long> subscriberCache = new ConcurrentHashMap<>();

    public TopicRingBuffer(int topicId, long regionStart) {
        this.topicId = topicId;
        this.regionStart = regionStart;
        int regionSize = getInt(regionStart + 4);
        this.dataStart = regionStart + 64;
        this.dataCapacity = regionSize - 64;
        this.writeOffsetAddress = regionStart + 8;
        this.readOffsetAddress = regionStart + 16;

        long currentWriteOffset = getLong(writeOffsetAddress);
        long currentReadOffset = getLong(readOffsetAddress);

        loadSubscribers();

        logger.debug("TopicRingBuffer created: topic={}, capacity={} bytes, writeOffset={}, readOffset={}",
                topicId, dataCapacity, currentWriteOffset, currentReadOffset);
    }

    private void loadSubscribers() {
        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            long entryAddr = regionStart + SUBSCRIBER_TABLE_START + (i * SUBSCRIBER_ENTRY_SIZE);
            String subscriberId = readSubscriberId(entryAddr);
            if (!subscriberId.isEmpty()) {
                long readOffset = getLong(entryAddr + SUBSCRIBER_READ_OFFSET_OFFSET);
                subscriberCache.put(subscriberId, readOffset);
                logger.debug("Loaded subscriber: {} -> readOffset={}", subscriberId, readOffset);
            }
        }
    }

    public synchronized long registerSubscriber(String subscriberId) {
        Long cachedOffset = subscriberCache.get(subscriberId);
        if (cachedOffset != null) {
            updateLastActive(subscriberId);
            return cachedOffset;
        }

        long writeOffset = 0;

        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            long entryAddr = regionStart + SUBSCRIBER_TABLE_START + (i * SUBSCRIBER_ENTRY_SIZE);
            String existingId = readSubscriberId(entryAddr);
            if (existingId.isEmpty()) {
                writeSubscriberId(entryAddr, subscriberId);
                putLong(entryAddr + SUBSCRIBER_READ_OFFSET_OFFSET, writeOffset);
                putLong(entryAddr + SUBSCRIBER_LAST_ACTIVE_OFFSET, System.currentTimeMillis());
                subscriberCache.put(subscriberId, writeOffset);
                logger.info("Registered subscriber {} for topic {} with readOffset={}",
                        subscriberId, topicId, writeOffset);
                return writeOffset;
            }
        }

        throw new RuntimeException("Too many subscribers for topic " + topicId);
    }

    public long getSubscriberReadOffset(String subscriberId) {
        Long offset = subscriberCache.get(subscriberId);
        if (offset != null) {
            updateLastActive(subscriberId);
            return offset;
        }
        return -1;
    }

    public void updateSubscriberReadOffset(String subscriberId, long newOffset) {
        subscriberCache.put(subscriberId, newOffset);

        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            long entryAddr = regionStart + SUBSCRIBER_TABLE_START + (i * SUBSCRIBER_ENTRY_SIZE);
            String existingId = readSubscriberId(entryAddr);
            if (existingId.equals(subscriberId)) {
                putLong(entryAddr + SUBSCRIBER_READ_OFFSET_OFFSET, newOffset);
                putLong(entryAddr + SUBSCRIBER_LAST_ACTIVE_OFFSET, System.currentTimeMillis());
                return;
            }
        }
    }

    private void updateLastActive(String subscriberId) {
        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            long entryAddr = regionStart + SUBSCRIBER_TABLE_START + (i * SUBSCRIBER_ENTRY_SIZE);
            String existingId = readSubscriberId(entryAddr);
            if (existingId.equals(subscriberId)) {
                putLong(entryAddr + SUBSCRIBER_LAST_ACTIVE_OFFSET, System.currentTimeMillis());
                return;
            }
        }
    }

    private String readSubscriberId(long entryAddr) {
        byte[] bytes = new byte[SUBSCRIBER_ID_LENGTH];
        for (int i = 0; i < SUBSCRIBER_ID_LENGTH; i++) {
            bytes[i] = getByte(entryAddr + SUBSCRIBER_ID_OFFSET + i);
        }
        return new String(bytes, StandardCharsets.UTF_8).trim();
    }

    private void writeSubscriberId(long entryAddr, String id) {
        byte[] bytes = id.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < SUBSCRIBER_ID_LENGTH; i++) {
            putByte(entryAddr + SUBSCRIBER_ID_OFFSET + i, i < bytes.length ? bytes[i] : 0);
        }
    }

    public boolean publish(byte[] data, Long ttlMs) {
        if (closed) {
            throw new IllegalStateException("TopicRingBuffer is closed");
        }
        if (data == null || data.length == 0) {
            return false;
        }

        long ttl = (ttlMs == null) ? 10000 : ttlMs;

        int totalSize = MSG_HEADER_SIZE + data.length;
        totalSize = (totalSize + 7) & ~7;

        long writeOffset = getLong(writeOffsetAddress);
        long readOffset = getLong(readOffsetAddress);
        long writePos = dataStart + (writeOffset % dataCapacity);
        long readPos = dataStart + (readOffset % dataCapacity);

        long available;
        if (writePos >= readPos) {
            available = dataCapacity - (writePos - readPos);
        } else {
            available = readPos - writePos;
        }

        if (available < totalSize + 8) {
            logger.warn("Topic {} memory full: available={}, needed={}", topicId, available, totalSize + 8);
            return false;
        }

        long now = System.currentTimeMillis();

        putInt(writePos + MSG_LENGTH_OFFSET, data.length);
        putLong(writePos + MSG_TIMESTAMP_OFFSET, now);
        putLong(writePos + MSG_TTL_OFFSET, ttl);

        for (int i = 0; i < data.length; i++) {
            putByte(writePos + MSG_HEADER_SIZE + i, data[i]);
        }

        putLong(writeOffsetAddress, writeOffset + totalSize);
        return true;
    }

    public byte[] poll(String subscriberId) {
        if (closed) {
            throw new IllegalStateException("TopicRingBuffer is closed");
        }

        cleanExpiredMessages();

        long writeOffset = getLong(writeOffsetAddress);
        Long readOffsetObj = subscriberCache.get(subscriberId);
        if (readOffsetObj == null) {
            throw new IllegalArgumentException("Subscriber not registered: " + subscriberId);
        }
        long readOffset = readOffsetObj;

        if (readOffset >= writeOffset) {
            return null;
        }

        long readPos = dataStart + (readOffset % dataCapacity);
        int length = getInt(readPos + MSG_LENGTH_OFFSET);

        if (length <= 0 || length > 1024 * 1024) {
            logger.warn("Invalid message for subscriber {} at offset {}, skipping", subscriberId, readOffset);
            updateSubscriberReadOffset(subscriberId, readOffset + 1);
            return null;
        }

        long timestamp = getLong(readPos + MSG_TIMESTAMP_OFFSET);
        long ttl = getLong(readPos + MSG_TTL_OFFSET);
        long now = System.currentTimeMillis();

        if (now - timestamp > ttl) {
            int totalSize = MSG_HEADER_SIZE + length;
            totalSize = (totalSize + 7) & ~7;
            updateSubscriberReadOffset(subscriberId, readOffset + totalSize);
            logger.debug("Message expired for subscriber {} at offset {}, skipping", subscriberId, readOffset);
            return null;
        }

        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = getByte(readPos + MSG_HEADER_SIZE + i);
        }

        int totalSize = MSG_HEADER_SIZE + length;
        totalSize = (totalSize + 7) & ~7;
        updateSubscriberReadOffset(subscriberId, readOffset + totalSize);

        logger.trace("Subscriber {} consumed {} bytes (readOffset={})",
                subscriberId, length, readOffset);
        return data;
    }

    private void cleanExpiredMessages() {
        long writeOffset = getLong(writeOffsetAddress);
        long readOffset = getLong(readOffsetAddress);
        long now = System.currentTimeMillis();
        boolean cleaned = false;

        while (readOffset < writeOffset) {
            long readPos = dataStart + (readOffset % dataCapacity);
            int length = getInt(readPos + MSG_LENGTH_OFFSET);

            if (length <= 0 || length > 1024 * 1024) {
                putLong(readOffsetAddress, readOffset + 1);
                readOffset = getLong(readOffsetAddress);
                continue;
            }

            long timestamp = getLong(readPos + MSG_TIMESTAMP_OFFSET);
            long ttl = getLong(readPos + MSG_TTL_OFFSET);

            if (now - timestamp > ttl) {
                int totalSize = MSG_HEADER_SIZE + length;
                totalSize = (totalSize + 7) & ~7;
                putLong(readOffsetAddress, readOffset + totalSize);
                readOffset = getLong(readOffsetAddress);
                cleaned = true;
            } else {
                break;
            }
        }

        if (cleaned) {
            logger.debug("Cleaned expired messages in topic {}", topicId);
        }
    }

    public long getWriteOffset() {
        return getLong(writeOffsetAddress);
    }

    public long getSubscriberPendingBytes(String subscriberId) {
        Long readOffset = subscriberCache.get(subscriberId);
        if (readOffset == null) return 0;
        return getWriteOffset() - readOffset;
    }

    public long getSubscriberPendingCount(String subscriberId) {
        Long readOffset = subscriberCache.get(subscriberId);
        if (readOffset == null) return 0;
        long writeOffset = getWriteOffset();
        long pendingBytes = writeOffset - readOffset;

        if (pendingBytes <= 0) return 0;

        long count = 0;
        long scanOffset = readOffset;

        while (scanOffset < writeOffset) {
            long readPos = dataStart + (scanOffset % dataCapacity);
            int length = getInt(readPos + MSG_LENGTH_OFFSET);

            if (length <= 0 || length > 1024 * 1024) {
                break;
            }

            int totalSize = MSG_HEADER_SIZE + length;
            totalSize = (totalSize + 7) & ~7;
            scanOffset += totalSize;
            count++;
        }

        return count;
    }

    public long getPendingCount() {
        long writeOffset = getLong(writeOffsetAddress);
        long readOffset = getLong(readOffsetAddress);
        long pendingBytes = writeOffset - readOffset;

        if (pendingBytes <= 0) {
            return 0;
        }

        long count = 0;
        long scanOffset = readOffset;

        while (scanOffset < writeOffset) {
            long readPos = dataStart + (scanOffset % dataCapacity);
            int length = getInt(readPos + MSG_LENGTH_OFFSET);

            if (length <= 0 || length > 1024 * 1024) {
                break;
            }

            int totalSize = MSG_HEADER_SIZE + length;
            totalSize = (totalSize + 7) & ~7;
            scanOffset += totalSize;
            count++;
        }

        return count;
    }

    public long getPendingBytes() {
        return getLong(writeOffsetAddress) - getLong(readOffsetAddress);
    }

    public int getCapacity() {
        return dataCapacity;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
    }

    public static class MessageWithMeta {
        public final byte[] data;
        public final long timestamp;
        public final long ttl;

        public MessageWithMeta(byte[] data, long timestamp, long ttl) {
            this.data = data;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }
    }

    public MessageWithMeta pollWithMeta(String subscriberId) {
        if (closed) {
            throw new IllegalStateException("TopicRingBuffer is closed");
        }

        cleanExpiredMessages();

        long writeOffset = getLong(writeOffsetAddress);
        Long readOffsetObj = subscriberCache.get(subscriberId);
        if (readOffsetObj == null) {
            throw new IllegalArgumentException("Subscriber not registered: " + subscriberId);
        }
        long readOffset = readOffsetObj;

        if (readOffset >= writeOffset) {
            return null;
        }

        long readPos = dataStart + (readOffset % dataCapacity);
        int length = getInt(readPos + MSG_LENGTH_OFFSET);

        if (length <= 0 || length > 1024 * 1024) {
            logger.warn("Invalid message for subscriber {} at offset {}, skipping", subscriberId, readOffset);
            updateSubscriberReadOffset(subscriberId, readOffset + 1);
            return null;
        }

        long timestamp = getLong(readPos + MSG_TIMESTAMP_OFFSET);
        long ttl = getLong(readPos + MSG_TTL_OFFSET);
        long now = System.currentTimeMillis();

        if (now - timestamp > ttl) {
            int totalSize = MSG_HEADER_SIZE + length;
            totalSize = (totalSize + 7) & ~7;
            updateSubscriberReadOffset(subscriberId, readOffset + totalSize);
            logger.debug("Message expired for subscriber {} at offset {}, skipping", subscriberId, readOffset);
            return null;
        }

        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = getByte(readPos + MSG_HEADER_SIZE + i);
        }

        int totalSize = MSG_HEADER_SIZE + length;
        totalSize = (totalSize + 7) & ~7;
        updateSubscriberReadOffset(subscriberId, readOffset + totalSize);

        logger.trace("Subscriber {} consumed {} bytes (readOffset={})",
                subscriberId, length, readOffset);

        return new MessageWithMeta(data, timestamp, ttl);
    }
}