package top.nontage.jsharedmem.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.nontage.jsharedmem.exception.MemoryFullException;
import top.nontage.jsharedmem.exception.SharedMemoryException;

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
    private static final int SUBSCRIBER_TABLE_SIZE = MAX_SUBSCRIBERS * SUBSCRIBER_ENTRY_SIZE;
    private static final int SUBSCRIBER_FLAG_OFFSET = 0;
    private static final int SUBSCRIBER_ID_OFFSET = 4;
    private static final int SUBSCRIBER_READ_OFFSET_OFFSET = 32;
    private static final int SUBSCRIBER_LAST_ACTIVE_OFFSET = 40;
    private static final int SUBSCRIBER_ID_LENGTH = 28;

    private final int topicId;
    private final long regionStart;
    private final long dataStart;
    private final int dataCapacity;
    private final int maxMessageSize;

    private final long writeOffsetAddress;
    private final long readOffsetAddress;

    private volatile boolean closed = false;

    private final ConcurrentHashMap<String, Long> subscriberCache = new ConcurrentHashMap<>();

    public TopicRingBuffer(int topicId, long regionStart) {
        this.topicId = topicId;
        this.regionStart = regionStart;
        int regionSize = getInt(regionStart + 4);
        this.dataStart = regionStart + SUBSCRIBER_TABLE_START + SUBSCRIBER_TABLE_SIZE;
        this.dataCapacity = regionSize - SUBSCRIBER_TABLE_START - SUBSCRIBER_TABLE_SIZE;

        if (this.dataCapacity < 1024) {
            throw new SharedMemoryException(
                    String.format("Region too small: regionSize=%d, dataCapacity=%d (min 1024)",
                            regionSize, this.dataCapacity)
            );
        }

        this.writeOffsetAddress = regionStart + 8;
        this.readOffsetAddress = regionStart + 16;

        this.maxMessageSize = Math.min(Math.max(dataCapacity / 10, 1024), 1024 * 1024);

        long currentWriteOffset = getLong(writeOffsetAddress);
        long currentReadOffset = getLong(readOffsetAddress);

        loadSubscribers();

        logger.debug("TopicRingBuffer created: topic={}, capacity={} bytes, maxMsgSize={}, writeOffset={}, readOffset={}",
                topicId, dataCapacity, maxMessageSize, currentWriteOffset, currentReadOffset);
    }

    private void loadSubscribers() {
        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            long entryAddr = regionStart + SUBSCRIBER_TABLE_START + (i * SUBSCRIBER_ENTRY_SIZE);
            int flag = getInt(entryAddr + SUBSCRIBER_FLAG_OFFSET);
            if (flag == 0) continue;
            String subscriberId = readSubscriberId(entryAddr);
            if (!subscriberId.isEmpty()) {
                long readOffset = getLong(entryAddr + SUBSCRIBER_READ_OFFSET_OFFSET);
                subscriberCache.put(subscriberId, readOffset);
                logger.debug("Loaded subscriber: {} -> readOffset={}", subscriberId, readOffset);
            }
        }
    }

    public long registerSubscriber(String subscriberId) {
        Long cachedOffset = subscriberCache.get(subscriberId);
        if (cachedOffset != null) {
            updateLastActive(subscriberId);
            return cachedOffset;
        }

        long writeOffset = 0;

        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            long entryAddr = regionStart + SUBSCRIBER_TABLE_START + (i * SUBSCRIBER_ENTRY_SIZE);
            long flagAddr = entryAddr + SUBSCRIBER_FLAG_OFFSET;

            int flag = getInt(flagAddr);
            if (flag == 0) {
                if (compareAndSwapInt(null, flagAddr, 0, 1)) {
                    writeSubscriberId(entryAddr, subscriberId);
                    putLong(entryAddr + SUBSCRIBER_READ_OFFSET_OFFSET, writeOffset);
                    putLong(entryAddr + SUBSCRIBER_LAST_ACTIVE_OFFSET, System.currentTimeMillis());
                    subscriberCache.put(subscriberId, writeOffset);
                    logger.info("Registered subscriber {} for topic {} with readOffset={}",
                            subscriberId, topicId, writeOffset);
                    return writeOffset;
                }
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
            int flag = getInt(entryAddr + SUBSCRIBER_FLAG_OFFSET);
            if (flag == 0) continue;
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
            int flag = getInt(entryAddr + SUBSCRIBER_FLAG_OFFSET);
            if (flag == 0) continue;
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

    public void publish(byte[] data, Long ttlMs) {
        if (closed) {
            throw new IllegalStateException("TopicRingBuffer is closed");
        }
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }

        if (data.length > maxMessageSize) {
            throw new IllegalArgumentException("Message too large: " + data.length + " bytes (max " + maxMessageSize + ")");
        }

        long ttl = (ttlMs == null) ? 10000 : ttlMs;

        int totalSize = MSG_HEADER_SIZE + data.length;
        totalSize = (totalSize + 7) & ~7;

        long readOffset = getLong(readOffsetAddress);
        long writeOffset;
        long writePos;
        long available;

        while (true) {
            writeOffset = getLong(writeOffsetAddress);
            writePos = dataStart + (writeOffset % dataCapacity);
            long readPos = dataStart + (readOffset % dataCapacity);

            if (writePos >= readPos) {
                available = dataCapacity - (writePos - readPos);
            } else {
                available = readPos - writePos;
            }

            if (available < totalSize + 8) {
                throw new MemoryFullException("Topic " + topicId + " memory full: available=" + available + " bytes, needed=" + (totalSize + 8) + " bytes");
            }

            long newWriteOffset = writeOffset + totalSize;
            if (compareAndSwapLong(null, writeOffsetAddress, writeOffset, newWriteOffset)) {
                long now = System.currentTimeMillis();
                putInt(writePos + MSG_LENGTH_OFFSET, data.length);
                storeFence();
                int verifyLen = getInt(writePos + MSG_LENGTH_OFFSET);
                if (verifyLen != data.length) {
                    putLong(writeOffsetAddress, writeOffset);
                    throw new RuntimeException("Length mismatch at offset " + writeOffset + ": wrote " + data.length + ", read " + verifyLen);
                }
                putLong(writePos + MSG_TIMESTAMP_OFFSET, now);
                putLong(writePos + MSG_TTL_OFFSET, ttl);
                for (int i = 0; i < data.length; i++) {
                    putByte(writePos + MSG_HEADER_SIZE + i, data[i]);
                }
                storeFence();
                logger.trace("Published {} bytes to topic {}", data.length, topicId);
                return;
            }
        }
    }

    public byte[] poll(String subscriberId) {
        if (closed) {
            throw new IllegalStateException("TopicRingBuffer is closed");
        }

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
        loadFence();
        int length = getInt(readPos + MSG_LENGTH_OFFSET);

        if (length <= 0 || length > maxMessageSize || length > dataCapacity) {
            logger.warn("Invalid message at offset {}, length={}, max={}, skipping", readOffset, length, maxMessageSize);
            long skipSize = Math.min(1024, dataCapacity);
            updateSubscriberReadOffset(subscriberId, readOffset + skipSize);
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
        cleanExpiredMessages();

        logger.trace("Subscriber {} consumed {} bytes (readOffset={})", subscriberId, length, readOffset);
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

            if (length <= 0 || length > maxMessageSize) {
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

            if (length <= 0 || length > maxMessageSize) {
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

            if (length <= 0 || length > maxMessageSize) {
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

    public int getTopicId() {
        return topicId;
    }

    public long getDataStartAddress() {
        return dataStart;
    }

    public int getDataCapacity() {
        return dataCapacity;
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
        loadFence();
        int length = getInt(readPos + MSG_LENGTH_OFFSET);

        if (length <= 0 || length > maxMessageSize || length > dataCapacity) {
            logger.warn("Invalid message at offset {}, length={}, max={}, skipping", readOffset, length, maxMessageSize);
            long skipSize = Math.min(1024, dataCapacity);
            updateSubscriberReadOffset(subscriberId, readOffset + skipSize);
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
        cleanExpiredMessages();

        logger.trace("Subscriber {} consumed {} bytes (readOffset={})", subscriberId, length, readOffset);

        return new MessageWithMeta(data, timestamp, ttl);
    }
}