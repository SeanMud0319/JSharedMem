package top.nontage.jsharedmem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.nontage.jsharedmem.core.MemoryManager;
import top.nontage.jsharedmem.core.NativeBridge;
import top.nontage.jsharedmem.core.TopicRingBuffer;
import top.nontage.jsharedmem.exception.MemoryFullException;
import top.nontage.jsharedmem.model.MemoryStats;
import top.nontage.jsharedmem.model.TopicMetadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JSharedMem implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(JSharedMem.class);

    public static final long DEFAULT_MEMORY_SIZE = 1024 * 1024;
    public static final long MIN_MEMORY_SIZE = 64 * 1024;
    private static final double DEFAULT_MAX_DATA_SIZE_RATIO = 0.1;

    private final String name;
    private final long memorySize;
    private final int maxDataSize;
    private final MemoryManager memoryManager;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private JSharedMem(String name, long memorySize, long maxDataSize) {
        this.name = name;
        this.memorySize = memorySize;

        long calculatedMax = Math.min(maxDataSize, memorySize / 2);
        if (calculatedMax < 1024) {
            calculatedMax = 1024;
        }
        this.maxDataSize = (int) calculatedMax;

        long baseAddress = NativeBridge.createOrOpen(name, memorySize);
        this.memoryManager = new MemoryManager(baseAddress, memorySize);
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("jsharedmem-" + name + "-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (running.get()) {
                close();
            }
        }));

        logger.info("JSharedMem connected: name={}, size={} bytes, maxDataSize={} bytes", name, memorySize, this.maxDataSize);
    }

    public static JSharedMem connect(String name) {
        return connect(name, DEFAULT_MEMORY_SIZE);
    }

    public static JSharedMem connect(String name, long memorySize) {
        return connect(name, memorySize, (long) (memorySize * DEFAULT_MAX_DATA_SIZE_RATIO));
    }

    public static JSharedMem connect(String name, long memorySize, long maxDataSize) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name cannot be null or empty");
        }
        if (memorySize < MIN_MEMORY_SIZE) {
            throw new IllegalArgumentException("Memory size too small: " + memorySize);
        }
        if (maxDataSize <= 0) {
            throw new IllegalArgumentException("Max data size must be positive");
        }
        if (maxDataSize > memorySize / 2) {
            throw new IllegalArgumentException("Max data size cannot exceed half of memory size");
        }
        memorySize = ((memorySize + 4095) / 4096) * 4096;
        return new JSharedMem(name, memorySize, maxDataSize);
    }

    public void publish(int topicId, Object data) {
        publish(topicId, data, null);
    }

    public void publish(int topicId, Object data, Long ttlMs) {
        checkRunning();
        validateTopicId(topicId);
        byte[] bytes = toBytes(data);
        validateData(bytes);
        TopicRingBuffer buffer = memoryManager.getOrCreateTopic(topicId);
        doPublish(buffer, topicId, bytes, ttlMs);
    }

    public void publish(String topic, Object data) {
        publish(topic, data, null);
    }

    public void publish(String topic, Object data, Long ttlMs) {
        checkRunning();
        validateTopicString(topic);
        byte[] bytes = toBytes(data);
        validateData(bytes);
        TopicRingBuffer buffer = memoryManager.getOrCreateTopic(topic);
        doPublish(buffer, topic, bytes, ttlMs);
    }

    private void doPublish(TopicRingBuffer buffer, Object topic, byte[] data, Long ttlMs) {
        boolean success = buffer.publish(data, ttlMs);
        if (!success) {
            throw new MemoryFullException(
                    String.format("Topic %s memory full, data size: %d bytes, pending: %d messages",
                            topic, data.length, buffer.getPendingCount())
            );
        }
        logger.debug("Published {} bytes to topic {} with TTL {}ms", data.length, topic, ttlMs);
    }

    public void subscribe(int topicId, MessageListener listener) {
        String subscriberId = "sub-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
        subscribe(topicId, subscriberId, listener);
    }

    public void subscribe(int topicId, String subscriberId, MessageListener listener) {
        checkRunning();
        validateTopicId(topicId);
        validateListener(listener);

        TopicRingBuffer buffer = memoryManager.getOrCreateTopic(topicId);
        buffer.registerSubscriber(subscriberId);

        doSubscribe(topicId, subscriberId, buffer, listener);
    }

    public void subscribe(String topic, MessageListener listener) {
        String subscriberId = "sub-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
        subscribe(topic, subscriberId, listener);
    }

    public void subscribe(String topic, String subscriberId, MessageListener listener) {
        checkRunning();
        validateTopicString(topic);
        validateListener(listener);

        TopicRingBuffer buffer = memoryManager.getOrCreateTopic(topic);
        buffer.registerSubscriber(subscriberId);

        doSubscribe(topic, subscriberId, buffer, listener);
    }

    public void subscribeWithMeta(int topicId, MessageWithMetaListener listener) {
        String subscriberId = "sub-meta-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
        subscribeWithMeta(topicId, subscriberId, listener);
    }

    public void subscribeWithMeta(String topic, MessageWithMetaListener listener) {
        String subscriberId = "sub-meta-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
        subscribeWithMeta(topic, subscriberId, listener);
    }

    public void subscribeWithMeta(int topicId, String subscriberId, MessageWithMetaListener listener) {
        checkRunning();
        validateTopicId(topicId);
        validateListener(listener);

        TopicRingBuffer buffer = memoryManager.getOrCreateTopic(topicId);
        buffer.registerSubscriber(subscriberId);

        doSubscribeWithMeta(topicId, subscriberId, buffer, listener);
    }

    public void subscribeWithMeta(String topic, String subscriberId, MessageWithMetaListener listener) {
        checkRunning();
        validateTopicString(topic);
        validateListener(listener);

        TopicRingBuffer buffer = memoryManager.getOrCreateTopic(topic);
        buffer.registerSubscriber(subscriberId);

        doSubscribeWithMeta(topic, subscriberId, buffer, listener);
    }

    private void doSubscribe(int topicId, String subscriberId, TopicRingBuffer buffer, MessageListener listener) {
        executor.submit(() -> {
            logger.info("Subscribed to int topic {} with subscriberId={}", topicId, subscriberId);

            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    byte[] data = buffer.poll(subscriberId);
                    if (data != null) {
                        try {
                            listener.onMessage(topicId, data);
                        } catch (Exception e) {
                            logger.error("Listener error for subscriber " + subscriberId, e);
                        }
                    } else {
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in subscriber loop for " + subscriberId, e);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            logger.info("Unsubscribed: {}", subscriberId);
        });
    }

    private void doSubscribe(String topic, String subscriberId, TopicRingBuffer buffer, MessageListener listener) {
        int topicId = memoryManager.getTopicId(topic);
        doSubscribe(topicId, subscriberId, buffer, listener);
    }

    private void doSubscribeWithMeta(int topicId, String subscriberId, TopicRingBuffer buffer, MessageWithMetaListener listener) {
        executor.submit(() -> {
            logger.info("Subscribed with meta to int topic {} with subscriberId={}", topicId, subscriberId);

            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    TopicRingBuffer.MessageWithMeta msg = buffer.pollWithMeta(subscriberId);
                    if (msg != null) {
                        try {
                            listener.onMessage(topicId, msg.data, msg.timestamp, msg.ttl);
                        } catch (Exception e) {
                            logger.error("Listener error for subscriber " + subscriberId, e);
                        }
                    } else {
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in subscriber loop for " + subscriberId, e);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            logger.info("Unsubscribed: {}", subscriberId);
        });
    }

    private void doSubscribeWithMeta(String topic, String subscriberId, TopicRingBuffer buffer, MessageWithMetaListener listener) {
        int topicId = memoryManager.getTopicId(topic);
        doSubscribeWithMeta(topicId, subscriberId, buffer, listener);
    }

    public boolean topicExists(String topic) {
        if (topic == null || topic.trim().isEmpty()) return false;
        return memoryManager.topicExists(topic);
    }

    public boolean topicExists(int topicId) {
        return memoryManager.topicExists(topicId);
    }

    public List<String> getTopicList() {
        return memoryManager.getStringTopicList();
    }

    public List<Integer> getIntTopicList() {
        return memoryManager.getTopicList();
    }

    public Integer getTopicId(String topic) {
        int id = memoryManager.getTopicId(topic);
        return id == -1 ? null : id;
    }

    public String getTopicName(int topicId) {
        return memoryManager.getTopicName(topicId);
    }

    public TopicMetadata getTopicMetadata(int topicId) {
        if (!running.get()) {
            throw new IllegalStateException("JSharedMem is already closed");
        }
        return memoryManager.getTopicMetadata(topicId);
    }

    public TopicMetadata getTopicMetadata(String topic) {
        if (!running.get()) {
            throw new IllegalStateException("JSharedMem is already closed");
        }
        return memoryManager.getTopicMetadata(topic);
    }

    public long getTopicPendingCount(int topicId) {
        if (!running.get()) {
            return 0;
        }
        return memoryManager.getTopicPendingCount(topicId);
    }

    public long getTopicPendingCount(String topic) {
        if (!running.get()) {
            return 0;
        }
        return memoryManager.getTopicPendingCount(topic);
    }

    public long getTopicPendingCount(int topicId, String subscriberId) {
        if (!running.get()) {
            return 0;
        }
        return memoryManager.getTopicPendingCount(topicId, subscriberId);
    }

    public long getTopicPendingCount(String topic, String subscriberId) {
        if (!running.get()) {
            return 0;
        }
        return memoryManager.getTopicPendingCount(topic, subscriberId);
    }

    public long getTopicPendingBytes(int topicId) {
        if (!running.get()) {
            return 0;
        }
        TopicRingBuffer buffer = memoryManager.getTopic(topicId);
        if (buffer == null) {
            return 0;
        }
        return buffer.getPendingBytes();
    }

    public long getTopicPendingBytes(String topic) {
        if (!running.get()) {
            return 0;
        }
        TopicRingBuffer buffer = memoryManager.getTopic(topic);
        if (buffer == null) {
            return 0;
        }
        return buffer.getPendingBytes();
    }

    public long getTopicPendingBytes(int topicId, String subscriberId) {
        if (!running.get()) {
            return 0;
        }
        TopicRingBuffer buffer = memoryManager.getTopic(topicId);
        if (buffer == null) {
            return 0;
        }
        return buffer.getSubscriberPendingBytes(subscriberId);
    }

    public long getTopicPendingBytes(String topic, String subscriberId) {
        if (!running.get()) {
            return 0;
        }
        TopicRingBuffer buffer = memoryManager.getTopic(topic);
        if (buffer == null) {
            return 0;
        }
        return buffer.getSubscriberPendingBytes(subscriberId);
    }

    public int getMaxDataSize() {
        return maxDataSize;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public MemoryStats getStats() {
        return memoryManager.getStats();
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;

        logger.info("Closing JSharedMem: {}", name);

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Executor did not terminate in time");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        NativeBridge.close();
        logger.info("JSharedMem closed: {}", name);
    }

    private byte[] toBytes(Object data) {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }

        if (data instanceof byte[]) {
            return (byte[]) data;
        }

        if (data instanceof String) {
            return ((String) data).getBytes(StandardCharsets.UTF_8);
        }

        if (data instanceof Integer) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt((Integer) data);
            return buffer.array();
        }

        if (data instanceof Long) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong((Long) data);
            return buffer.array();
        }

        if (data instanceof Double) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putDouble((Double) data);
            return buffer.array();
        }

        if (data instanceof Float) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putFloat((Float) data);
            return buffer.array();
        }

        if (data instanceof Boolean) {
            return new byte[]{(byte) (((Boolean) data) ? 1 : 0)};
        }

        if (data instanceof Short) {
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.putShort((Short) data);
            return buffer.array();
        }

        if (data instanceof Character) {
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.putChar((Character) data);
            return buffer.array();
        }

        if (data instanceof UUID) {
            UUID uuid = (UUID) data;
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            return buffer.array();
        }

        throw new IllegalArgumentException("Unsupported data type: " + data.getClass());
    }

    @SuppressWarnings("unchecked")
    public <T> T fromBytes(byte[] data, Class<T> targetType) {
        if (targetType == byte[].class) {
            return (T) data;
        }

        if (targetType == String.class) {
            return (T) new String(data, StandardCharsets.UTF_8);
        }

        if (targetType == Integer.class || targetType == int.class) {
            return (T) Integer.valueOf(ByteBuffer.wrap(data).getInt());
        }

        if (targetType == Long.class || targetType == long.class) {
            return (T) Long.valueOf(ByteBuffer.wrap(data).getLong());
        }

        if (targetType == Double.class || targetType == double.class) {
            return (T) Double.valueOf(ByteBuffer.wrap(data).getDouble());
        }

        if (targetType == Float.class || targetType == float.class) {
            return (T) Float.valueOf(ByteBuffer.wrap(data).getFloat());
        }

        if (targetType == Boolean.class || targetType == boolean.class) {
            return (T) Boolean.valueOf(data[0] != 0);
        }

        if (targetType == Short.class || targetType == short.class) {
            return (T) Short.valueOf(ByteBuffer.wrap(data).getShort());
        }

        if (targetType == Character.class || targetType == char.class) {
            return (T) Character.valueOf(ByteBuffer.wrap(data).getChar());
        }

        if (targetType == UUID.class) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            return (T) new UUID(buffer.getLong(), buffer.getLong());
        }

        throw new IllegalArgumentException("Unsupported target type: " + targetType);
    }

    private void checkRunning() {
        if (!running.get()) {
            throw new IllegalStateException("JSharedMem is already closed");
        }
    }

    private void validateTopicId(int topicId) {
        if (topicId <= 0) {
            throw new IllegalArgumentException("Topic ID must be positive: " + topicId);
        }
    }

    private void validateTopicString(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (topic.length() > 255) {
            throw new IllegalArgumentException("Topic too long: " + topic.length() + " (max 255)");
        }
    }

    private void validateData(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }
        if (data.length > maxDataSize) {
            throw new IllegalArgumentException("Data too large: " + data.length + " bytes (max " + maxDataSize + ")");
        }
    }

    private void validateListener(MessageListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be null");
        }
    }

    private void validateListener(MessageWithMetaListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be null");
        }
    }

    @FunctionalInterface
    public interface MessageListener {
        void onMessage(int topicId, byte[] data);
    }

    @FunctionalInterface
    public interface MessageWithMetaListener {
        void onMessage(int topicId, byte[] data, long timestamp, long ttl);
    }
}