# JSharedMem - 工程樣品 純屬研究

## 簡介
JSharedMem 是一個基於共享記憶體 (Shared Memory) 的 Java 內部進程通訊庫，支援發布/訂閱 (Pub/Sub) 模式。  
使用 `sun.misc.Unsafe` 與 JNA 直接操作記憶體，可在 Windows 與 Linux 上跨平台運作。  
**本專案為個人研究與實驗用途，屬於工程樣品，請勿用於正式環境。**

## 功能特點
- **跨平台** – Windows 使用 Memory Mapped File (kernel32)，Linux 使用 POSIX shm_open/mmap
- **雙主題模式** – 支援整數 `int` 主題 ID 與字串 `String` 主題名稱
- **多訂閱者** – 每個主題最多 32 個訂閱者，各自獨立的讀取進度
- **訊息過期 (TTL)** – 發布時可設定存活時間，過期訊息自動跳過
- **元資料存取** – 可連同時間戳與 TTL 一併讀取訊息
- **回調與監聽器** – 提供非同步監聽器 (Listener) 模式，內部使用線程池進行輪詢
- **記憶體統計** – 可查詢整體與各主題的記憶體使用量、訊息數量等
- **自動清理** – 會自動跳過無效訊息，並支援手動移除主題

## 系統需求
- Java 8 以上
- 依賴：JNA 5.x、SLF4J（需搭配實作如 logback）
- 作業系統：Windows 或 Linux（需有對應原生函式庫）
- 執行權限：需能建立/開啟共享記憶體物件

## 快速開始

### 1. 取得原始碼
```bash
git clone https://github.com/SeanMud0319/JSharedMem.git
cd JSharedMem
```

### 2. 建置並安裝到本地 Maven 儲存庫
```bash
mvn clean install
```

### 3. 加入專案依賴
在你的 `pom.xml` 中加入以下依賴（請根據實際建置的 groupId、artifactId 與版本調整）：
```xml
<dependency>
    <groupId>top.nontage</groupId>
    <artifactId>jsharedmem</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

### 4. 連接到共享記憶體
```java
// 預設 1MB 共享記憶體
JSharedMem shm = JSharedMem.connect("my_app");

// 自訂大小，例如 2MB
JSharedMem shm = JSharedMem.connect("my_app", 2 * 1024 * 1024);

// 完整參數：名稱、記憶體大小、單筆最大訊息大小、主題區域預設大小
JSharedMem shm = JSharedMem.connect("my_app", 2 * 1024 * 1024, 256 * 1024, 128 * 1024);
```

### 5. 發布訊息
```java
// 使用字串主題
shm.publish("orders", "New order created!");

// 使用整數主題
shm.publish(1, "Sensor reading: 42.5");

// 指定 TTL（毫秒），例如 10 秒後過期
shm.publish("alerts", "High temperature", 10_000L);
```

### 6. 訂閱訊息
```java
// 字串主題 + 自動產生訂閱者 ID
shm.subscribe("orders", (topicId, data) -> {
    String msg = shm.fromBytes(data, String.class);
    System.out.println("收到: " + msg);
});

// 自行指定訂閱者 ID
shm.subscribe(1, "my_subscriber", (topicId, data) -> {
    double reading = shm.fromBytes(data, double.class);
    System.out.printf("感測器值: %.2f%n", reading);
});
```

### 7. 訂閱並取得元資料
```java
shm.subscribeWithMeta("alerts", (topicId, data, timestamp, ttl) -> {
    String msg = shm.fromBytes(data, String.class);
    System.out.printf("訊息: %s, 時間戳: %d, TTL: %d%n", msg, timestamp, ttl);
});
```

### 8. 查詢統計與管理主題
```java
// 整體統計
MemoryStats stats = shm.getStats();
System.out.println(stats);

// 主題列表
List<String> topics = shm.getTopicList();
System.out.println("現有主題: " + topics);

// 主題詳細資訊
TopicMetadata meta = shm.getTopicMetadata("orders");
if (meta != null) {
    System.out.println("待處理訊息數: " + meta.getPendingCount());
}

// 移除主題
shm.removeTopic("orders");
```

### 9. 關閉連線
```java
shm.close();  // 中斷所有訂閱線程，釋放資源
// 亦支援 try-with-resources (AutoCloseable)
try (JSharedMem shm = JSharedMem.connect("test")) {
    // ...
}
```

## 設定參數說明
| 參數 | 預設值 | 說明 |
|------|--------|------|
| `DEFAULT_MEMORY_SIZE` | 1 MB (1024×1024) | 共享記憶體總大小，必須為 4KB 對齊倍數 |
| `maxDataSize` | 記憶體大小的 10%（最大不超過一半） | 單一訊息允許的最大位元組數 |
| `defaultRegionSize` | 64 KB | 每個主題的記憶體區域大小 |
| 訊息 TTL | 10000 ms (10 秒) | 發布時可自訂，過期後訂閱者會自動跳過 |

## 內部結構概覽
- **全域標頭 (1024 bytes)**：包含魔術數字、版本、已使用大小、主題數量、建立時間等。
- **字串主題映射 (最多 32 個)**：將字串主題名稱與整數 ID 對應，存放在共享記憶體固定偏移處。
- **主題區域 (Region)**：每個主題獨立一段記憶體，包含主題頭、訂閱者表（最多 32 個）及環形緩衝區。
- **訊息格式**：長度 (4 bytes)、時間戳 (8 bytes)、TTL (8 bytes)、實際資料。
- **同步機制**：利用 `Unsafe.compareAndSwap` 進行無鎖寫入。

## 注意事項
- **本程式碼為工程樣品，僅供研究、學習與把玩，穩定性、效能與安全性皆未經完整驗證，切勿用於生產環境。**
- 共享記憶體在程式異常退出後可能殘留，Linux 上可手動刪除 `/dev/shm/<name>`，Windows 通常會自動回收。
- 主題數量上限 32 個（包含字串與整數主題共用 ID 空間）。
- 每個主題訂閱者上限 32 個。
- 訊息大小受限於區域容量與 `maxDataSize`。
- 多進程同時操作時需由應用層確保關閉順序，避免競爭。

## 授權
本專案採用 [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) 授權條款。