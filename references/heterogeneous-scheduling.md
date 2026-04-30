# 異質節點調度策略參考（Flink 1.19）

## 核心機制：SlotSharingGroup + ResourceProfile

### 基本概念

Flink 1.19 中，可以透過以下方式讓不同算子「偏好」不同類型的節點：

1. **SlotSharingGroup**：控制哪些算子共用同一個 slot（同一個 TaskManager）
2. **ResourceProfile**：為每個 slot group 指定所需的 CPU 和記憶體（需要 Resource-aware scheduling 開啟）

---

## 策略一：依計算強度分 SlotSharingGroup

```java
// 計算密集型算子 → 分配到 CPU 強的節點
SlotSharingGroup computeHeavyGroup = SlotSharingGroup.newBuilder("compute-heavy")
    .setCpuCores(4.0)           // 要求 4 個 CPU core
    .setTaskHeapMemory(MemorySize.ofMebiBytes(512))
    .build();

// 資料傳輸型算子 → 對 CPU 要求低，但需要好的網路
SlotSharingGroup networkBoundGroup = SlotSharingGroup.newBuilder("network-bound")
    .setCpuCores(1.0)
    .setTaskHeapMemory(MemorySize.ofMebiBytes(256))
    .build();

// 套用到算子
DataStream<Event> processed = source
    .map(new HeavyComputeFunction())
        .slotSharingGroup(computeHeavyGroup)   // 計算密集
    .filter(new FilterFunction())
        .slotSharingGroup(networkBoundGroup)    // 網路密集
    .keyBy(Event::getKey)
    .process(new StatefulProcess())
        .slotSharingGroup(computeHeavyGroup);   // 有狀態計算，需要 CPU
```

---

## 策略二：動態 Parallelism 依節點能力調整

在 flink-conf.yaml 中，透過 label 區分節點類型：

```yaml
# 強節點的 TaskManager（4 CPU cores）
taskmanager.resource.cpu.cores: 4.0
taskmanager.numberOfTaskSlots: 4

# 弱節點的 TaskManager（1 CPU core）  
taskmanager.resource.cpu.cores: 1.0
taskmanager.numberOfTaskSlots: 1
```

然後在 Job 中動態設定：

```java
// 根據可用 slot 動態調整計算密集型算子的 parallelism
int totalSlots = env.getParallelism();
int computeParallelism = Math.max(1, totalSlots * 2 / 3);  // 2/3 資源給計算
int ioParallelism = totalSlots - computeParallelism;        // 1/3 給 IO

source.setParallelism(ioParallelism)
    .map(new ComputeFunction()).setParallelism(computeParallelism)
    .addSink(sink).setParallelism(ioParallelism);
```

---

## 策略三：自訂 TaskManagerFilter（進階）

若需要精確控制哪個 Subtask 在哪個節點上執行（適合邊緣場景）：

Flink 1.19 不支援直接的節點親和性 API，但可以透過以下間接方式：

### 方法 A：利用 task.manager.resource.id 標籤

在 TaskManager 設定中加上自訂標籤：
```yaml
# 強節點
metrics.reporter.influxdb.tags.nodeType: strong

# 弱節點  
metrics.reporter.influxdb.tags.nodeType: weak
```

然後在 JobManager 的 scheduling strategy 中讀取這些標籤來分配。

### 方法 B：手動控制 parallelism 讓分配可預測

```java
// 強節點固定有 4 個 slot，弱節點固定有 1 個 slot
// 計算密集型算子 parallelism = 4（剛好填滿強節點的 4 個 slot）
// Flink 的 round-robin 分配會讓這些 subtask 集中在有足夠 slot 的 TaskManager
streamOp.setParallelism(4)
        .slotSharingGroup("compute-heavy");
```

---

## 網路感知調度建議

在邊緣環境中，相鄰算子最好在同一個 TaskManager 執行以減少網路傳輸：

```java
// 使用 chain 將網路敏感的算子鏈結在同一個 thread 執行
source
    .map(new DeserializeFunction())    // 與 source chain 在一起
    .filter(new PreFilterFunction())   // 也 chain 在一起（減少網路 hop）
    // 到這裡才跨節點傳輸
    .keyBy(Event::getKey)
    .process(new StatefulProcessFunction())
        .disableChaining()  // 這個算子單獨一個 task，可以獨立調度到強節點
```

---

## flink-conf.yaml 邊緣環境推薦設定

```yaml
# 啟用 resource-aware scheduling
cluster.fine-grained-resource-management.enabled: true

# 邊緣環境網路不穩定，增加心跳超時容忍
heartbeat.timeout: 60000
heartbeat.interval: 10000

# 網路 buffer 調整（邊緣環境頻寬有限）
taskmanager.network.memory.fraction: 0.1
taskmanager.network.memory.min: 64mb
taskmanager.network.memory.max: 1gb

# 降低 checkpointing 頻率以減少網路壓力
execution.checkpointing.interval: 60000
execution.checkpointing.timeout: 120000

# 邊緣節點離線容忍
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 5
restart-strategy.fixed-delay.delay: 10s
```

---

## 常見錯誤設定

| 錯誤 | 原因 | 修正 |
|------|------|------|
| 所有算子用同一個 SlotSharingGroup | 無法區分節點類型 | 依計算強度分 group |
| Source parallelism = 1 | 成為整個 job 的瓶頸入口 | 至少 = Kafka partition 數 |
| global parallelism 設定過高 | 超過 slot 數量，task 排隊 | parallelism ≤ 總 slot 數 |
| Checkpoint interval 太短 | 在頻寬小的邊緣網路造成大量 state 傳輸 | 至少 30-60 秒 |
