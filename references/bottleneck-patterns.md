# Flink 邊緣環境瓶頸模式參考

## 常見瓶頸偵測失效原因

### 1. 只看 job-level latency，沒有 operator-level 分解

**問題**：用 `jobLatency = System.currentTimeMillis() - event.timestamp` 只能知道整體慢，不知道哪個算子是瓶頸。

**正確做法**：使用 Flink Metrics API 取得每個算子的 `numRecordsOutPerSecond` 和 backpressure 比例：

```java
getRuntimeContext().getMetricGroup()
    .gauge("subtaskBackpressureRatio", () -> backpressureRatio);
```

---

### 2. Backpressure 偵測邏輯錯誤

**問題**：很多實作用 queue 長度來判斷 backpressure，但在邊緣環境網路波動大時，queue 長度可能是網路問題而非算子本身過載。

**區分方式**：
- `inputBufferUsage` 高 + `outputBufferUsage` 低 → 算子本身是瓶頸
- `inputBufferUsage` 低 + `outputBufferUsage` 高 → 下游或網路是瓶頸

---

### 3. 在異質節點上用固定閾值

**問題**：設定 `if (latency > 100ms) → 瓶頸`，但 CPU 弱的節點即使正常工作也可能超過此閾值，而 CPU 強的節點即使過載也可能在閾值內。

**正確做法**：改用相對指標——與該節點歷史基準值比較，或與同類算子的其他 Subtask 比較。

```java
// 相對偵測：比同類 Subtask 慢超過 2 倍才算瓶頸
double avgThroughput = peerSubtasks.stream()
    .mapToDouble(s -> s.getThroughput())
    .average()
    .orElse(1.0);
boolean isBottleneck = myThroughput < avgThroughput * 0.5;
```

---

### 4. 忽略 Data Skew 造成的假性瓶頸

**問題**：keyBy 後某個 key 的資料量遠大於其他 key，導致負責該 key 的 Subtask 過載，但偵測邏輯把它當成節點問題而非資料傾斜問題。

**識別方式**：比較同一算子的不同 Subtask 的 `numRecordsIn`——如果差異超過 3 倍，是 data skew 而非節點問題。

---

## Flink 1.19 可用的 Metrics API

```java
// 在 RichFunction 中取得 metrics
MetricGroup metrics = getRuntimeContext().getMetricGroup();

// 計數器
Counter recordsProcessed = metrics.counter("recordsProcessed");

// Gauge（瞬時值）
metrics.gauge("queueLength", () -> queue.size());

// Histogram（分佈）
Histogram latencyHistogram = metrics.histogram("processingLatency", 
    new DescriptiveStatisticsHistogram(1000));

// Meter（速率）
Meter throughput = metrics.meter("throughput", new MeterView(60));
```

---

## 邊緣環境特有的瓶頸模式

### 網路分區瓶頸
當某個邊緣節點與其他節點的網路延遲突然升高（例如從 5ms 到 50ms），上游算子的 outputBuffer 會積累，看起來像上游算子過載，實際上是網路問題。

**識別**：同時監控 `numBytesOutPerSecond` 和 `numBuffersOut`——如果 bytes 低但 buffers 高，是網路問題。

### 節點離線造成的重新分配瓶頸
邊緣節點突然離線後，其 Subtask 被重新分配到其他節點，造成短暫過載。這在 latency 曲線上看起來是突發的 spike 而非持續升高。

**識別**：與 TaskManager 生命週期事件關聯，spike 時間與節點離線時間一致。
