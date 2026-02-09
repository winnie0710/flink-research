# Flink ResourceManager 層級的 Resource-ID 匹配實現指南

## 概述

本指南說明如何在 Flink 的 ResourceManager 層級實現 resource-id 匹配邏輯，使得 Fine-Grained Slot Manager 能夠根據 migration_plan.json 將 subtask 分配到指定的 TaskManager。

## 為什麼需要在 ResourceManager 層級修改？

當 ResourceProfile 為 UNKNOWN 時，Fine-Grained Slot Manager 會繞過 JobMaster 的 SlotPool，直接進行資源分配。因此必須在 ResourceManager 層級添加 preferred resource-id 匹配邏輯。

## 修改步驟

### 1. 添加 PreferredResourceMatchingStrategy.java

文件位置：`flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/PreferredResourceMatchingStrategy.java`

已創建於：`structure_setup/flink_modifications/PreferredResourceMatchingStrategy.java`

這個策略會：
- 讀取 `/opt/flink/plan/migration_plan.json`
- 優先將 slot 分配到 migration plan 中指定的 resource-id
- 如果 preferred resource 不可用，fallback 到原本的策略

### 2. 修改 DefaultResourceAllocationStrategy.java

文件位置：`flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/DefaultResourceAllocationStrategy.java`

#### 修改點 1: 添加構造函數參數

在構造函數中添加一個 boolean 參數來啟用 preferred resource matching：

```java
public DefaultResourceAllocationStrategy(
        ResourceProfile totalResourceProfile,
        int numSlotsPerWorker,
        TaskManagerLoadBalanceMode taskManagerLoadBalanceMode,
        Time taskManagerTimeout,
        int redundantTaskManagerNum,
        CPUResource minTotalCPU,
        MemorySize minTotalMemory,
        boolean enablePreferredResourceMatching) {  // 新增參數

    this.totalResourceProfile = totalResourceProfile;
    this.numSlotsPerWorker = numSlotsPerWorker;
    this.defaultSlotResourceProfile =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    totalResourceProfile, numSlotsPerWorker);

    // 根據配置選擇策略
    ResourceMatchingStrategy baseStrategy =
            taskManagerLoadBalanceMode == TaskManagerLoadBalanceMode.SLOTS
                    ? LeastUtilizationResourceMatchingStrategy.INSTANCE
                    : AnyMatchingResourceMatchingStrategy.INSTANCE;

    // 如果啟用 preferred resource matching，使用包裝策略
    this.availableResourceMatchingStrategy = enablePreferredResourceMatching
            ? new PreferredResourceMatchingStrategy(baseStrategy)
            : baseStrategy;

    this.taskManagerTimeout = taskManagerTimeout;
    this.redundantTaskManagerNum = redundantTaskManagerNum;
    this.minTotalCPU = minTotalCPU;
    this.minTotalMemory = minTotalMemory;
}
```

### 3. 修改 FineGrainedSlotManager.java

文件位置：`flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/FineGrainedSlotManager.java`

在創建 DefaultResourceAllocationStrategy 時傳入 `enablePreferredResourceMatching = true`：

```java
// 查找創建 ResourceAllocationStrategy 的位置
// 通常在 FineGrainedSlotManager 的構造函數或工廠方法中

ResourceAllocationStrategy strategy = new DefaultResourceAllocationStrategy(
        totalResourceProfile,
        numSlotsPerWorker,
        taskManagerLoadBalanceMode,
        taskManagerTimeout,
        redundantTaskManagerNum,
        minTotalCPU,
        minTotalMemory,
        true  // 啟用 preferred resource matching
);
```

### 4. 添加 InternalResourceInfo 的 getResourceId() 方法（如果不存在）

查看 `InternalResourceInfo` 類，確保它有 `getResourceId()` 方法：

```java
public ResourceID getResourceId() {
    return this.taskManagerConnection.getResourceID();
}
```

### 5. 配置文件修改

在 `flink-conf.yaml` 中確保 resource-id 已設置（Docker Compose 已完成）：

```yaml
taskmanager.resource-id: tm-20c-1
```

## Migration Plan JSON 格式

`/opt/flink/plan/migration_plan.json`:

```json
{
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_0": "tm-20c-1",
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_1": "tm-20c-4",
  "Window_Max____Map_0": "tm-10c-3-cpu"
}
```

## 工作流程

1. **檢測階段** (detector.py):
   - 檢測過載的 subtask
   - 選擇負載最低的 TaskManager (resource-id)
   - 生成 migration_plan.json

2. **重啟階段**:
   - 使用 savepoint 停止 Job
   - 從 savepoint 重新啟動 Job

3. **分配階段** (ResourceManager):
   - FineGrainedSlotManager 調用 ResourceAllocationStrategy
   - PreferredResourceMatchingStrategy 讀取 migration_plan.json
   - 優先將 slot 分配到 preferred resource-id
   - 如果 preferred resource 不可用，使用原策略

## 日誌檢查

啟用後，應該能在 JobManager 日誌中看到：

```
INFO PreferredResourceMatchingStrategy - Loaded migration plan with 3 entries
INFO PreferredResourceMatchingStrategy -   Source:_..._0 -> tm-20c-1
INFO PreferredResourceMatchingStrategy - Attempting to allocate on preferred resource: tm-20c-1
INFO PreferredResourceMatchingStrategy - Successfully allocated slot on preferred resource: tm-20c-1
```

## 注意事項

1. **ResourceID 格式**：確保 `ResourceID.getResourceIdString()` 返回的格式與 docker-compose.yml 中設置的 resource-id 一致

2. **Fallback 機制**：如果 preferred resource 不可用（滿載或不存在），策略會自動 fallback 到原本的分配邏輯

3. **動態更新**：migration plan 每 5 秒重新加載一次，支持動態更新

4. **Thread Safety**：PreferredResourceMatchingStrategy 在 FineGrainedSlotManager 的主線程中運行，不需要額外的同步

## 測試

1. 啟動 Flink 集群並提交 Job
2. 使用 detector.py 檢測瓶頸並生成 migration plan
3. 使用 savepoint 重啟 Job
4. 檢查日誌確認 slot 被分配到 preferred resource
5. 使用 Flink REST API 驗證 subtask 位置：
   ```bash
   curl http://localhost:8081/jobs/<job-id>/vertices/<vertex-id>
   ```

## 故障排除

### 問題 1: Migration plan 未被讀取
- 檢查文件路徑：`/opt/flink/plan/migration_plan.json`
- 檢查文件權限：確保 Flink 進程可讀
- 查看日誌中的 "Loaded migration plan" 訊息

### 問題 2: Slot 未分配到 preferred resource
- 檢查 resource-id 格式是否匹配
- 確認 preferred resource 有足夠的可用資源
- 查看 "Attempting to allocate" 和 "Successfully allocated" 日誌

### 問題 3: ResourceID 格式不匹配
- 打印 `resourceId.getResourceIdString()` 查看實際格式
- 確認與 migration_plan.json 中的格式一致
- 必要時修改 docker-compose.yml 中的 resource-id 設置
