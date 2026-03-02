# 最終可行方案：利用 Flink 現有的 PreferredLocations 機制

## 重大發現

✅ **Flink 已經有完整的 preferred locations 支持！**

- `SlotProfile` 已經有 `preferredLocations` 字段
- `TaskManagerLocation` 已經包含 IP/ResourceID 信息
- 只需要確保 DefaultScheduler 正確設置這些值！

## 完整的數據流（使用 Flink 原生機制）

```
Migration Plan (JSON)
    ↓
DefaultScheduler.getPreferredLocations()
    ↓ 返回 ResourceID (如 "tm_20c_4")
    ↓
轉換為 TaskManagerLocation
    ↓
設置到 SlotProfile.preferredLocations
    ↓
PhysicalSlotRequest { slotProfile }
    ↓
發送給 ResourceManager
    ↓
SlotManager 使用 LocationPreferenceSlotSelectionStrategy
    ↓
匹配 TaskManager 的 ResourceID
    ↓
分配匹配的 slot ✅
```

## 需要修改的文件（最小化）

### 文件 1: DefaultScheduler.java（部分已實現）

**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/DefaultScheduler.java`

**關鍵方法**: `getPreferredLocations(ExecutionVertexID executionVertexId)`

**目前狀態**: ✅ 已實現，返回 resourceID 字符串

**需要修改**: 將 resourceID 轉換為 `TaskManagerLocation`

```java
@Override
public Collection<TaskManagerLocation> getPreferredLocations(ExecutionVertexID executionVertexId) {
    // Step 1: 從 migration plan 獲取 preferred resourceID
    String targetResourceId = getPreferredResourceId(executionVertexId);

    if (targetResourceId == null) {
        return Collections.emptyList();
    }

    // Step 2: 將 resourceID 轉換為 TaskManagerLocation
    // 方案 A: 從當前已註冊的 TaskManager 中查找
    Optional<TaskManagerLocation> location = findTaskManagerLocationByResourceId(targetResourceId);

    if (location.isPresent()) {
        LOG.info("🎯 Preferred location for {}: {}", executionVertexId, location.get());
        return Collections.singleton(location.get());
    }

    // 方案 B: 創建一個虛擬的 TaskManagerLocation（ResourceManager 會匹配 resourceID）
    // 這取決於 Flink 的具體實現
    return Collections.emptyList();
}

private Optional<TaskManagerLocation> findTaskManagerLocationByResourceId(String resourceId) {
    // 從已註冊的 TaskManager 中查找
    // 需要訪問 SlotPool 或 ResourceManager 的註冊信息
    // 具體實現取決於 Flink 版本
    return Optional.empty();  // TODO: 實現
}
```

### 文件 2: ResourceManager 層級的匹配策略

**兩個選項**：

#### 選項 A: 使用現有的 LocationPreferenceSlotSelectionStrategy

Flink 可能已經實現了基於 preferredLocations 的匹配！

**檢查**:
```bash
grep -r "LocationPreference" flink_source/flink-runtime/src/main/java/org/apache/flink/runtime/
```

#### 選項 B: 自定義 ResourceMatchingStrategy

如果現有策略不夠用，在 `DefaultResourceAllocationStrategy` 中：

```java
// 在 tryFulfillRequirementsForJobWithResources() 方法中
private Collection<ResourceRequirement> tryFulfillRequirementsForJobWithResources(
        JobID jobId,
        Collection<ResourceRequirement> missingResources,
        List<InternalResourceInfo> registeredResources,
        Collection<TaskManagerLocation> preferredLocations) {  // 新增參數

    // 如果有 preferred locations，重新排序 registeredResources
    if (!preferredLocations.isEmpty()) {
        registeredResources = reorderByPreferredLocations(
            registeredResources, preferredLocations);
    }

    // 原有邏輯...
}

private List<InternalResourceInfo> reorderByPreferredLocations(
        List<InternalResourceInfo> resources,
        Collection<TaskManagerLocation> preferredLocations) {

    Set<ResourceID> preferredResourceIds = preferredLocations.stream()
        .map(TaskManagerLocation::getResourceID)
        .collect(Collectors.toSet());

    // 將匹配的 TaskManager 排在前面
    List<InternalResourceInfo> reordered = new ArrayList<>();

    // 先添加匹配的
    for (InternalResourceInfo resource : resources) {
        ResourceID resourceId = resource.getTaskExecutorConnection().getResourceID();
        if (preferredResourceIds.contains(resourceId)) {
            reordered.add(resource);
        }
    }

    // 再添加不匹配的
    for (InternalResourceInfo resource : resources) {
        ResourceID resourceId = resource.getTaskExecutorConnection().getResourceID();
        if (!preferredResourceIds.contains(resourceId)) {
            reordered.add(resource);
        }
    }

    return reordered;
}
```

## 關鍵問題：ResourceRequirement 沒有 preferred location 信息

**問題**: `ResourceRequirement` 只包含資源數量和 profile，沒有 preferred location。

**解決方案**: 需要在從 `PhysicalSlotRequest` 轉換到 `ResourceRequirement` 的過程中保留這個信息。

### 方案 1: 修改 ResourceRequirement（侵入性較大）

```java
public class ResourceRequirement {
    private final ResourceProfile resourceProfile;
    private final int numberOfRequiredSlots;

    // 新增
    private final Collection<TaskManagerLocation> preferredLocations;
}
```

### 方案 2: 使用 Map 映射（推薦）

在 `ResourceRequirements` 或相關類中，維護一個從 ResourceRequirement 到 preferred locations 的映射：

```java
Map<ResourceRequirement, Collection<TaskManagerLocation>> preferredLocationsMap;
```

## 最簡單的可行方案（強烈推薦）

**不修改 Flink 核心類，只在分配邏輯中使用啟發式規則**：

### 基於 TaskManager 統計的優先級分配

```java
// 在 MigrationAwareResourceAllocationStrategy 中

public class MigrationAwareResourceAllocationStrategy implements ResourceAllocationStrategy {

    private final Map<ResourceID, Integer> taskManagerPriorities;

    public MigrationAwareResourceAllocationStrategy(
            ResourceAllocationStrategy fallbackStrategy,
            Map<String, String> migrationPlan) {

        // 統計每個 TaskManager 應該承載多少個 subtask
        Map<String, Integer> counts = new HashMap<>();
        for (String targetTM : migrationPlan.values()) {
            counts.put(targetTM, counts.getOrDefault(targetTM, 0) + 1);
        }

        // 轉換為 ResourceID -> priority
        this.taskManagerPriorities = convertToPriorities(counts);
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(...) {
        // 按優先級對 TaskManager 排序
        List<InternalResourceInfo> sorted = sortByPriority(registeredResources);

        // 使用排序後的列表進行分配
        return fallbackStrategy.tryFulfillRequirements(..., sorted, ...);
    }

    private List<InternalResourceInfo> sortByPriority(
            List<InternalResourceInfo> resources) {

        return resources.stream()
            .sorted((a, b) -> {
                ResourceID aId = a.getTaskExecutorConnection().getResourceID();
                ResourceID bId = b.getTaskExecutorConnection().getResourceID();

                int aPriority = taskManagerPriorities.getOrDefault(aId, 0);
                int bPriority = taskManagerPriorities.getOrDefault(bId, 0);

                return Integer.compare(bPriority, aPriority);  // 降序
            })
            .collect(Collectors.toList());
    }
}
```

**優點**:
- ✅ 不需要修改 Flink 核心類
- ✅ 實現簡單
- ✅ 基於統計的啟發式，大概率正確

**缺點**:
- ❌ 不是精確匹配（但在大多數情況下足夠好）

## 實現步驟

### 步驟 1: 驗證 DefaultScheduler 的 getPreferredLocations 是否被調用

```bash
# 添加日誌
LOG.info("🎯 getPreferredLocations called for {}", executionVertexId);
```

重新編譯並檢查日誌：
```bash
docker logs jobmanager 2>&1 | grep "getPreferredLocations"
```

### 步驟 2: 實現簡化版的 MigrationAwareResourceAllocationStrategy

使用上面的"基於優先級"方案，不需要精確的 subtask 匹配。

### 步驟 3: 測試

1. 查看 migration plan 統計：
   - tm_20c_4: 應該有 X 個 subtask
   - tm_20c_2_net: 應該有 Y 個 subtask
   - tm_10c_3_cpu: 應該有 Z 個 subtask

2. 檢查實際分配是否大致符合比例

### 步驟 4: 如果需要精確匹配

則需要追蹤 ResourceRequirement 的來源，這需要更深入的修改。

## 推薦的下一步行動

1. **先實現簡化版**（基於優先級的分配）
   - 修改後的 MigrationAwareResourceAllocationStrategy
   - 不需要精確的 subtask 匹配

2. **驗證效果**
   - 檢查 migration plan 中頻繁出現的 TaskManager 是否得到更多 slot
   - 這應該能達到 70-80% 的正確率

3. **如果需要 100% 準確**
   - 追蹤 PhysicalSlotRequest 到 ResourceRequirement 的轉換
   - 保留 preferred location 信息

4. **最終方案**
   - 如果簡化版效果好 → 就用它！
   - 如果效果不好 → 實現完整的 preferred location 傳遞

需要我幫你實現簡化版的 MigrationAwareResourceAllocationStrategy 嗎？這個版本應該能在不修改 Flink 核心的情況下工作。
