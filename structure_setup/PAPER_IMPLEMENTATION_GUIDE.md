# 基於論文的 Migration Plan 實現指南

## 論文的完整方案

根據你提供的論文描述，他們的實現包含**兩個層級的修改**：

### 1. JobMaster 層級
**在 SlotRequest 中添加 preferred IP 信息**

```
JobMaster (DefaultScheduler)
  ↓ 讀取 migration_plan.json
  ↓ 在請求 slot 時，將 preferred IP 添加到 SlotRequest
  ↓ 發送給 ResourceManager
```

### 2. ResourceManager 層級
**在 SlotManager 中匹配 IP 信息**

```
ResourceManager (SlotManager)
  ↓ 接收到包含 preferred IP 的 SlotRequest
  ↓ 比對已註冊的 TaskManager
  ↓ 找到 IP 匹配的 TaskManager
  ↓ 分配該 TaskManager 的 slot
```

### 3. TaskManager 層級
**在 ResourceProfile 中添加 IP 信息**

```
TaskManager
  ↓ 讀取自己的 IP 或 resource-id
  ↓ 將 IP 添加到 ResourceProfile
  ↓ 向 ResourceManager 註冊時攜帶此信息
```

## 完整的數據流

```
Migration Plan (JSON)
    ↓
JobMaster 讀取
    ↓
創建 SlotRequest { preferredIP: "tm_20c_4" }
    ↓
發送給 ResourceManager
    ↓
ResourceManager 接收
    ↓
遍歷已註冊的 TaskManager
    ↓
TaskManager { ResourceProfile { IP: "tm_20c_4" } }  ← 匹配！
    ↓
分配這個 TaskManager 的 slot
    ↓
返回給 JobMaster
```

## 需要修改的文件

### 階段 1: TaskManager 註冊時攜帶 IP

#### 文件 1: `ResourceProfile.java`
**位置**: `flink-core/src/main/java/org/apache/flink/runtime/clusterframework/types/ResourceProfile.java`

**修改內容**：添加 IP 或 resourceId 字段

```java
public class ResourceProfile implements Serializable {
    // 現有字段...
    private final CPUResource cpuCores;
    private final MemorySize taskHeapMemory;

    // 新增字段
    private final String resourceId;  // 或 preferredIp

    // 修改構造函數和 builder
    public static class Builder {
        private String resourceId;

        public Builder setResourceId(String resourceId) {
            this.resourceId = resourceId;
            return this;
        }
    }

    public String getResourceId() {
        return resourceId;
    }
}
```

#### 文件 2: `TaskExecutor.java` 或 `TaskManagerRunner.java`
**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/TaskExecutor.java`

**修改內容**：在向 ResourceManager 註冊時，將 resource-id 添加到 ResourceProfile

```java
private void registerAtResourceManager() {
    // 讀取配置的 resource-id
    String resourceId = configuration.getString(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID);

    // 創建包含 resourceId 的 ResourceProfile
    ResourceProfile totalResource = ResourceProfile.newBuilder()
        .setCpuCores(...)
        .setTaskHeapMemory(...)
        .setResourceId(resourceId)  // 新增
        .build();

    // 註冊到 ResourceManager
    resourceManagerGateway.registerTaskExecutor(..., totalResource, ...);
}
```

### 階段 2: JobMaster 在 SlotRequest 中添加 preferred IP

#### 文件 3: `SlotRequest.java`
**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/slotpool/SlotRequest.java`

**修改內容**：添加 preferred location 字段

```java
public class SlotRequest {
    private final SlotRequestId slotRequestId;
    private final ResourceProfile resourceProfile;

    // 新增字段
    private final String preferredResourceId;  // 或 preferredLocation

    public SlotRequest(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            String preferredResourceId) {  // 新增參數
        this.slotRequestId = slotRequestId;
        this.resourceProfile = resourceProfile;
        this.preferredResourceId = preferredResourceId;
    }

    public String getPreferredResourceId() {
        return preferredResourceId;
    }
}
```

#### 文件 4: `DefaultScheduler.java` (已部分實現)
**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/DefaultScheduler.java`

**修改內容**：在創建 SlotRequest 時添加 preferred location

```java
private SlotRequest createSlotRequest(ExecutionVertexID executionVertexId) {
    // 從 migration plan 獲取 preferred location
    String preferredResourceId = getPreferredLocations(executionVertexId)
        .orElse(null);

    // 創建包含 preferred location 的 SlotRequest
    return new SlotRequest(
        slotRequestId,
        resourceProfile,
        preferredResourceId  // 傳遞 preferred location
    );
}
```

### 階段 3: ResourceManager 匹配 IP 並分配 Slot

#### 文件 5: `FineGrainedSlotManager.java` 或創建新的匹配策略
**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/FineGrainedSlotManager.java`

**修改內容**：在處理 SlotRequest 時，優先分配匹配的 TaskManager

```java
public void processResourceRequirements(ResourceRequirements resourceRequirements) {
    for (ResourceRequirement requirement : resourceRequirements.getResourceRequirements()) {
        // 獲取 preferred location（從某處傳遞過來）
        String preferredResourceId = getPreferredResourceId(requirement);

        if (preferredResourceId != null) {
            // 優先在指定的 TaskManager 上分配
            Optional<TaskManagerInfo> preferredTM =
                findTaskManagerByResourceId(preferredResourceId);

            if (preferredTM.isPresent() && hasAvailableSlot(preferredTM.get())) {
                allocateSlot(preferredTM.get(), requirement);
                continue;
            }
        }

        // Fallback: 使用默認策略
        allocateSlotWithDefaultStrategy(requirement);
    }
}

private Optional<TaskManagerInfo> findTaskManagerByResourceId(String resourceId) {
    for (TaskManagerInfo tm : registeredTaskManagers) {
        String tmResourceId = tm.getTotalResource().getResourceId();
        if (resourceId.equals(tmResourceId)) {
            return Optional.of(tm);
        }
    }
    return Optional.empty();
}
```

## 實現步驟

### 步驟 1: 修改 ResourceProfile（核心）

```bash
cd /home/yenwei/research/flink_source
```

**1.1 找到 ResourceProfile.java**
```bash
find . -name "ResourceProfile.java" -path "*/clusterframework/types/*"
```

**1.2 添加 resourceId 字段**
- 在 ResourceProfile 類中添加 `private final String resourceId`
- 更新 Builder 模式
- 更新序列化/反序列化邏輯

### 步驟 2: 修改 TaskManager 註冊邏輯

**2.1 找到 TaskExecutor.java**
```bash
find . -name "TaskExecutor.java"
```

**2.2 在註冊時添加 resource-id**
- 從配置讀取 `taskmanager.resource-id`
- 創建 ResourceProfile 時設置 resourceId

### 步驟 3: 修改 SlotRequest

**3.1 找到 SlotRequest 相關類**
```bash
find . -name "*SlotRequest*.java" -path "*/jobmaster/*"
```

**3.2 添加 preferredResourceId 字段**

### 步驟 4: 修改 DefaultScheduler

**4.1 在創建 SlotRequest 時使用 migration plan**
- 從 MigrationPlanReader 獲取 preferred location
- 傳遞給 SlotRequest

### 步驟 5: 修改 ResourceManager 匹配邏輯

**5.1 創建新的匹配策略**
```bash
# 可以基於你之前創建的 MigrationAwareResourceAllocationStrategy
# 但這次有了 preferred location 信息
```

**5.2 在 SlotManager 中使用 preferred location**
- 接收 SlotRequest 中的 preferredResourceId
- 匹配 TaskManager 的 ResourceProfile.resourceId
- 優先分配匹配的 TaskManager

## 關鍵點

### ✅ 為什麼論文的方案可行？

1. **端到端的信息傳遞**
   - TaskManager 註冊時攜帶 IP → ResourceManager 知道每個 TM 的 IP
   - JobMaster 請求時攜帶 preferred IP → ResourceManager 知道要分配到哪個 TM
   - **信息完整傳遞，不丟失！**

2. **在正確的層級做正確的事**
   - JobMaster: 知道 subtask → 知道 preferred location
   - ResourceManager: 知道 TaskManager → 知道每個 TM 的 IP
   - 匹配在 ResourceManager 完成：**有足夠的信息！**

### ❌ 為什麼你之前的方案不行？

1. **信息丟失**
   - JobMaster 的 preferred location 沒有傳遞給 ResourceManager
   - ResourceManager 只收到 "需要 X 個 slot"，不知道是給哪個 subtask 的

2. **在錯誤的層級做事**
   - 試圖在 ResourceAllocationStrategy 層級匹配 subtask
   - 但這個層級根本沒有 subtask 信息！

## 簡化的實現方案

如果完整實現太複雜，可以使用簡化方案：

### 方案 A: 只修改匹配邏輯（最小改動）

**假設 SlotRequest 已經包含 preferred location** (通過 TaskManagerLocation)：

只需修改 `ResourceMatchingStrategy`，優先匹配 preferred location。

### 方案 B: 使用現有的 TaskManagerLocation

Flink 已經有 `TaskManagerLocation` 類，可能已經包含 IP 信息。

檢查是否可以直接使用：
```bash
grep -r "TaskManagerLocation" flink_source/flink-runtime/src/main/java/
```

## 下一步建議

1. **先檢查 Flink 現有機制**
   ```bash
   # 查看 SlotRequest 是否已經支持 preferred location
   grep -A20 "class SlotRequest" flink_source/flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/slotpool/

   # 查看 ResourceProfile 的字段
   grep -A30 "class ResourceProfile" flink_source/flink-core/src/main/java/org/apache/flink/runtime/clusterframework/types/
   ```

2. **確定需要修改的最小範圍**
   - 如果 SlotRequest 已經支持 preferred location → 只需修改匹配邏輯
   - 如果沒有 → 需要完整實現論文方案

3. **逐步實現**
   - 第一步：TaskManager 註冊時添加 resource-id
   - 第二步：驗證 ResourceManager 能看到這個信息
   - 第三步：JobMaster 在 SlotRequest 中添加 preferred location
   - 第四步：ResourceManager 匹配邏輯
   - 第五步：端到端測試

需要我幫你檢查 Flink 現有的 SlotRequest 和 ResourceProfile 實現，看看需要做哪些最小修改嗎？
