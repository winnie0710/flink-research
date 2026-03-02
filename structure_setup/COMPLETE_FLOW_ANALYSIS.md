# 完整流程分析：從 Migration Plan 到精確 Subtask 分配

## 概述

這個文檔詳細說明了從讀取 migration plan 到精確分配 subtask 的完整流程，以及 JobMaster 與 ResourceManager 之間如何溝通和傳遞 ResourceID 信息。

---

## 🔄 完整數據流程

```
Migration Plan (JSON)
        ↓
[1] DefaultScheduler 初始化
        ↓
[2] JobMaster: SlotProfile 創建 (帶 preferred TaskManagerLocation)
        ↓
[3] JobMaster: 聲明資源需求 (ResourceRequirements)
        ↓
[4] ResourceManager: 接收需求並分配資源
        ↓
[5] TaskManager 註冊並提供 Slots
        ↓
[6] JobMaster: 從可用 Slots 中選擇匹配的
        ↓
[7] 精確分配 Subtask 到目標 TaskManager
```

---

## 📝 詳細流程分解

### **階段 1: Migration Plan 讀取與初始化**

#### 文件位置
`DefaultScheduler.java` (構造函數)

#### 代碼路徑
```java
// Line 173-176 in DefaultScheduler.java
this.migrationPlanReader = new MigrationPlanReader();
this.migrationPlan = migrationPlanReader.readMigrationPlan();
log.info("Loaded migration plan with {} entries for job {}",
        migrationPlan.size(), jobGraph.getJobID());
```

#### 數據內容
```json
{
  "Source: Custom Source_0": "tm-source",
  "Source: Custom Source_1": "tm-source",
  "Timestamps/Watermarks_0": "tm-compute-1",
  "Timestamps/Watermarks_1": "tm-compute-2"
}
```

**Migration Plan 存儲在**: `DefaultScheduler.migrationPlan` (Map<String, String>)

---

### **階段 2: Slot 分配請求 (JobMaster 側)**

當 DefaultScheduler 需要調度 execution vertices 時：

#### 2.1 調度策略觸發
```java
// DefaultScheduler.java
allocateSlotsAndDeploy(List<ExecutionVertexID> verticesToDeploy)
    ↓
executionSlotAllocator.allocateSlotsFor(executionAttemptIds)
```

#### 2.2 SlotProfile 創建

**文件**: `SlotSharingExecutionSlotAllocatorFactory.java`

```java
// Line 79-94: 創建 MigrationPlanAwarePreferredLocationsRetriever
SyncPreferredLocationsRetriever defaultRetriever =
        new DefaultSyncPreferredLocationsRetriever(context, context);

SyncPreferredLocationsRetriever preferredLocationsRetriever;
if (slotProvider instanceof SlotPool) {
    LOG.info("🎯 Using MigrationPlanAwarePreferredLocationsRetriever");
    preferredLocationsRetriever = new MigrationPlanAwarePreferredLocationsRetriever(
            defaultRetriever,
            context,  // 包含 getPreferredIp() 方法
            (SlotPool) slotProvider);
}
```

#### 2.3 獲取 Preferred Location

**文件**: `MigrationPlanAwarePreferredLocationsRetriever.java`

```java
// Line 66-80: getPreferredLocations()
public Collection<TaskManagerLocation> getPreferredLocations(
        ExecutionVertexID executionVertexId,
        Set<ExecutionVertexID> producersToIgnore) {

    // ✅ 步驟 1: 從 migration plan 獲取 preferred ResourceID
    String preferredResourceId = context.getPreferredIp(executionVertexId);
    //   → 調用 DefaultScheduler.getPreferredIp()
    //   → 查找 migrationPlan: "Source: Custom Source_0" → "tm-source"

    if (preferredResourceId != null) {
        // ✅ 步驟 2: 將 ResourceID 轉換為 TaskManagerLocation
        Optional<TaskManagerLocation> location =
                convertResourceIdToTaskManagerLocation(preferredResourceId);

        if (location.isPresent()) {
            return Collections.singleton(location.get());
        }
    }

    // Fallback 到默認行為
    return fallbackRetriever.getPreferredLocations(executionVertexId, producersToIgnore);
}
```

#### 2.4 ResourceID → TaskManagerLocation 轉換

```java
// Line 102-145: convertResourceIdToTaskManagerLocation()
private Optional<TaskManagerLocation> convertResourceIdToTaskManagerLocation(
        String resourceIdString) {

    ResourceID targetResourceId = new ResourceID(resourceIdString);  // "tm-source"

    // 查詢 SlotPool 中的可用 slots
    Collection<SlotInfo> availableSlots = slotPool.getAvailableSlotsInformation();

    for (SlotInfo slotInfo : availableSlots) {
        TaskManagerLocation location = slotInfo.getTaskManagerLocation();
        ResourceID resourceId = location.getResourceID();

        if (resourceId.equals(targetResourceId)) {
            // 找到匹配的 TaskManager!
            // TaskManagerLocation 包含: ResourceID, hostname, port, dataPort 等
            return Optional.of(location);
        }
    }

    // 也檢查已分配的 slots
    // ...
}
```

#### 2.5 創建 SlotProfile

**文件**: `MergingSharedSlotProfileRetrieverFactory.java`

```java
// Line 96-119: getSlotProfile()
public SlotProfile getSlotProfile(
        ExecutionSlotSharingGroup executionSlotSharingGroup,
        ResourceProfile physicalSlotResourceProfile) {

    Collection<TaskManagerLocation> preferredLocations = new ArrayList<>();

    for (ExecutionVertexID execution : executionSlotSharingGroup.getExecutionVertexIds()) {
        // 獲取 preferred locations (來自 MigrationPlanAwarePreferredLocationsRetriever)
        preferredLocations.addAll(
                preferredLocationsRetriever.getPreferredLocations(
                        execution, producersToIgnore));
    }

    // ✅ 創建帶有 preferred locations 的 SlotProfile
    return SlotProfile.priorAllocation(
            physicalSlotResourceProfile,
            physicalSlotResourceProfile,
            preferredLocations,  // ← 包含 TaskManagerLocation (tm-source)
            priorAllocations,
            reservedAllocationIds);
}
```

**關鍵**: SlotProfile 現在包含了 preferred TaskManagerLocation!

---

### **階段 3: JobMaster → ResourceManager 通信**

#### ⚠️ 重要發現：Declarative Slot Pool 架構

在 Flink 1.19 的 **Declarative Slot Pool** 架構中：

#### 3.1 資源需求聚合

**文件**: `DefaultDeclarativeSlotPool.java`

```java
// Line 164-175: getResourceRequirements()
public Collection<ResourceRequirement> getResourceRequirements() {
    Collection<ResourceRequirement> currentResourceRequirements = new ArrayList<>();

    // 從 totalResourceRequirements 獲取聚合的需求
    for (Map.Entry<ResourceProfile, Integer> resourceRequirement :
            totalResourceRequirements.getResourcesWithCount()) {
        currentResourceRequirements.add(
                ResourceRequirement.create(
                        resourceRequirement.getKey(),    // ResourceProfile
                        resourceRequirement.getValue())  // count
        );
    }

    return currentResourceRequirements;
}
```

**注意**: 這裡創建的 ResourceRequirement **不包含** preferred locations!
- 只包含: ResourceProfile + 數量
- **不包含**: 具體的 preferred TaskManagerLocation

#### 3.2 聲明資源需求

**文件**: `DeclarativeSlotPoolService.java`

```java
// Line 300-305: declareResourceRequirements()
private void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
    resourceRequirementServiceConnectionManager.declareResourceRequirements(
            ResourceRequirements.create(jobId, jobManagerAddress, resourceRequirements));
}
```

#### 3.3 ResourceManager 接收

**文件**: `ResourceManager.java`

```java
// Line 573-583: declareResourceRequirements()
public CompletableFuture<Acknowledge> declareResourceRequirements(
        JobMasterId jobMasterId, ResourceRequirements resourceRequirements, Time timeout) {

    return callInMainThread(() -> {
        // 傳遞給 SlotManager
        slotManager.processResourceRequirements(resourceRequirements);
        return Acknowledge.get();
    }, timeout);
}
```

**傳遞的數據**:
```java
ResourceRequirements {
    jobId: <job-id>,
    jobManagerAddress: "akka://...",
    resourceRequirements: [
        ResourceRequirement {
            resourceProfile: ResourceProfile(cpus=1, memory=1024MB),
            numberOfRequiredSlots: 4
            // ❌ 沒有 preferredLocations!
        }
    ]
}
```

---

### **階段 4: ResourceManager 分配資源**

#### 4.1 SlotManager 處理需求

**文件**: `FineGrainedSlotManager.java`

```java
// Line 309: processResourceRequirements()
public void processResourceRequirements(ResourceRequirements resourceRequirements) {
    // 存儲 job 的資源需求
    // 觸發資源分配
}

// Line 641: checkResourceRequirements()
private void checkResourceRequirements() {
    ResourceAllocationResult result =
        resourceAllocationStrategy.tryFulfillRequirements(
            getMissingResources(),
            taskManagerResourceInfoProvider,
            blockedTaskManagerChecker);
}
```

#### 4.2 資源分配策略

**文件**: `DefaultResourceAllocationStrategy.java`

```java
// Line 296-338: tryFulfillRequirementsForJobWithResources()
private Collection<ResourceRequirement> tryFulfillRequirementsForJobWithResources(
        JobID jobId,
        Collection<ResourceRequirement> missingResources,
        List<InternalResourceInfo> registeredResources) {

    for (ResourceRequirement resourceRequirement : missingResources) {
        Collection<TaskManagerLocation> preferredLocations =
                resourceRequirement.getPreferredLocations();

        int numUnfulfilled = resourceRequirement.getNumberOfRequiredSlots();

        // ✅ 如果有 preferred locations (但在當前架構下是空的)
        if (!preferredLocations.isEmpty()) {
            numUnfulfilled = tryAllocateOnPreferredTaskManagers(
                    registeredResources,
                    numUnfulfilled,
                    resourceRequirement.getResourceProfile(),
                    preferredLocations,
                    jobId);
        }

        // 使用默認策略分配
        if (numUnfulfilled > 0) {
            numUnfulfilled = availableResourceMatchingStrategy
                    .tryFulfilledRequirementWithResource(...);
        }
    }
}
```

**關鍵問題**:
- ResourceRequirement 的 `preferredLocations` 在當前實現中是**空的**
- 因為 DeclarativeSlotPool 只聚合資源數量，不保留單個請求的 preferred locations

#### 4.3 ResourceManager 的角色

ResourceManager 做的是：
1. 確保有足夠的 TaskManagers 運行
2. 讓 TaskManagers 向 JobMaster 提供它們的 slots

**ResourceManager 不做精確匹配**，它只是確保資源可用！

---

### **階段 5: TaskManager 註冊並提供 Slots**

#### 5.1 TaskManager 連接

```java
// TaskManager 向 ResourceManager 註冊
TaskManager → ResourceManager: registerTaskManager()
    ↓
ResourceManager 接受並記錄 TaskManager
    ↓
ResourceManager → JobMaster: offerSlots()
```

#### 5.2 Slot Offers

**文件**: `DeclarativeSlotPoolBridge.java`

```java
// JobMaster 收到 slot offers
public Collection<SlotOffer> offerSlots(
        Collection<? extends SlotOffer> offers,
        TaskManagerLocation taskManagerLocation,  // ← 包含 ResourceID!
        TaskManagerGateway taskManagerGateway,
        long currentTime) {

    // DeclarativeSlotPool 決定是否接受這些 slots
}
```

**Slot Offer 包含**:
```java
SlotOffer {
    allocationId: <allocation-id>,
    taskManagerLocation: TaskManagerLocation {
        resourceId: "tm-source",      // ← 關鍵！
        hostname: "taskmanager-1",
        port: 6121,
        dataPort: 6122
    }
}
```

---

### **階段 6: JobMaster 選擇匹配的 Slot** ⭐

這是**精確匹配的關鍵階段**！

#### 6.1 Slot 匹配過程

**文件**: `DefaultDeclarativeSlotPool.java`

```java
// 當 TaskManager 提供 slots 時
public Collection<SlotOffer> offerSlots(...) {
    // 對每個 offered slot
    for (SlotOffer offer : offers) {
        // 嘗試匹配 pending requests
        Optional<PendingRequest> matchingRequest =
            findMatchingPendingRequest(offer, taskManagerLocation);
    }
}
```

#### 6.2 PhysicalSlotRequest 匹配

**文件**: `PhysicalSlotRequestBulkImpl.java` 或類似文件

```java
// 檢查 slot 是否匹配 request 的 SlotProfile
boolean matches(SlotOffer offer, TaskManagerLocation location) {
    SlotProfile slotProfile = request.getSlotProfile();

    // ✅ 檢查 preferred locations
    Collection<TaskManagerLocation> preferredLocations =
        slotProfile.getPreferredLocations();

    if (!preferredLocations.isEmpty()) {
        // 檢查這個 TaskManager 是否在 preferred locations 中
        for (TaskManagerLocation preferred : preferredLocations) {
            if (preferred.getResourceID().equals(location.getResourceID())) {
                // ✅ 匹配！這是我們想要的 TaskManager!
                return true;
            }
        }
        // 如果有 preferred locations 但不匹配，可能拒絕或降級
    }
}
```

#### 6.3 實際的匹配邏輯

在 Declarative Slot Pool 中，匹配邏輯可能在：

**文件**: `DefaultDeclarativeSlotPool.java`

```java
private Optional<ResourceProfile> matchWithOutstandingRequirement(
        ResourceProfile offeredProfile) {

    // 查找匹配的 pending request
    // 如果 request 有 preferred location，檢查是否匹配
    // 這個邏輯可能需要增強來支持精確匹配
}
```

---

### **階段 7: Slot 分配與 Subtask 部署**

#### 7.1 Slot 分配確認

```java
// JobMaster 接受 slot offer
SlotPool.allocateSlot() → returns PhysicalSlot
    ↓
PhysicalSlot {
    allocationId: <allocation-id>,
    taskManagerLocation: TaskManagerLocation("tm-source"),
    taskManagerGateway: <gateway>
}
```

#### 7.2 Subtask 部署

```java
// DefaultScheduler.java
deployExecution(ExecutionVertex vertex, PhysicalSlot slot) {
    // 將 subtask 部署到選定的 slot
    vertex.deployToSlot(slot);
}
```

#### 7.3 最終結果

```
Subtask: Source: Custom Source_0
    ↓
Deployed to: TaskManager(resourceId="tm-source")
    ✅ 精確匹配成功！
```

---

## 🔍 關鍵發現：實際的精確匹配點

### ⚠️ ResourceManager 層級**不進行**精確匹配

在 Declarative Slot Pool 架構中：

1. **ResourceManager 的角色**:
   - 接收聚合的資源需求 (總共需要多少個 slot)
   - 確保有足夠的 TaskManagers 運行
   - 讓 TaskManagers 向 JobMaster 提供 slots

2. **JobMaster 的角色** (精確匹配在這裡發生):
   - 創建帶有 preferred locations 的 SlotProfile
   - 當收到 TaskManager 的 slot offers 時
   - 檢查 offered slot 的 TaskManagerLocation 是否匹配 preferred location
   - 只接受匹配的 slots

### ✅ 實際的數據流

```
Migration Plan
    ↓
DefaultScheduler.migrationPlan: Map<String, String>
    ↓
DefaultScheduler.getPreferredIp(ExecutionVertexID)
    ↓
MigrationPlanAwarePreferredLocationsRetriever.getPreferredLocations()
    ↓
SlotProfile.preferredLocations: Collection<TaskManagerLocation>
    ↓
PhysicalSlotRequest.slotProfile
    ↓
[等待 TaskManager offer slots]
    ↓
TaskManager offers slots with TaskManagerLocation
    ↓
JobMaster 匹配: offered.resourceId == preferred.resourceId
    ↓
✅ 只接受匹配的 slot
```

---

## 📊 ResourceID 傳遞路徑總結

### JobMaster 側 (創建 Preferred Location)

```
Migration Plan: "Source_0" → "tm-source"
    ↓
context.getPreferredIp() → "tm-source"
    ↓
slotPool 查詢 → TaskManagerLocation {
    resourceId: ResourceID("tm-source"),
    hostname: "...",
    ...
}
    ↓
SlotProfile.preferredLocations → [TaskManagerLocation]
    ↓
PhysicalSlotRequest 保存 SlotProfile
```

### ResourceManager 側 (資源分配)

```
❌ 不接收單個 request 的 preferred locations
✅ 只接收聚合的資源需求:
    ResourceRequirement {
        resourceProfile: ...,
        numberOfRequiredSlots: 4
    }
```

### TaskManager 側 (Slot Offer)

```
TaskManager 註冊時帶上自己的 ResourceID
    ↓
TaskManager offers slots:
    SlotOffer {
        taskManagerLocation: TaskManagerLocation {
            resourceId: "tm-source"  ← 關鍵！
        }
    }
    ↓
JobMaster 接收 offer
```

### JobMaster 側 (Slot 選擇) ⭐

```
收到 SlotOffer from "tm-source"
    ↓
檢查 pending PhysicalSlotRequest
    ↓
request.slotProfile.preferredLocations 包含 "tm-source"?
    ↓
✅ 匹配！接受這個 slot
    ↓
分配 slot 給 subtask
```

---

## 💡 核心結論

### 1. **精確匹配發生在 JobMaster，而非 ResourceManager**

Declarative Slot Pool 架構下：
- ResourceManager 只負責確保資源可用
- JobMaster 負責精確匹配和選擇

### 2. **ResourceID 不通過 ResourceRequirement 傳遞**

相反，流程是：
- JobMaster 創建帶 preferred location 的 SlotProfile
- TaskManager 在 offer slots 時帶上自己的 ResourceID
- JobMaster 匹配 preferred location 與 offered location

### 3. **我們的實現正確性**

✅ **階段一** (JobMaster): 創建正確的 SlotProfile with preferred locations
✅ **階段二** (ResourceRequirement): 擴展了 ResourceRequirement，雖然在 Declarative 架構下未直接使用
✅ **階段三** (ResourceManager): 在 ResourceManager 層級添加了精確匹配邏輯（作為備用）

### 4. **實際的精確匹配點**

需要確保 **DeclarativeSlotPool** 在接受 slot offers 時檢查 preferred locations：

```java
// 在 DefaultDeclarativeSlotPool 或 PhysicalSlotRequestBulk 中
// 當匹配 slot offer 與 pending request 時：
if (!slotProfile.getPreferredLocations().isEmpty()) {
    boolean matches = slotProfile.getPreferredLocations().stream()
        .anyMatch(preferred ->
            preferred.getResourceID().equals(offeredLocation.getResourceID()));

    if (!matches) {
        // 不接受這個 slot，等待匹配的 TaskManager
        return false;
    }
}
```

---

## 🎯 下一步建議

### 需要驗證的關鍵點

1. **DeclarativeSlotPool 的 slot 匹配邏輯**
   - 檢查 `DefaultDeclarativeSlotPool.offerSlots()` 是否考慮 preferred locations
   - 可能需要增強 slot 選擇邏輯

2. **PhysicalSlotRequestBulk 的匹配**
   - 檢查 bulk 如何選擇 slots
   - 確保 preferred locations 被考慮

3. **日誌驗證**
   - 運行測試並檢查日誌
   - 確認 SlotProfile 包含正確的 preferred locations
   - 確認 slot 選擇過程考慮了這些 locations

### 可能需要的額外修改

如果發現 DeclarativeSlotPool 不考慮 preferred locations，可能需要：

1. 修改 `DefaultDeclarativeSlotPool.matchWithOutstandingRequirement()`
2. 增強 slot 選擇邏輯以檢查 preferred locations
3. 添加日誌以追蹤 slot 匹配決策

---

## 📝 總結

完整流程中的 ResourceID 傳遞：

```
[Migration Plan]
     ↓ (String resourceId)
[JobMaster Context]
     ↓ (查詢 SlotPool)
[TaskManagerLocation]
     ↓ (包含在 SlotProfile)
[PhysicalSlotRequest]
     ↓ (等待 offer)
[TaskManager Slot Offer] ← (帶 ResourceID)
     ↓ (匹配檢查)
[JobMaster Slot Selection] ⭐ 精確匹配點
     ↓
[Subtask Deployment]
```

**關鍵**: ResourceID 不通過 ResourceManager 傳遞，而是在 JobMaster 側進行匹配！
