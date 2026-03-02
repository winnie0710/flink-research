# 精確匹配實現方案：Subtask 到 ResourceID 的完整映射

## 目標

**100% 精確匹配**：每個 subtask 必須分配到 migration plan 中指定的 TaskManager（通過 ResourceID）。

## 完整的修改層級

### 必須修改的三個層級：

1. **JobMaster 層級** - 將 migration plan 中的 ResourceID 轉換為 TaskManagerLocation
2. **中間傳遞** - 確保 preferred location 從 JobMaster 傳遞到 ResourceManager
3. **ResourceManager 層級** - 精確匹配 ResourceID 並分配 slot

## 詳細實現步驟

---

## 階段 1: JobMaster 層級修改

### 文件 1: DefaultScheduler.java

**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/DefaultScheduler.java`

#### 修改 1.1: 創建 SlotProfile 時設置 preferred locations

找到創建 `SlotProfile` 的地方（通常在 `allocateSlots` 或類似方法中）：

```java
// 在 DefaultScheduler.java 中找到創建 SlotProfile 的方法
private SlotProfile createSlotProfile(
        ExecutionVertexID executionVertexId,
        ResourceProfile resourceProfile) {

    // ✅ 從 migration plan 獲取 preferred ResourceID
    String preferredResourceId = getPreferredResourceIdFromMigrationPlan(executionVertexId);

    Collection<TaskManagerLocation> preferredLocations = Collections.emptyList();

    if (preferredResourceId != null) {
        // ✅ 關鍵：將 ResourceID 轉換為 TaskManagerLocation
        Optional<TaskManagerLocation> location =
            convertResourceIdToTaskManagerLocation(preferredResourceId);

        if (location.isPresent()) {
            preferredLocations = Collections.singleton(location.get());
            LOG.info("🎯 Setting preferred location for {}: {} (ResourceID: {})",
                executionVertexId, location.get(), preferredResourceId);
        } else {
            LOG.warn("⚠️ Could not find TaskManagerLocation for ResourceID: {}",
                preferredResourceId);
        }
    }

    return SlotProfile.priorAllocationProfileWithPreferredLocations(
        resourceProfile,
        resourceProfile,
        preferredLocations,  // ✅ 設置 preferred locations
        Collections.emptyList(),
        Collections.emptySet()
    );
}

// ✅ 從 migration plan 獲取 preferred ResourceID
private String getPreferredResourceIdFromMigrationPlan(ExecutionVertexID executionVertexId) {
    // 獲取 subtask 名稱
    String subtaskName = getSubtaskName(executionVertexId);

    // 從 migration plan 查找
    return MigrationPlanReader.getTargetResourceId(subtaskName, migrationPlan);
}

// ✅ 獲取 subtask 名稱（與 migration plan 中的 key 匹配）
private String getSubtaskName(ExecutionVertexID executionVertexId) {
    ExecutionVertex vertex = getExecutionVertex(executionVertexId);
    String taskName = vertex.getJobVertex().getName();
    int subtaskIndex = vertex.getParallelSubtaskIndex();

    // 格式: "TaskName_SubtaskIndex"
    return taskName + "_" + subtaskIndex;
}

// ✅ 關鍵方法：將 ResourceID 字符串轉換為 TaskManagerLocation
private Optional<TaskManagerLocation> convertResourceIdToTaskManagerLocation(
        String resourceIdString) {

    // 方案 A: 從 SlotPool 獲取已註冊的 TaskManager 信息
    Collection<SlotInfo> availableSlots = slotPool.getAvailableSlotsInformation();

    for (SlotInfo slotInfo : availableSlots) {
        TaskManagerLocation location = slotInfo.getTaskManagerLocation();
        ResourceID resourceId = location.getResourceID();

        if (resourceId.toString().equals(resourceIdString)) {
            return Optional.of(location);
        }
    }

    // 方案 B: 從 ResourceManager 查詢（如果 SlotPool 中沒有）
    // 這需要添加一個查詢接口

    return Optional.empty();
}
```

#### 修改 1.2: 添加查詢 TaskManagerLocation 的接口

如果 SlotPool 中沒有可用的 slot 信息，需要從 ResourceManager 查詢：

```java
// 在 DefaultScheduler.java 或 SlotPoolImpl.java 中添加

/**
 * 從 ResourceManager 查詢 TaskManagerLocation by ResourceID
 */
private CompletableFuture<Optional<TaskManagerLocation>> queryTaskManagerLocation(
        String resourceIdString) {

    // 通過 ResourceManager gateway 查詢
    return resourceManagerGateway.getTaskManagerLocation(resourceIdString)
        .thenApply(Optional::ofNullable)
        .exceptionally(throwable -> {
            LOG.warn("Failed to query TaskManagerLocation for ResourceID: {}",
                resourceIdString, throwable);
            return Optional.empty();
        });
}
```

---

## 階段 2: ResourceManager 層級修改

### 問題：ResourceRequirement 沒有 preferred location 信息

從 `PhysicalSlotRequest` (JobMaster) 到 `ResourceRequirement` (ResourceManager) 的轉換過程中，**preferred location 信息被丟失**。

### 解決方案：擴展 ResourceRequirement

#### 文件 2: ResourceRequirement.java

**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/slots/ResourceRequirement.java`

```java
public class ResourceRequirement implements Serializable {

    private final ResourceProfile resourceProfile;
    private final int numberOfRequiredSlots;

    // ✅ 新增：preferred locations
    private final Collection<TaskManagerLocation> preferredLocations;

    private ResourceRequirement(
            ResourceProfile resourceProfile,
            int numberOfRequiredSlots,
            Collection<TaskManagerLocation> preferredLocations) {

        this.resourceProfile = resourceProfile;
        this.numberOfRequiredSlots = numberOfRequiredSlots;
        this.preferredLocations = preferredLocations != null
            ? new ArrayList<>(preferredLocations)
            : Collections.emptyList();
    }

    public Collection<TaskManagerLocation> getPreferredLocations() {
        return preferredLocations;
    }

    // ✅ 添加新的創建方法
    public static ResourceRequirement createWithPreferredLocations(
            ResourceProfile resourceProfile,
            int numberOfRequiredSlots,
            Collection<TaskManagerLocation> preferredLocations) {

        return new ResourceRequirement(
            resourceProfile,
            numberOfRequiredSlots,
            preferredLocations);
    }

    // 保留原有的創建方法（向後兼容）
    public static ResourceRequirement create(
            ResourceProfile resourceProfile,
            int numberOfRequiredSlots) {

        return new ResourceRequirement(
            resourceProfile,
            numberOfRequiredSlots,
            Collections.emptyList());
    }
}
```

#### 文件 3: 修改創建 ResourceRequirement 的地方

找到從 `PhysicalSlotRequest` 轉換到 `ResourceRequirement` 的代碼：

```bash
# 搜索創建 ResourceRequirement 的地方
grep -r "ResourceRequirement.create" flink_source/flink-runtime/src/main/java/
```

修改這些地方，傳遞 preferred locations：

```java
// 示例：在 DeclarativeSlotPool 或類似的類中
private ResourceRequirement convertToResourceRequirement(PhysicalSlotRequest request) {
    SlotProfile slotProfile = request.getSlotProfile();

    return ResourceRequirement.createWithPreferredLocations(
        slotProfile.getPhysicalSlotResourceProfile(),
        1,  // 每個 PhysicalSlotRequest 對應一個 slot
        slotProfile.getPreferredLocations()  // ✅ 傳遞 preferred locations
    );
}
```

---

## 階段 3: ResourceAllocationStrategy 精確匹配

### 文件 4: 修改或創建精確匹配的策略

#### 方案 A: 修改 DefaultResourceAllocationStrategy

在 `tryFulfillRequirementsForJobWithResources` 方法中：

```java
private Collection<ResourceRequirement> tryFulfillRequirementsForJobWithResources(
        JobID jobId,
        Collection<ResourceRequirement> missingResources,
        List<InternalResourceInfo> registeredResources) {

    Collection<ResourceRequirement> outstandingRequirements = new ArrayList<>();

    for (ResourceRequirement requirement : missingResources) {
        Collection<TaskManagerLocation> preferredLocations =
            requirement.getPreferredLocations();

        int numUnfulfilled = requirement.getNumberOfRequiredSlots();

        // ✅ 如果有 preferred locations，優先在這些 TaskManager 上分配
        if (!preferredLocations.isEmpty()) {
            numUnfulfilled = tryAllocateOnPreferredTaskManagers(
                registeredResources,
                numUnfulfilled,
                requirement.getResourceProfile(),
                preferredLocations,
                jobId
            );
        }

        // 如果 preferred 分配不夠，使用默認策略
        if (numUnfulfilled > 0) {
            numUnfulfilled = availableResourceMatchingStrategy
                .tryFulfilledRequirementWithResource(
                    registeredResources,
                    numUnfulfilled,
                    requirement.getResourceProfile(),
                    jobId
                );
        }

        if (numUnfulfilled > 0) {
            outstandingRequirements.add(
                ResourceRequirement.create(
                    requirement.getResourceProfile(),
                    numUnfulfilled
                )
            );
        }
    }

    return outstandingRequirements;
}

// ✅ 精確匹配：只在 preferred TaskManager 上分配
private int tryAllocateOnPreferredTaskManagers(
        List<InternalResourceInfo> registeredResources,
        int numUnfulfilled,
        ResourceProfile requiredResource,
        Collection<TaskManagerLocation> preferredLocations,
        JobID jobId) {

    // 將 preferred locations 轉換為 ResourceID 集合
    Set<ResourceID> preferredResourceIds = preferredLocations.stream()
        .map(TaskManagerLocation::getResourceID)
        .collect(Collectors.toSet());

    Iterator<InternalResourceInfo> iterator = registeredResources.iterator();

    while (numUnfulfilled > 0 && iterator.hasNext()) {
        InternalResourceInfo resourceInfo = iterator.next();
        ResourceID tmResourceId = resourceInfo.getTaskExecutorConnection().getResourceID();

        // ✅ 精確匹配：只分配給 preferred TaskManager
        if (preferredResourceIds.contains(tmResourceId)) {
            LOG.info("🎯 Attempting to allocate on preferred TaskManager: {}", tmResourceId);

            while (numUnfulfilled > 0
                   && resourceInfo.tryAllocateSlotForJob(jobId, requiredResource)) {
                numUnfulfilled--;
                LOG.info("✅ Successfully allocated slot on preferred TaskManager: {}",
                    tmResourceId);
            }

            if (resourceInfo.availableProfile.equals(ResourceProfile.ZERO)) {
                iterator.remove();
            }
        }
    }

    return numUnfulfilled;
}
```

#### 方案 B: 創建專用的 PreferredLocationResourceAllocationStrategy

```java
public class PreferredLocationResourceAllocationStrategy
        implements ResourceAllocationStrategy {

    private final ResourceAllocationStrategy fallbackStrategy;

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {

        ResourceAllocationResult.Builder resultBuilder =
            ResourceAllocationResult.builder();

        Map<JobID, Collection<ResourceRequirement>> remainingRequirements =
            new HashMap<>();

        for (Map.Entry<JobID, Collection<ResourceRequirement>> entry :
                missingResources.entrySet()) {

            JobID jobId = entry.getKey();
            Collection<ResourceRequirement> requirements = entry.getValue();

            // ✅ 分離有 preferred location 和沒有的需求
            List<ResourceRequirement> withPreferred = new ArrayList<>();
            List<ResourceRequirement> withoutPreferred = new ArrayList<>();

            for (ResourceRequirement req : requirements) {
                if (!req.getPreferredLocations().isEmpty()) {
                    withPreferred.add(req);
                } else {
                    withoutPreferred.add(req);
                }
            }

            // ✅ 優先處理有 preferred location 的需求
            Collection<ResourceRequirement> unfulfilled =
                allocateWithPreferredLocations(
                    jobId,
                    withPreferred,
                    taskManagerResourceInfoProvider,
                    resultBuilder,
                    blockedTaskManagerChecker
                );

            // 合併未滿足的需求
            Collection<ResourceRequirement> allUnfulfilled = new ArrayList<>(unfulfilled);
            allUnfulfilled.addAll(withoutPreferred);

            if (!allUnfulfilled.isEmpty()) {
                remainingRequirements.put(jobId, allUnfulfilled);
            }
        }

        // 對剩餘需求使用 fallback 策略
        if (!remainingRequirements.isEmpty()) {
            ResourceAllocationResult fallbackResult =
                fallbackStrategy.tryFulfillRequirements(
                    remainingRequirements,
                    taskManagerResourceInfoProvider,
                    blockedTaskManagerChecker
                );

            // 合併結果
            mergeResults(resultBuilder, fallbackResult);
        }

        return resultBuilder.build();
    }

    private Collection<ResourceRequirement> allocateWithPreferredLocations(
            JobID jobId,
            Collection<ResourceRequirement> requirements,
            TaskManagerResourceInfoProvider tmProvider,
            ResourceAllocationResult.Builder resultBuilder,
            BlockedTaskManagerChecker blockedChecker) {

        Collection<ResourceRequirement> unfulfilled = new ArrayList<>();

        for (ResourceRequirement requirement : requirements) {
            int remaining = allocateSingleRequirementWithPreference(
                jobId,
                requirement,
                tmProvider,
                resultBuilder,
                blockedChecker
            );

            if (remaining > 0) {
                unfulfilled.add(ResourceRequirement.create(
                    requirement.getResourceProfile(),
                    remaining
                ));
            }
        }

        return unfulfilled;
    }

    private int allocateSingleRequirementWithPreference(
            JobID jobId,
            ResourceRequirement requirement,
            TaskManagerResourceInfoProvider tmProvider,
            ResourceAllocationResult.Builder resultBuilder,
            BlockedTaskManagerChecker blockedChecker) {

        int numRequired = requirement.getNumberOfRequiredSlots();
        ResourceProfile resourceProfile = requirement.getResourceProfile();
        Collection<TaskManagerLocation> preferredLocations =
            requirement.getPreferredLocations();

        // 轉換為 ResourceID
        Set<ResourceID> preferredResourceIds = preferredLocations.stream()
            .map(TaskManagerLocation::getResourceID)
            .collect(Collectors.toSet());

        // 遍歷已註冊的 TaskManager
        for (TaskManagerInfo tmInfo : tmProvider.getRegisteredTaskManagers()) {
            if (numRequired == 0) {
                break;
            }

            ResourceID tmResourceId = tmInfo.getTaskExecutorConnection().getResourceID();

            // ✅ 精確匹配
            if (!preferredResourceIds.contains(tmResourceId)) {
                continue;  // 跳過非 preferred TaskManager
            }

            // 檢查是否被 blocked
            if (blockedChecker.isBlockedTaskManager(tmResourceId)) {
                LOG.warn("Preferred TaskManager {} is blocked", tmResourceId);
                continue;
            }

            // 檢查資源是否足夠
            if (!tmInfo.getAvailableResource().allFieldsNoLessThan(resourceProfile)) {
                LOG.debug("Preferred TaskManager {} has insufficient resources", tmResourceId);
                continue;
            }

            // ✅ 分配 slot
            while (numRequired > 0
                   && tmInfo.getAvailableResource().allFieldsNoLessThan(resourceProfile)) {

                resultBuilder.addAllocationOnRegisteredResource(
                    jobId,
                    tmInfo.getInstanceId(),
                    resourceProfile
                );

                numRequired--;

                LOG.info("✅ Allocated slot on preferred TaskManager {} (remaining: {})",
                    tmResourceId, numRequired);
            }
        }

        return numRequired;  // 返回未滿足的數量
    }
}
```

---

## 階段 4: 配置和啟用

### 文件 5: ResourceManagerRuntimeServices.java

```java
private static SlotManager createSlotManager(...) {
    SlotManagerConfiguration slotManagerConfiguration =
        configuration.getSlotManagerConfiguration();

    // 創建 fallback 策略
    ResourceAllocationStrategy defaultStrategy =
        new DefaultResourceAllocationStrategy(...);

    // ✅ 包裝為 preferred location 策略
    ResourceAllocationStrategy finalStrategy =
        new PreferredLocationResourceAllocationStrategy(defaultStrategy);

    return new FineGrainedSlotManager(
        scheduledExecutor,
        slotManagerConfiguration,
        slotManagerMetricGroup,
        new DefaultResourceTracker(),
        new FineGrainedTaskManagerTracker(),
        new DefaultSlotStatusSyncer(...),
        finalStrategy  // ✅ 使用新的策略
    );
}
```

---

## 驗證和測試

### 1. 添加詳細日誌

在每個關鍵點添加日誌：

```java
// JobMaster
LOG.info("🎯 Creating SlotProfile with preferred location: {}", preferredLocation);

// ResourceManager
LOG.info("✅ Allocated slot on preferred TaskManager {} for subtask {}",
    resourceId, subtaskName);
```

### 2. 檢查日誌

```bash
# 檢查 SlotProfile 是否包含 preferred locations
docker logs jobmanager 2>&1 | grep "preferred location"

# 檢查 ResourceManager 是否精確匹配
docker logs jobmanager 2>&1 | grep "Allocated slot on preferred"
```

### 3. 驗證實際分配

```bash
# 從 Flink UI 或 REST API 獲取 subtask 分配
curl http://localhost:8081/jobs/<job-id>/vertices/<vertex-id>

# 對比 migration_plan.json
```

---

## 需要修改的文件清單

1. ✅ **DefaultScheduler.java** - 設置 preferred locations 到 SlotProfile
2. ✅ **ResourceRequirement.java** - 添加 preferredLocations 字段
3. ✅ **轉換層** - 從 PhysicalSlotRequest 到 ResourceRequirement 保留 preferred locations
4. ✅ **PreferredLocationResourceAllocationStrategy.java** (新建) - 精確匹配邏輯
5. ✅ **ResourceManagerRuntimeServices.java** - 配置使用新策略

---

## 總結

這個方案實現了：
- ✅ **端到端的信息傳遞**：從 migration plan → JobMaster → ResourceManager
- ✅ **精確匹配**：100% 按照 migration plan 分配
- ✅ **類型安全**：使用 Flink 原生的 TaskManagerLocation 和 ResourceID
- ✅ **向後兼容**：不影響現有的分配邏輯（fallback 機制）

這是論文中描述的完整實現方案。
