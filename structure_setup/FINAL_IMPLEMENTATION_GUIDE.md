# 使用 Flink 原生 ResourceID 实现 Subtask 迁移的完整指南

## 核心理念

✅ **使用 Flink 原生的 ResourceID 机制**，而不是添加自定义的 metrics scope 变量
✅ **在 ResourceManager 层级直接进行 ResourceID 匹配**
✅ **类型安全的比对**：`ResourceID.toString().equals(targetResourceId)`

## 已完成的配置

### 1. TaskManager Resource-ID 设置

**start-taskmanager-with-metrics.sh** (已修改):
```bash
# 如果設置了 TM_RESOURCE_ID 環境變量，寫入 flink-conf.yaml
if [ -n "$TM_RESOURCE_ID" ]; then
    echo "Setting TaskManager resource-id to: $TM_RESOURCE_ID"
    echo "taskmanager.resource-id: $TM_RESOURCE_ID" >> /opt/flink/conf/flink-conf.yaml
fi
```

**docker-compose.yml** (已修改):
```yaml
tm-20c-1:
  environment:
    - TM_RESOURCE_ID=tm-20c-1

tm-20c-2-net:
  environment:
    - TM_RESOURCE_ID=tm-20c-2-net

tm-10c-3-cpu:
  environment:
    - TM_RESOURCE_ID=tm-10c-3-cpu

tm-20c-4:
  environment:
    - TM_RESOURCE_ID=tm-20c-4
```

### 2. Detector.py 配置

**不需要修改！** 因为：
- 当设置 `taskmanager.resource-id` 后，`tm_id` 标签就会变成 resource-id
- 现有的 `get_taskmanager_info()` 和 `get_subtask_locations()` 会自动使用 resource-id
- Migration plan 会自动生成 resource-id 格式的 JSON

## 工作流程

### Phase 1: 设置 Resource-ID ✅

1. 重新构建 Docker 镜像（如果修改了 Dockerfile）
   ```bash
   cd /home/yenwei/research/structure_setup
   docker-compose build
   ```

2. 启动集群
   ```bash
   docker-compose up -d
   ```

3. 验证 Resource-ID 是否生效
   ```bash
   # 查看 TaskManager 日志
   docker logs tm-20c-1 2>&1 | grep -i "resource"

   # 应该看到：
   # Setting TaskManager resource-id to: tm-20c-1
   ```

4. 检查 Prometheus metrics
   ```bash
   curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond' | \
     python3 -c "import sys, json; r=json.load(sys.stdin)['data']['result']; print('tm_id:', r[0]['metric']['tm_id'] if r else 'No data')"

   # 应该看到：
   # tm_id: tm-20c-1  (而不是 172_18_0_8:43289_03381c)
   ```

### Phase 2: 生成 Migration Plan

运行 detector.py：
```python
from detector import FlinkDetector

detector = FlinkDetector()
reports = detector.detect_bottleneck()

# 找出过载的 subtask
overloaded_subtasks = []
for report in reports:
    if report['status'] == "🔴 OVERLOAD":
        task_name = report['task_name']
        for idx, busy in enumerate(report['details']):
            if busy > 700:
                subtask_id = f"{task_name}_{idx}"
                overloaded_subtasks.append((subtask_id, busy))

# 生成 migration plan
migration_plan = detector.generate_migration_plan(overloaded_subtasks)
detector.write_migration_plan(migration_plan)
```

**期望的 migration_plan.json**:
```json
{
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_0": "tm-20c-1",
  "Window_Max____Map_0": "tm-20c-4"
}
```

### Phase 3: 实现 ResourceManager 层级的匹配

需要在 Flink 源码中实现以下修改：

#### 3.1 修改 ResourceAllocationStrategy 接口

**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/ResourceAllocationStrategy.java`

添加一个新的方法，允许传递 subtask 信息：

```java
public interface ResourceAllocationStrategy {

    // 现有方法
    ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker);

    // 新增方法：带 subtask 上下文信息
    default ResourceAllocationResult tryFulfillRequirementsWithContext(
            Map<JobID, Collection<ResourceRequirementWithContext>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {
        // Default implementation delegates to original method
        Map<JobID, Collection<ResourceRequirement>> requirements = new HashMap<>();
        missingResources.forEach((jobId, reqs) -> {
            requirements.put(jobId, reqs.stream()
                    .map(ResourceRequirementWithContext::getRequirement)
                    .collect(Collectors.toList()));
        });
        return tryFulfillRequirements(requirements, taskManagerResourceInfoProvider, blockedTaskManagerChecker);
    }
}

// 新增类：带上下文信息的 ResourceRequirement
class ResourceRequirementWithContext {
    private final ResourceRequirement requirement;
    private final String subtaskIdentifier;  // e.g., "Source:_..._0"
    private final String preferredResourceId;  // e.g., "tm-20c-1"

    // constructor, getters...
}
```

#### 3.2 修改 DefaultResourceAllocationStrategy

**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/DefaultResourceAllocationStrategy.java`

```java
public class DefaultResourceAllocationStrategy implements ResourceAllocationStrategy {

    // 添加成员变量
    private final MigrationPlanReader migrationPlanReader;

    public DefaultResourceAllocationStrategy(
            ResourceProfile totalResourceProfile,
            int numSlotsPerWorker,
            TaskManagerLoadBalanceMode taskManagerLoadBalanceMode,
            Time taskManagerTimeout,
            int redundantTaskManagerNum,
            CPUResource minTotalCPU,
            MemorySize minTotalMemory,
            boolean enableMigrationPlan) {  // 新增参数

        // ... 现有初始化代码 ...

        // 如果启用 migration plan，创建 reader
        if (enableMigrationPlan) {
            this.migrationPlanReader = new MigrationPlanReader("/opt/flink/plan/migration_plan.json");
        } else {
            this.migrationPlanReader = null;
        }
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {

        // 如果启用了 migration plan，使用增强的匹配策略
        if (migrationPlanReader != null) {
            return tryFulfillRequirementsWithMigrationPlan(
                    missingResources, taskManagerResourceInfoProvider, blockedTaskManagerChecker);
        }

        // 否则使用原有逻辑
        // ... 现有代码 ...
    }

    private ResourceAllocationResult tryFulfillRequirementsWithMigrationPlan(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {

        // 加载 migration plan
        Map<String, String> migrationPlan = migrationPlanReader.load();

        // ... 实现带 preferred resource-id 的分配逻辑 ...
    }
}
```

#### 3.3 核心实现：ResourceMatchingStrategy with ResourceID

```java
private int tryFulfilledRequirementWithResource(
        List<InternalResourceInfo> internalResources,
        int numUnfulfilled,
        ResourceProfile requiredResource,
        JobID jobId,
        String subtaskIdentifier) {  // 新增参数

    // ✅ 从 migration plan 获取 target resource-id
    String targetResourceId = migrationPlan.get(subtaskIdentifier);

    if (targetResourceId != null) {
        // Phase 1: 尝试在 preferred TaskManager 上分配
        for (InternalResourceInfo taskManager : internalResources) {
            // ✅ 直接使用 ResourceID（Flink原生支援）
            ResourceID taskManagerId = taskManager.getTaskExecutorConnection().getResourceID();

            // ✅ 类型安全的比对
            if (targetResourceId.equals(taskManagerId.toString())) {
                LOG.info("Attempting allocation on preferred TaskManager: {} for subtask: {}",
                        taskManagerId, subtaskIdentifier);

                if (taskManager.tryAllocateSlotForJob(jobId, requiredResource)) {
                    numUnfulfilled--;
                    LOG.info("Successfully allocated on preferred TaskManager: {}", taskManagerId);
                }

                break;  // 找到 preferred TM，退出
            }
        }
    }

    // Phase 2: Fallback - 使用原有策略
    if (numUnfulfilled > 0) {
        // 使用 AnyMatching 或 LeastUtilization 策略
    }

    return numUnfulfilled;
}
```

### Phase 4: 传递 Subtask 信息的挑战

**关键问题**：如何在 ResourceManager 层级知道"这个 resource requirement 是为哪个 subtask"？

#### 解决方案 A: 使用 AllocationID 映射

1. **在 JobMaster 创建 SlotRequest 时**，记录 `AllocationID -> subtask identifier` 的映射
2. **通过共享存储** (如文件系统) 或 **REST API** 将映射传递给 ResourceManager
3. **ResourceManager 在分配时** 根据 AllocationID 查找对应的 subtask identifier

#### 解决方案 B: 扩展 ResourceProfile

```java
public class ResourceProfile {
    // 现有字段...
    private final String preferredResourceId;  // 新增字段

    public static ResourceProfile newBuilder() {
        return new Builder();
    }

    public static class Builder {
        // 现有方法...

        public Builder setPreferredResourceId(String preferredResourceId) {
            this.preferredResourceId = preferredResourceId;
            return this;
        }
    }
}
```

然后在 JobMaster 创建 ResourceRequirement 时设置：
```java
ResourceProfile profile = ResourceProfile.newBuilder()
        .setCpuCores(1.0)
        .setTaskHeapMemory(MemorySize.ofMebiBytes(512))
        .setPreferredResourceId(getPreferredResourceIdForSubtask(subtaskId))
        .build();
```

#### 解决方案 C: 简化版 - 基于 JobID + 顺序

如果你的系统中每次只有一个 job，可以简化：
- Migration plan 使用 `subtask_0`, `subtask_1`, ... 这样的简单标识
- ResourceManager 按照请求顺序分配到对应的 preferred resource

## 验证步骤

### 1. 验证 Resource-ID 设置

```bash
docker-compose up -d
sleep 30

# 检查 tm_id 是否是 resource-id
curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond' | \
  python3 -c "import sys, json; r=json.load(sys.stdin)['data']['result']; print([m['metric']['tm_id'] for m in r[:4]])"

# 期望输出: ['tm-20c-1', 'tm-20c-2-net', 'tm-10c-3-cpu', 'tm-20c-4']
```

### 2. 验证 Migration Plan 生成

```bash
cat /home/yenwei/research/structure_setup/plan/migration_plan.json

# 期望输出: 使用 resource-id 而不是 IP 格式的 tm_id
# {"Source:..._0": "tm-20c-1", ...}
```

### 3. 验证 ResourceManager 匹配

提交 Job 后，检查日志：
```bash
docker logs jobmanager 2>&1 | grep -i "preferred"

# 期望看到:
# Attempting allocation on preferred TaskManager: tm-20c-1 for subtask: Source:..._0
# Successfully allocated on preferred TaskManager: tm-20c-1
```

## 总结

### ✅ 优势

1. **使用 Flink 原生机制**：ResourceID 是官方支持的
2. **类型安全**：直接使用 ResourceID 对象，而不是字符串
3. **无需修改 metrics**：tm_id 自动变成 resource-id
4. **简洁清晰**：代码逻辑直观易懂

### 🎯 关键点

1. **ResourceID 设置** → `taskmanager.resource-id: tm-20c-1`
2. **tm_id 标签** → 自动变成 `tm-20c-1`
3. **Migration Plan** → 使用 resource-id: `{"subtask": "tm-20c-1"}`
4. **ResourceManager 匹配** → `taskManagerId.toString().equals(targetResourceId)`

### 📋 剩余工作

唯一的挑战是：**如何在 ResourceManager 层级获取 subtask identifier**

推荐方案：扩展 ResourceProfile 添加 `preferredResourceId` 字段，在 JobMaster 创建请求时设置。
