# ResourceID 实现总结

## ✅ 核心理念

**使用 Flink 原生的 ResourceID 机制**，而不是创建自定义的 metrics scope 变量。

### 为什么这样做？

1. **ResourceID 是 Flink 官方机制**：`taskmanager.resource-id` 是官方配置选项
2. **tm_id 就是 ResourceID.toString()**：设置 resource-id 后，metrics 中的 tm_id 自动变成 resource-id
3. **类型安全**：在 ResourceManager 中可以直接使用 `ResourceID` 对象进行匹配
4. **无需修改 Flink metrics scope**：不需要添加 `<resource_id>` 变量

## 📁 已修改的文件

### 1. **start-taskmanager-with-metrics.sh**
```bash
# 从环境变量读取 TM_RESOURCE_ID 并写入 flink-conf.yaml
if [ -n "$TM_RESOURCE_ID" ]; then
    echo "taskmanager.resource-id: $TM_RESOURCE_ID" >> /opt/flink/conf/flink-conf.yaml
fi
```

### 2. **docker-compose.yml**
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

### 3. **prometheus.yml**
```yaml
# 恢复为简单配置，不需要手动添加 resource_id 标签
scrape_configs:
  - job_name: 'flink-cluster'
    static_configs:
      - targets:
          - 'tm-20c-1:9249'
          - 'tm-20c-2-net:9249'
          - 'tm-10c-3-cpu:9249'
          - 'tm-20c-4:9249'
```

### 4. **detector.py**
**不需要修改！** 现有代码会自动工作：
- `get_taskmanager_info()` 返回 `{resource_id: {...}}`，因为 tm_id 已经是 resource-id
- `get_subtask_locations()` 返回 `{subtask: resource_id}`
- `generate_migration_plan()` 生成 `{subtask: resource_id}` 格式的 JSON

## 🔄 工作流程

```
┌─────────────────────┐
│  docker-compose.yml │
│  TM_RESOURCE_ID=    │
│  tm-20c-1           │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  start-tm...sh      │
│  写入 flink-conf    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  TaskManager 启动   │
│  ResourceID=        │
│  tm-20c-1           │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Prometheus metrics │
│  tm_id: tm-20c-1    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  detector.py        │
│  读取 tm_id         │
│  (就是 resource-id) │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  migration_plan.json│
│  {"subtask":        │
│   "tm-20c-1"}       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  ResourceManager    │
│  匹配 ResourceID    │
└─────────────────────┘
```

## 🎯 期望结果

### Prometheus 查询
```bash
curl 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond'
```

**期望的 tm_id**:
```json
{
  "metric": {
    "tm_id": "tm-20c-1",  // ✅ ResourceID format
    "host": "172.18.0.8",
    "task_name": "Source:_..."
  }
}
```

**而不是**:
```json
{
  "metric": {
    "tm_id": "172_18_0_8:43289_03381c",  // ❌ IP format
    ...
  }
}
```

### Migration Plan
```json
{
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_0": "tm-20c-1",
  "Window_Max____Map_0": "tm-20c-4"
}
```

✅ 使用 resource-id
❌ 不是 `172_18_0_9:37963_9c569e`

### Detector.py 输出
```
📋 計畫遷移: Source:_..._0
從: tm-10c-3-cpu -> 到: tm-20c-1
目標 host: 172.18.0.8, 負載: 350.2
```

✅ 显示易读的 resource-id
✅ JSON 存储也是 resource-id

## 🚀 下一步：ResourceManager 实现

在 Flink 源码中实现 ResourceID 匹配：

```java
// 在 ResourceMatchingStrategy 中
for (InternalResourceInfo taskManager : internalResources) {
    // ✅ 直接使用 ResourceID（Flink原生支援）
    ResourceID taskManagerId = taskManager.getTaskExecutorConnection().getResourceID();

    // ✅ 从 migration plan 取得 target resource-id
    String targetResourceId = migrationPlan.get(subtaskIdentifier);

    // ✅ 类型安全的比对
    if (targetResourceId != null && targetResourceId.equals(taskManagerId.toString())) {
        if (taskManager.tryAllocateSlotForJob(jobId, requiredResource)) {
            numUnfulfilled--;
        }
    }
}
```

详细实现请参考：
- `flink_modifications/PreferredResourceIDMatchingStrategy_v2.java`
- `FINAL_IMPLEMENTATION_GUIDE.md`

## 📋 验证清单

- [ ] 重新构建 Docker 镜像（如果需要）
- [ ] 启动集群
- [ ] 检查 TaskManager 日志中有 "Setting TaskManager resource-id"
- [ ] 检查 Prometheus metrics 中 tm_id 是 resource-id 格式
- [ ] 运行 detector.py，确认返回 resource-id
- [ ] 生成 migration plan，确认使用 resource-id
- [ ] 实现 Flink ResourceManager 层级的匹配
- [ ] 测试完整的迁移流程

## 🔧 快速验证

运行验证脚本：
```bash
cd /home/yenwei/research/structure_setup
./quick_verify.sh
```

这个脚本会自动检查：
1. TaskManager 日志
2. Prometheus metrics
3. detector.py 功能
4. 生成诊断报告

## 📚 相关文档

- `FINAL_IMPLEMENTATION_GUIDE.md` - 完整实现指南
- `flink_modifications/PreferredResourceIDMatchingStrategy_v2.java` - Java 实现
- `diagnose_resource_id.md` - 问题诊断
- `quick_verify.sh` - 快速验证脚本

## 💡 关键要点

1. **ResourceID 是官方机制** - 不要自己发明轮子
2. **tm_id = ResourceID.toString()** - 设置 resource-id 后自动生效
3. **在 ResourceManager 层级匹配** - 使用类型安全的 ResourceID 对象
4. **detector.py 不需要修改** - 会自动使用 resource-id

---

最后更新：2026-02-09
