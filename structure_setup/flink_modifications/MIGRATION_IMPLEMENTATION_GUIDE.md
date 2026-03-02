# Flink Migration Plan Implementation Guide

## 概述

此實現在 Flink ResourceManager 層級整合了 migration plan，使得從 savepoint 重啟時，subtask 可以按照預定的計劃分配到特定的 TaskManager 上。

## 修改的文件

### 1. **新增文件**

#### `MigrationAwareResourceAllocationStrategy.java`
**路徑：** `flink_source/flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/`

**功能：**
- 包裝 `DefaultResourceAllocationStrategy`
- 從 `/opt/flink/plan/migration_plan.json` 讀取遷移計劃
- 優先按照計劃分配 slot 到指定的 TaskManager
- 對於未在計劃中的 subtask，使用預設策略分配

**關鍵特性：**
- 每 5 秒自動重新載入 migration plan（避免頻繁讀取）
- 支援動態更新 migration plan
- 失敗時自動回退到預設策略
- 詳細的日誌記錄，便於除錯

### 2. **修改的文件**

#### `ResourceManagerRuntimeServices.java`
**路徑：** `flink_source/flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/`

**修改內容：**
```java
// 修改前：
new DefaultResourceAllocationStrategy(...)

// 修改後：
new MigrationAwareResourceAllocationStrategy(
    new DefaultResourceAllocationStrategy(...))
```

## 工作流程

### 1. **Migration Plan 生成**
```bash
# 使用 detector.py 生成 migration plan
cd /home/yenwei/research/structure_setup
python caom_core/detector.py

# 生成的文件：structure_setup/plan/migration_plan.json
```

### 2. **部署 Migration Plan**
```bash
# 將 migration plan 複製到 Flink 容器
docker cp structure_setup/plan/migration_plan.json flink-jobmanager:/opt/flink/plan/
docker cp structure_setup/plan/migration_plan.json flink-taskmanager-1:/opt/flink/plan/
docker cp structure_setup/plan/migration_plan.json flink-taskmanager-2:/opt/flink/plan/
docker cp structure_setup/plan/migration_plan.json flink-taskmanager-3:/opt/flink/plan/

# 或在 docker-compose.yml 中掛載
volumes:
  - ./structure_setup/plan:/opt/flink/plan
```

### 3. **編譯 Flink**
```bash
cd /home/yenwei/research/flink_source

# 編譯整個項目
mvn clean install -DskipTests

# 或只編譯 runtime 模組
mvn clean compile -pl flink-runtime -am -DskipTests
```

### 4. **重啟 Job**
```bash
# 1. 停止 Job 並創建 savepoint
flink stop <job-id> -p /opt/flink/savepoints

# 2. 等待 migration plan 載入（ResourceManager 會自動載入）

# 3. 從 savepoint 重啟
flink run -s /opt/flink/savepoints/savepoint-xxx -d your-job.jar
```

## Migration Plan 格式

```json
{
  "TaskName_SubtaskIndex": "target_resource_id",
  "Window_Max____Map_0": "tm_20c_2_net",
  "Window_Max____Map_1": "tm_20c_4",
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_0": "tm_20c_2_net",
  ...
}
```

**格式說明：**
- **Key**: Subtask 識別符，格式為 `{TaskName}_{SubtaskIndex}`
- **Value**: 目標 TaskManager 的 resource-id（對應 `taskmanager.resource-id` 配置）

## 資源分配流程

### Flink 原生流程
```
JobMaster
  ↓ (請求 slot)
ResourceManager
  ↓
FineGrainedSlotManager
  ↓
ResourceAllocationStrategy.tryFulfillRequirements()
  ↓
allocateSlotsAccordingTo()
  ↓
SlotStatusSyncer.allocateSlot()
  ↓
TaskManager (分配 slot)
```

### 整合後的流程
```
JobMaster
  ↓
ResourceManager
  ↓
FineGrainedSlotManager
  ↓
MigrationAwareResourceAllocationStrategy  ← 讀取 migration_plan.json
  ↓
  ├─ 有 migration plan？
  │    ├─ 是 → 按計劃分配到指定 TaskManager
  │    └─ 否 → DefaultResourceAllocationStrategy
  ↓
allocateSlotsAccordingTo()
  ↓
TaskManager (slot 分配到計劃中的 TM)
```

## 關鍵配置

### TaskManager 配置
確保每個 TaskManager 都有唯一的 `resource-id`：

```yaml
# flink-conf.yaml 或 docker-compose.yml 環境變數
taskmanager.resource-id: tm_20c_2_net
```

### Migration Plan 路徑
預設路徑：`/opt/flink/plan/migration_plan.json`

如需修改，編輯 `MigrationAwareResourceAllocationStrategy.java`：
```java
private static final String MIGRATION_PLAN_PATH = "/your/custom/path/migration_plan.json";
```

## 監控與除錯

### 查看日誌
```bash
# ResourceManager 日誌
docker logs flink-jobmanager 2>&1 | grep -i migration

# 查看分配決策
docker logs flink-jobmanager 2>&1 | grep "MigrationAwareResourceAllocationStrategy"
```

### 日誌輸出範例
```
INFO  MigrationAwareResourceAllocationStrategy - ✅ Loaded migration plan with 16 entries
INFO  MigrationAwareResourceAllocationStrategy - 🔄 Using migration-aware allocation strategy with 16 planned migrations
INFO  MigrationAwareResourceAllocationStrategy - ✅ Allocated slot for JobID xxx on preferred TaskManager tm_20c_4 (subtask: Window_Max____Map_1)
```

## 驗證

### 1. 驗證 Migration Plan 已載入
```bash
docker logs flink-jobmanager 2>&1 | grep "Loaded migration plan"
# 應該看到：✅ Loaded migration plan with X entries
```

### 2. 驗證 Slot 分配
```bash
# 查看 Flink Web UI
# http://localhost:8081/#/job/<job-id>/overview

# 或使用 REST API
curl http://localhost:8081/jobs/<job-id>/vertices/<vertex-id>/subtasks
```

### 3. 查看 Subtask 位置
```bash
# 使用 Prometheus 查詢
curl 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond'
```

## 故障排除

### 問題 1: Migration Plan 未載入
**症狀：** 日誌中看到 "Migration plan file not found"

**解決：**
```bash
# 確認文件存在
docker exec flink-jobmanager ls -la /opt/flink/plan/migration_plan.json

# 確認文件格式正確
docker exec flink-jobmanager cat /opt/flink/plan/migration_plan.json | python -m json.tool
```

### 問題 2: Slot 未按計劃分配
**症狀：** Subtask 沒有分配到指定的 TaskManager

**可能原因：**
1. TaskManager 的 resource-id 不匹配
2. 目標 TaskManager 資源不足
3. 目標 TaskManager 被標記為 blocked

**解決：**
```bash
# 檢查 TaskManager resource-id
docker exec flink-taskmanager-1 cat /opt/flink/conf/flink-conf.yaml | grep resource-id

# 檢查可用資源
curl http://localhost:8081/taskmanagers
```

### 問題 3: 編譯錯誤
**症狀：** Maven 編譯失敗

**解決：**
```bash
# 檢查 Java 版本（需要 Java 11）
java -version

# 清理並重新編譯
cd /home/yenwei/research/flink_source
mvn clean
mvn install -DskipTests
```

## 限制與已知問題

### 當前限制
1. **Subtask 識別問題：** 當前實現無法完美識別每個 ResourceRequirement 對應的具體 subtask，因此採用「盡力而為」的策略
2. **一次性分配：** Migration plan 中的每個條目只會使用一次，分配後會被移除
3. **無法處理動態擴縮容：** 如果 job 在運行中改變並行度，需要重新生成 migration plan

### 改進方向
1. **增強 Subtask 識別：** 需要在 ResourceRequirement 中增加 subtask 相關的元數據
2. **支援部分遷移：** 允許只遷移部分 subtask，其他使用預設策略
3. **支援動態更新：** 在 job 運行時動態調整 slot 分配

## 相關文件

- `structure_setup/caom_core/detector.py` - Migration plan 生成器
- `structure_setup/plan/migration_plan.json` - 當前的 migration plan
- `structure_setup/FINAL_IMPLEMENTATION_GUIDE.md` - 完整實現指南
