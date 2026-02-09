# 重啟並驗證 Resource-ID 配置

## 已完成的修改

1. ✅ **detector.py** - 完全使用 resource_id
2. ✅ **prometheus.yml** - 為每個 TaskManager 添加 resource_id 標籤
3. ✅ **docker-compose.yml** - 已設置 FLINK_PROPERTIES resource-id（雖然暫時不生效）

## 重啟步驟

### 1. 停止現有服務

```bash
cd /home/yenwei/research/structure_setup
docker-compose down
```

### 2. 清理舊數據（可選，如果需要全新開始）

```bash
# 清理 Prometheus 數據
sudo rm -rf /home/yenwei/server_data/prometheus/*

# 清理 Kafka 數據
docker volume rm structure_setup_kafka_data
```

### 3. 啟動服務

```bash
docker-compose up -d
```

### 4. 檢查服務狀態

```bash
# 檢查所有容器是否運行
docker-compose ps

# 應該看到以下容器都是 Up 狀態：
# - kafka
# - jobmanager
# - tm-20c-1
# - tm-20c-2-net
# - tm-10c-3-cpu
# - tm-20c-4
# - prometheus
# - grafana
```

### 5. 等待服務完全啟動（重要！）

```bash
# 等待 30 秒讓所有服務完全啟動
sleep 30
```

### 6. 驗證 Resource-ID 配置

```bash
# 運行驗證腳本
./verify_resource_id.sh
```

期望看到的輸出：
```
✅ resource_id 標籤存在
可用的 resource_id: tm-10c-3-cpu, tm-20c-1, tm-20c-2-net, tm-20c-4

✅ 找到 metrics
  - task: Source:..., resource_id: tm-20c-1, tm_id: 172_18_0_8:43289...

✅ detector.py 成功獲取 TaskManager 資訊
  - resource_id: tm-20c-1, host: 172.18.0.8, load: 100.5
  - resource_id: tm-20c-2-net, host: 172.18.0.9, load: 200.3
```

### 7. 提交 Flink Job（如果還沒有）

```bash
# 示例：提交 Nexmark Job
docker exec -it jobmanager bash
cd /opt/nexmark
flink run -d nexmark-flink-0.3-SNAPSHOT.jar ...
```

### 8. 等待 Job 運行並產生 Metrics

```bash
# 等待 1-2 分鐘讓 Job 運行並產生足夠的 metrics
sleep 120
```

### 9. 測試 Migration Plan 生成

```bash
# 測試生成 migration plan
python3 << 'EOF'
import sys
sys.path.insert(0, '/home/yenwei/research/structure_setup/caom_core')
from detector import FlinkDetector

detector = FlinkDetector()

# 檢測瓶頸
reports = detector.detect_bottleneck()
print(f"\n找到 {len(reports)} 個 Task")

# 模擬過載的 subtask
overloaded_subtasks = []
for report in reports:
    if report['max_busy'] > 500:  # 降低閾值用於測試
        task_name = report['task_name']
        for idx, busy in enumerate(report['details']):
            if busy > 500:
                subtask_id = f"{task_name}_{idx}"
                overloaded_subtasks.append((subtask_id, busy))

print(f"\n檢測到 {len(overloaded_subtasks)} 個過載的 Subtask")

if overloaded_subtasks:
    # 生成 migration plan
    migration_plan = detector.generate_migration_plan(overloaded_subtasks)

    if migration_plan:
        # 寫入文件
        detector.write_migration_plan(migration_plan)
        print(f"\n✅ Migration Plan 已生成，包含 {len(migration_plan)} 個遷移項目")

        # 顯示前 3 個
        for subtask_id, resource_id in list(migration_plan.items())[:3]:
            print(f"  {subtask_id[:60]} -> {resource_id}")
    else:
        print("\n⚠️ 無法生成 Migration Plan")
else:
    print("\n⚠️ 未檢測到過載的 Subtask（可能需要調整閾值或等待更多數據）")

EOF
```

### 10. 檢查生成的 Migration Plan

```bash
# 查看生成的 migration_plan.json
cat /home/yenwei/research/structure_setup/plan/migration_plan.json | python3 -m json.tool
```

期望看到：
```json
{
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_0": "tm-20c-1",
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_2": "tm-20c-4",
  "Window_Max____Map_0": "tm-10c-3-cpu"
}
```

**而不是：**
```json
{
  "Source:...": "172_18_0_9:37963_9c569e"  ❌
}
```

## 故障排除

### 問題 1: resource_id 標籤仍然不存在

**檢查：**
```bash
# 重新加載 Prometheus 配置
docker exec prometheus kill -HUP 1

# 或重啟 Prometheus
docker-compose restart prometheus
```

### 問題 2: Migration Plan 仍使用 tm_id

**檢查：**
```bash
# 確認 Prometheus 查詢返回 resource_id
curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond' | \
  python3 -c "import sys, json; print(json.load(sys.stdin)['data']['result'][0]['metric'])"
```

應該看到 `'resource_id': 'tm-20c-1'`

### 問題 3: detector.py 返回空結果

**檢查：**
```bash
# 確認 Job 正在運行
curl http://localhost:8081/jobs

# 確認有 metrics 數據
curl http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond
```

## 驗證成功的標誌

✅ Prometheus 有 `resource_id` 標籤
✅ detector.py 的 `get_taskmanager_info()` 返回 resource_id 作為 key
✅ detector.py 的 `get_subtask_locations()` 返回 resource_id
✅ migration_plan.json 使用 resource_id（如 `tm-20c-1`）而不是 tm_id（如 `172_18_0_9:37963_9c569e`）

## 下一步

一旦 migration plan 正確使用 resource_id，就可以：

1. 實施 Flink ResourceManager 層級的修改（參考 `flink_modifications/MODIFICATION_GUIDE.md`）
2. 測試完整的遷移流程
3. 使用 savepoint 重啟 Job 並驗證 subtask 被分配到正確的 TaskManager
