# 自動從 Savepoint 重新提交 Job

## 概述

`detector.py` 現在支援自動從 savepoint 重新提交 Flink job，無需手動執行命令。

## 功能特性

### 1. 自動配置
在初始化 `FlinkDetector` 時，可以傳入 `job_config` 參數：

```python
job_config = {
    "container": "jobmanager",              # Docker 容器名稱
    "entry_class": "com.github.nexmark.flink.BenchmarkIsoQ7",  # 主類別
    "parallelism": 4,                       # 並行度
    "jar_path": "/opt/flink/usrlib/nexmark.jar",  # JAR 路徑
    "nexmark_conf_dir": "/opt/nexmark",     # 配置目錄
    "program_args": [                       # 程序參數
        "--queries", "q7-isolated",
        "--location", "/opt/nexmark",
        "--suite-name", "100m",
        "--category", "oa",
        "--kafka-server", "kafka:9092"
    ]
}

detector = FlinkDetector(
    prometheus_url="http://localhost:9090",
    flink_rest_url="http://localhost:8081",
    migration_plan_path="/path/to/migration_plan.json",
    savepoint_dir="file:///opt/flink/savepoints",
    job_config=job_config  # 傳入配置
)
```

### 2. 自動重啟（預設啟用）

```python
# 自動重啟（預設）
detector.trigger_migration(migration_plan)

# 或明確指定
detector.trigger_migration(migration_plan, auto_restart=True)
```

**執行流程：**
1. 寫入遷移計畫到 JSON 檔案
2. 停止當前 Job 並建立 Savepoint
3. 等待 Job 完全停止（5 秒）
4. **自動**執行 `docker exec` 命令從 Savepoint 重新提交 Job
5. 解析並顯示新的 Job ID

### 3. 手動模式

```python
# 禁用自動重啟，只顯示命令
detector.trigger_migration(migration_plan, auto_restart=False)
```

輸出範例：
```
⚠️ 請手動使用以下指令重新提交 Job:
   docker exec -it jobmanager bash -c "
     export NEXMARK_CONF_DIR=/opt/nexmark && \
     /opt/flink/bin/flink run \
     -s file:/opt/flink/savepoints/savepoint-xxx-yyy \
     -d \
     -c com.github.nexmark.flink.BenchmarkIsoQ7 \
     -p 4 \
     /opt/flink/usrlib/nexmark.jar \
     --queries q7-isolated --location /opt/nexmark --suite-name 100m --category oa --kafka-server kafka:9092
   "
```

## 使用範例

### 範例 1: auto_migrate.py（推薦）

已更新的 `auto_migrate.py` 自動包含 job 配置：

```bash
python3 auto_migrate.py
```

系統會：
- 持續監控叢集狀態
- 檢測到瓶頸時自動生成遷移計畫
- **自動**停止 Job、建立 Savepoint、重新提交

### 範例 2: 自定義腳本

```python
from detector import FlinkDetector

# 配置 Job 資訊
job_config = {
    "container": "jobmanager",
    "entry_class": "your.main.Class",
    "parallelism": 4,
    "jar_path": "/opt/flink/usrlib/your-job.jar",
    "nexmark_conf_dir": "/opt/config",
    "program_args": ["--arg1", "value1", "--arg2", "value2"]
}

# 初始化
detector = FlinkDetector(job_config=job_config)

# 執行檢測
detector.detect_bottleneck()

# 生成遷移計畫
migration_plan = detector.generate_migration_plan()

# 自動執行遷移（包含重啟）
detector.trigger_migration(migration_plan)
```

## 執行的命令

內部會執行類似以下的命令：

```bash
docker exec -i jobmanager bash -c \
  "export NEXMARK_CONF_DIR=/opt/nexmark && \
   /opt/flink/bin/flink run \
   -s file:/opt/flink/savepoints/savepoint-xxx-yyy \
   -d \
   -c com.github.nexmark.flink.BenchmarkIsoQ7 \
   -p 4 \
   /opt/flink/usrlib/nexmark.jar \
   --queries q7-isolated --location /opt/nexmark --suite-name 100m --category oa --kafka-server kafka:9092"
```

## 配置參數說明

| 參數 | 說明 | 範例 |
|------|------|------|
| `container` | Docker 容器名稱 | `"jobmanager"` |
| `entry_class` | Flink Job 主類別 | `"com.github.nexmark.flink.BenchmarkIsoQ7"` |
| `parallelism` | 並行度 | `4` |
| `jar_path` | JAR 檔案在容器內的路徑 | `"/opt/flink/usrlib/nexmark.jar"` |
| `nexmark_conf_dir` | 配置目錄（環境變數） | `"/opt/nexmark"` |
| `program_args` | 程序參數列表 | `["--queries", "q7-isolated", ...]` |

## 錯誤處理

### 自動重啟失敗
如果自動重啟失敗，系統會顯示 savepoint 路徑：

```
❌ 自動重啟失敗，請手動重新提交 Job:
   Savepoint 路徑: file:/opt/flink/savepoints/savepoint-xxx-yyy
```

你可以手動執行：
```bash
docker exec -it jobmanager bash -c "..."
```

### 超時處理
- Docker 命令執行超時：30 秒
- Savepoint 建立超時：120 秒

## 注意事項

1. **Docker 權限**：確保執行腳本的用戶有權限執行 `docker exec` 命令
2. **容器名稱**：確保 `container` 參數與實際 Docker 容器名稱一致
3. **路徑正確性**：所有路徑都是**容器內**的路徑，不是主機路徑
4. **冷卻時間**：預設 5 分鐘冷卻時間，避免頻繁重啟

## 疑難排解

### 問題：Job 未啟動
- 檢查 Docker 容器是否正在運行：`docker ps`
- 檢查 JAR 路徑是否正確
- 查看容器日誌：`docker logs jobmanager`

### 問題：Savepoint 路徑錯誤
- 確保 `savepoint_dir` 使用 `file://` 前綴
- 檢查目錄權限：容器內需要有寫入權限

### 問題：參數錯誤
- 確保 `program_args` 中的參數格式正確
- 使用 `auto_restart=False` 先檢查生成的命令
