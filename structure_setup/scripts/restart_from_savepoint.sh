#!/bin/bash
# 從 Savepoint 重啟 Job 的腳本

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <savepoint-path>"
    echo "Example: $0 file:/opt/flink/savepoints/savepoint-6b7904-c4d05d05ec99"
    exit 1
fi

SAVEPOINT_PATH="$1"

echo "=== 從 Savepoint 重啟 Job ==="
echo "Savepoint: $SAVEPOINT_PATH"

# 1. 停止所有運行中的 Job (使用 stop with savepoint 更安全)
echo "1. 停止現有 Job..."
RUNNING_JOBS=$(docker exec jobmanager /opt/flink/bin/flink list -r 2>/dev/null | grep -oP '[a-f0-9]{32}' || true)

if [ -n "$RUNNING_JOBS" ]; then
    for JOB_ID in $RUNNING_JOBS; do
        echo "  停止 Job: $JOB_ID"
        docker exec jobmanager /opt/flink/bin/flink cancel "$JOB_ID" 2>/dev/null || true
    done
    echo "  等待 Job 完全停止..."
    sleep 8
fi

# 2. 等待 metrics 清理
echo "2. 等待系統穩定..."
sleep 5

# 3. 從 Savepoint 重啟
echo "3. 從 Savepoint 重啟 Job..."
docker exec -it jobmanager bash -c "
    export NEXMARK_CONF_DIR=/opt/nexmark && \
    /opt/flink/bin/flink run \
    -s $SAVEPOINT_PATH \
    -d \
    -c com.github.nexmark.flink.BenchmarkIsoQ7 \
    -p 4 \
    /opt/flink/usrlib/nexmark.jar \
    --queries q7-isolated \
    --location /opt/nexmark \
    --suite-name 100m \
    --category oa \
    --kafka-server kafka:9092
"

echo "✅ Job 已從 Savepoint 重啟"
echo "💡 提示: 等待 15-20 秒後檢查 Job 狀態和 metrics"
