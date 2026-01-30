#!/bin/bash
# 安全提交 Flink Job 的腳本
# 確保舊 Job 完全清理後再提交新 Job

set -e

echo "=== 安全 Job 提交腳本 ==="

# 1. 取消所有運行中的 Job
echo "1. 檢查並取消運行中的 Job..."
RUNNING_JOBS=$(docker exec jobmanager /opt/flink/bin/flink list -r 2>/dev/null | grep -oP '[a-f0-9]{32}' || true)

if [ -n "$RUNNING_JOBS" ]; then
    echo "發現運行中的 Job，正在取消..."
    for JOB_ID in $RUNNING_JOBS; do
        echo "  取消 Job: $JOB_ID"
        docker exec jobmanager /opt/flink/bin/flink cancel "$JOB_ID" 2>/dev/null || true
    done

    # 等待 Job 完全停止
    echo "  等待 Job 完全停止..."
    sleep 5
else
    echo "  沒有運行中的 Job"
fi

# 2. 等待 Prometheus 數據穩定
echo "2. 等待 metrics 穩定..."
sleep 3

# 3. 提交新 Job
echo "3. 提交新 Job..."
docker exec -it jobmanager bash -c "
    export NEXMARK_CONF_DIR=/opt/nexmark && \
    /opt/flink/bin/flink run -d \
    -c com.github.nexmark.flink.BenchmarkIsoQ7 \
    -p 4 \
    /opt/flink/usrlib/nexmark.jar \
    --queries q7-isolated \
    --location /opt/nexmark \
    --suite-name 100m \
    --category oa \
    --kafka-server kafka:9092
"

echo "✅ Job 提交完成"
