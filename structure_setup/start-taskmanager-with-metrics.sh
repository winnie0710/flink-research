#!/usr/bin/env bash
################################################################################
# TaskManager 啟動腳本，同時啟動 CPU Metric Sender
################################################################################

# 如果設置了 TM_RESOURCE_ID 環境變量，寫入 flink-conf.yaml
if [ -n "$TM_RESOURCE_ID" ]; then
    # 底線格式，與 propose.py 的 replace('-','_') 對齊
    TM_RESOURCE_ID_UNDERSCORE="${TM_RESOURCE_ID//-/_}"
    echo "Setting TaskManager resource-id to: $TM_RESOURCE_ID_UNDERSCORE"
    # 用 sed 覆蓋，不要用 >> 追加
    sed -i "/taskmanager\.resource-id/d" /opt/flink/conf/flink-conf.yaml
    echo "taskmanager.resource-id: $TM_RESOURCE_ID_UNDERSCORE" >> /opt/flink/conf/flink-conf.yaml
fi

# 啟動 TaskManager（背景執行）
# 使用 Flink 的標準啟動方式
/opt/flink/bin/taskmanager.sh start-foreground &
TASKMANAGER_PID=$!

# 等待 TaskManager 完全啟動（等待 10 秒）
sleep 10

# 檢查是否有 nexmark jar 和配置
if [ -f "/opt/flink/usrlib/nexmark.jar" ] && [ -f "/opt/nexmark/nexmark.yaml" ]; then
    echo "Starting CPU Metric Sender..."

    # 讀取 metric reporter 配置
    METRIC_HOST=$(grep "nexmark.metric.reporter.host" /opt/nexmark/nexmark.yaml | awk '{print $2}')
    METRIC_PORT=$(grep "nexmark.metric.reporter.port" /opt/nexmark/nexmark.yaml | awk '{print $2}')

    if [ -z "$METRIC_HOST" ]; then
        METRIC_HOST="jobmanager"
    fi

    if [ -z "$METRIC_PORT" ]; then
        METRIC_PORT="9098"
    fi

    echo "Metric reporter: $METRIC_HOST:$METRIC_PORT"

    # 設定 NEXMARK_CONF_DIR 環境變數
    export NEXMARK_CONF_DIR=/opt/nexmark

    # 啟動 CPU Metric Sender（背景執行）
    java -cp "/opt/flink/usrlib/nexmark.jar:/opt/flink/lib/*" \
        com.github.nexmark.flink.metric.cpu.CpuMetricSender \
        > /tmp/metric-sender.log 2>&1 &

    SENDER_PID=$!
    echo "CPU Metric Sender started (PID: $SENDER_PID)"
    echo "Check logs: docker exec <container> cat /tmp/metric-sender.log"
else
    echo "Nexmark jar or config not found, skipping CPU Metric Sender"
fi

# 等待 TaskManager 進程
wait $TASKMANAGER_PID
