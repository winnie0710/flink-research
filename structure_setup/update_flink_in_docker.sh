#!/bin/bash

# 直接更新運行中的 Docker 容器內的 Flink JAR 文件
# 這是最快的方式，不需要重新構建映像

set -e

echo "=========================================="
echo "更新 Docker 容器中的 Flink JAR"
echo "=========================================="

# 找到編譯好的 flink-runtime JAR
RUNTIME_JAR="/home/yenwei/research/flink_source/flink-runtime/target/flink-runtime-1.19.0.jar"

if [ ! -f "$RUNTIME_JAR" ]; then
    echo "❌ 找不到 flink-runtime JAR: $RUNTIME_JAR"
    echo "請先編譯 Flink:"
    echo "  cd /home/yenwei/research/flink_source"
    echo "  mvn clean install -pl flink-runtime -am -DskipTests"
    exit 1
fi

echo "找到 flink-runtime JAR: $RUNTIME_JAR"
echo "檔案大小: $(du -h $RUNTIME_JAR | cut -f1)"

# 檢查容器是否運行
if ! docker ps | grep -q jobmanager; then
    echo "❌ jobmanager 容器未運行！"
    echo "請先啟動容器: docker-compose up -d"
    exit 1
fi

# 備份原有的 JAR（在容器內）
echo ""
echo "步驟 1: 備份原有的 flink-runtime JAR..."
docker exec jobmanager bash -c "cp /opt/flink/lib/flink-runtime-*.jar /opt/flink/lib/flink-runtime.jar.backup 2>/dev/null || true"

# 複製新的 JAR 到容器
echo ""
echo "步驟 2: 複製新的 JAR 到 jobmanager..."
docker cp "$RUNTIME_JAR" jobmanager:/opt/flink/lib/

# 同樣更新所有 taskmanager
echo ""
echo "步驟 3: 更新所有 TaskManager..."
for tm in $(docker ps --filter "name=tm-" --format "{{.Names}}"); do
    echo "  更新 $tm..."
    docker exec $tm bash -c "cp /opt/flink/lib/flink-runtime-*.jar /opt/flink/lib/flink-runtime.jar.backup 2>/dev/null || true"
    docker cp "$RUNTIME_JAR" $tm:/opt/flink/lib/
done

echo ""
echo "✅ JAR 文件已更新到所有容器"
echo ""
echo "=========================================="
echo "現在需要重啟 Flink cluster"
echo "=========================================="
echo ""
echo "執行以下命令重啟:"
echo "  cd /home/yenwei/research"
echo "  docker-compose restart"
echo ""
echo "或者完全重啟:"
echo "  docker-compose down"
echo "  docker-compose up -d"
echo ""
