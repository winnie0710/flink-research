#!/bin/bash

# 快速重新編譯和部署腳本
# 修復依賴問題後使用

set -e

echo "=========================================="
echo "重新編譯並部署 Flink"
echo "=========================================="

cd /home/yenwei/research/flink_source

# 步驟 1: 清理舊的編譯
echo ""
echo "步驟 1/5: 清理舊的編譯..."
mvn clean -pl flink-runtime

# 步驟 2: 重新編譯 flink-runtime
echo ""
echo "步驟 2/5: 編譯 flink-runtime..."
mvn install -pl flink-runtime -am -DskipTests

if [ $? -ne 0 ]; then
    echo "❌ 編譯失敗！"
    exit 1
fi

echo "✅ 編譯成功"

# 步驟 3: 找到編譯好的 JAR
echo ""
echo "步驟 3/5: 檢查編譯產物..."
RUNTIME_JAR="flink-runtime/target/flink-runtime-1.19.0.jar"

if [ ! -f "$RUNTIME_JAR" ]; then
    echo "❌ 找不到 JAR: $RUNTIME_JAR"
    exit 1
fi

echo "找到 JAR: $RUNTIME_JAR"
ls -lh "$RUNTIME_JAR"

# 步驟 4: 停止 Flink cluster
echo ""
echo "步驟 4/5: 停止 Flink cluster..."
cd /home/yenwei/research
docker-compose down

echo "等待容器停止..."
sleep 3

# 步驟 5: 更新 JAR 並重啟
echo ""
echo "步驟 5/5: 啟動 Flink cluster..."
docker-compose up -d

echo "等待容器啟動..."
sleep 5

# 複製新的 JAR 到運行中的容器
echo ""
echo "複製新的 JAR 到容器..."
docker cp /home/yenwei/research/flink_source/$RUNTIME_JAR jobmanager:/opt/flink/lib/

for tm in $(docker ps --filter "name=tm-" --format "{{.Names}}"); do
    echo "  更新 $tm..."
    docker cp /home/yenwei/research/flink_source/$RUNTIME_JAR $tm:/opt/flink/lib/
done

# 再次重啟讓 JAR 生效
echo ""
echo "重啟容器以載入新的 JAR..."
docker-compose restart

echo ""
echo "=========================================="
echo "✅ 部署完成！"
echo "=========================================="
echo ""
echo "驗證步驟："
echo "1. 檢查 JobManager 是否正常啟動:"
echo "   docker logs jobmanager 2>&1 | tail -20"
echo ""
echo "2. 檢查是否載入 migration plan:"
echo "   docker logs jobmanager 2>&1 | grep MigrationAware"
echo ""
echo "3. 檢查是否有錯誤:"
echo "   docker logs jobmanager 2>&1 | grep -i error"
echo ""
