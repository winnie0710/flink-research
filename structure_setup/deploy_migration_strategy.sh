#!/bin/bash

# 部署 Migration-Aware Resource Allocation Strategy
# 這個腳本會編譯 Flink、打包並更新 Docker 容器

set -e  # 遇到錯誤立即退出

echo "=========================================="
echo "部署 Migration-Aware Strategy 到 Flink"
echo "=========================================="

# 步驟 1: 編譯 Flink
echo ""
echo "步驟 1/5: 編譯 Flink..."
cd /home/yenwei/research/flink_source

# 只編譯 flink-runtime 模組會更快
echo "正在編譯 flink-runtime 模組..."
mvn clean install -pl flink-runtime -am -DskipTests -Dfast

if [ $? -ne 0 ]; then
    echo "❌ 編譯失敗！"
    exit 1
fi

echo "✅ 編譯完成"

# 步驟 2: 打包 flink-dist（生成完整的 Flink 發行版）
echo ""
echo "步驟 2/5: 打包 Flink 發行版..."
cd /home/yenwei/research/flink_source
mvn clean package -pl flink-dist -am -DskipTests -Dfast

if [ $? -ne 0 ]; then
    echo "❌ 打包失敗！"
    exit 1
fi

echo "✅ 打包完成"

# 步驟 3: 找到生成的 Flink 發行版
echo ""
echo "步驟 3/5: 查找 Flink 發行版..."
FLINK_DIST=$(find /home/yenwei/research/flink_source/flink-dist/target -name "flink-*.tar.gz" -not -name "*-src.tar.gz" | head -1)

if [ -z "$FLINK_DIST" ]; then
    echo "❌ 找不到 Flink 發行版文件！"
    exit 1
fi

echo "找到 Flink 發行版: $FLINK_DIST"

# 步驟 4: 解壓並複製到 Docker 構建目錄
echo ""
echo "步驟 4/5: 準備 Docker 構建目錄..."

# 創建臨時目錄
TEMP_DIR="/tmp/flink-custom-build"
rm -rf $TEMP_DIR
mkdir -p $TEMP_DIR

# 解壓 Flink
echo "解壓 Flink..."
tar -xzf $FLINK_DIST -C $TEMP_DIR

# 找到解壓後的目錄
FLINK_DIR=$(find $TEMP_DIR -maxdepth 1 -type d -name "flink-*" | head -1)

if [ -z "$FLINK_DIR" ]; then
    echo "❌ 解壓失敗！"
    exit 1
fi

echo "Flink 目錄: $FLINK_DIR"

# 步驟 5: 更新 Docker 容器
echo ""
echo "步驟 5/5: 更新 Docker 容器..."

# 停止當前運行的容器
echo "停止 Flink 容器..."
cd /home/yenwei/research
docker-compose down

# 將新的 Flink lib 複製到容器會掛載的目錄
# 如果你的 docker-compose.yml 中有掛載 lib 目錄，可以直接複製
# 否則需要重新構建 Docker 映像

# 方案 A: 如果有 volume 掛載（推薦）
if [ -d "/home/yenwei/research/flink-lib" ]; then
    echo "複製新的 Flink lib 到掛載目錄..."
    rm -rf /home/yenwei/research/flink-lib/*
    cp -r $FLINK_DIR/lib/* /home/yenwei/research/flink-lib/
fi

# 方案 B: 重新構建 Docker 映像（如果需要）
# 這裡假設你有一個自定義的 Dockerfile
if [ -f "/home/yenwei/research/Dockerfile.flink" ]; then
    echo "重新構建 Flink Docker 映像..."
    docker build -t custom-flink:latest -f /home/yenwei/research/Dockerfile.flink .
fi

echo ""
echo "=========================================="
echo "✅ 部署完成！"
echo "=========================================="
echo ""
echo "下一步操作："
echo "1. 檢查 migration_plan.json 的 resource-id 格式"
echo "2. 啟動 Flink cluster:"
echo "   docker-compose up -d"
echo "3. 查看日誌驗證:"
echo "   docker logs jobmanager 2>&1 | grep MigrationAware"
echo ""
