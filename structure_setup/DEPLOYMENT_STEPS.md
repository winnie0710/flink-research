# 部署 MigrationAwareResourceAllocationStrategy 的完整步驟

## 問題診斷

當前狀況：
- ✅ `MigrationAwareResourceAllocationStrategy.java` 已創建並編譯成功
- ✅ Migration plan 已在 JobMaster 層級被讀取（有 16 個條目）
- ❌ **ResourceManager 層級的策略沒有被執行**（容器中沒有新的 JAR）

## 解決方案：更新 Docker 容器中的 Flink

### 方法 1：直接替換 JAR（最快，推薦用於測試）

```bash
# 步驟 1: 確認編譯完成
cd /home/yenwei/research/flink_source
ls -lh flink-runtime/target/flink-runtime-1.19.0.jar

# 步驟 2: 使用腳本自動更新
cd /home/yenwei/research/structure_setup
chmod +x update_flink_in_docker.sh
./update_flink_in_docker.sh

# 步驟 3: 重啟 Flink cluster
cd /home/yenwei/research
docker-compose restart

# 步驟 4: 驗證
docker logs jobmanager 2>&1 | grep "MigrationAwareResourceAllocationStrategy"
```

### 方法 2：完整重新編譯和打包（生產環境推薦）

```bash
# 步驟 1: 完整編譯 Flink
cd /home/yenwei/research/flink_source
mvn clean install -DskipTests

# 這會需要較長時間（10-30 分鐘），可以添加 -Dfast 加速：
mvn clean install -DskipTests -Dfast

# 步驟 2: 打包發行版
mvn clean package -pl flink-dist -am -DskipTests

# 步驟 3: 找到生成的發行版
find flink-dist/target -name "flink-*.tar.gz" -not -name "*-src.tar.gz"

# 步驟 4: 解壓並替換 Docker 中的 Flink
# （這一步取決於你的 Docker 配置）

# 步驟 5: 重新構建 Docker 映像（如果有自定義 Dockerfile）
cd /home/yenwei/research
docker-compose down
docker-compose build
docker-compose up -d
```

### 方法 3：使用 Volume 掛載（最靈活）

如果你的 `docker-compose.yml` 使用 volume 掛載 Flink 的 lib 目錄：

```bash
# 1. 停止容器
docker-compose down

# 2. 複製新編譯的 JAR 到掛載目錄
cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar ./flink-lib/

# 3. 重新啟動
docker-compose up -d
```

## 驗證部署成功

### 1. 檢查 JAR 是否更新

```bash
# 查看容器中的 JAR 時間戳
docker exec jobmanager ls -lh /opt/flink/lib/flink-runtime-*.jar

# 檢查是否包含新的類
docker exec jobmanager jar tf /opt/flink/lib/flink-runtime-*.jar | grep MigrationAwareResourceAllocationStrategy
```

預期輸出應該包含：
```
org/apache/flink/runtime/resourcemanager/slotmanager/MigrationAwareResourceAllocationStrategy.class
```

### 2. 檢查日誌

```bash
# 啟動後檢查 ResourceManager 初始化日誌
docker logs jobmanager 2>&1 | grep -A5 "FineGrainedSlotManager"

# 檢查是否載入 migration plan
docker logs jobmanager 2>&1 | grep "MigrationAware"
```

預期輸出：
```
INFO  MigrationAwareResourceAllocationStrategy - ✅ Loaded migration plan with 16 entries
```

### 3. 提交測試 Job

```bash
# 從 savepoint 重啟 job
docker exec jobmanager flink run -s /opt/flink/savepoints/savepoint-xxx -d /path/to/your-job.jar

# 查看 slot 分配日誌
docker logs jobmanager 2>&1 | grep "Allocated slot"
```

預期輸出：
```
INFO  MigrationAwareResourceAllocationStrategy - ✅ Allocated slot for JobID xxx on preferred TaskManager tm_20c_4
```

## 常見問題排查

### 問題 1: 編譯錯誤

```bash
# 確認 Java 版本
java -version  # 應該是 Java 11

# 清理並重新編譯
cd /home/yenwei/research/flink_source
mvn clean
mvn install -DskipTests
```

### 問題 2: JAR 沒有被更新

```bash
# 檢查容器掛載
docker inspect jobmanager | grep -A10 Mounts

# 手動複製
docker cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar jobmanager:/opt/flink/lib/

# 重啟容器
docker restart jobmanager
```

### 問題 3: Migration plan 格式不匹配

檢查 resource-id 格式：

```bash
# 查看 migration plan
cat structure_setup/plan/migration_plan.json

# 查看 TaskManager 的實際 resource-id
docker exec tm-20c-1 cat /opt/flink/conf/flink-conf.yaml | grep resource-id
docker exec jobmanager grep "resource_id" /opt/flink/log/*.log
```

**確保格式一致！**
- Migration plan: `"Window_Max____Map_1": "tm_20c_4"`
- TaskManager config: `taskmanager.resource-id: tm_20c_4`

如果不一致，需要修改：
1. 統一使用底線 `_` 或連字號 `-`
2. 重新生成 migration plan 或修改 TaskManager 配置

## 快速命令參考

```bash
# 編譯
cd /home/yenwei/research/flink_source && mvn clean install -pl flink-runtime -am -DskipTests

# 更新容器
docker cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar jobmanager:/opt/flink/lib/

# 重啟
docker-compose restart

# 驗證
docker logs jobmanager 2>&1 | grep MigrationAware
docker exec jobmanager jar tf /opt/flink/lib/flink-runtime-*.jar | grep MigrationAware
```

## 完整工作流程

1. **生成 migration plan**
   ```bash
   cd structure_setup
   python caom_core/detector.py
   ```

2. **編譯 Flink**
   ```bash
   cd /home/yenwei/research/flink_source
   mvn clean install -pl flink-runtime -am -DskipTests
   ```

3. **更新 Docker**
   ```bash
   ./structure_setup/update_flink_in_docker.sh
   docker-compose restart
   ```

4. **驗證部署**
   ```bash
   docker logs jobmanager 2>&1 | grep "Loaded migration plan"
   ```

5. **停止 Job 並創建 savepoint**
   ```bash
   docker exec jobmanager flink stop <job-id> -p /opt/flink/savepoints
   ```

6. **從 savepoint 重啟**
   ```bash
   docker exec jobmanager flink run -s /opt/flink/savepoints/savepoint-xxx -d your-job.jar
   ```

7. **監控 slot 分配**
   ```bash
   docker logs -f jobmanager | grep "Allocated slot"
   ```
