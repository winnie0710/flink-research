# 快速修復 Jackson 依賴問題

## 問題
```
java.lang.NoClassDefFoundError: com/fasterxml/jackson/databind/ObjectMapper
```

## 已修復
✅ 移除了 Jackson 依賴，使用簡單的 JSON 解析器

## 立即執行的步驟

### 方法 1: 使用腳本（推薦）

```bash
cd /home/yenwei/research/structure_setup
chmod +x rebuild_and_deploy.sh
./rebuild_and_deploy.sh
```

### 方法 2: 手動執行

```bash
# 1. 清理並重新編譯
cd /home/yenwei/research/flink_source
mvn clean install -pl flink-runtime -am -DskipTests

# 2. 停止 Flink
cd /home/yenwei/research
docker-compose down

# 3. 啟動 Flink
docker-compose up -d

# 4. 等待容器啟動
sleep 10

# 5. 複製新的 JAR
docker cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar jobmanager:/opt/flink/lib/
docker cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar tm-20c-1:/opt/flink/lib/
docker cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar tm-20c-2-net:/opt/flink/lib/
docker cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar tm-10c-3-cpu:/opt/flink/lib/
docker cp flink_source/flink-runtime/target/flink-runtime-1.19.0.jar tm-20c-4:/opt/flink/lib/

# 6. 重啟讓 JAR 生效
docker-compose restart

# 7. 等待啟動
sleep 15
```

### 驗證

```bash
# 檢查 JobManager 是否正常啟動
docker logs jobmanager 2>&1 | tail -30

# 應該看到類似這樣的日誌（沒有錯誤）：
# INFO  org.apache.flink.runtime.resourcemanager.ResourceManager - ResourceManager started

# 檢查是否載入 migration plan
docker logs jobmanager 2>&1 | grep "MigrationAware"

# 應該看到：
# ✅ Loaded migration plan with 16 entries from /opt/flink/plan/migration_plan.json
```

### 如果還有錯誤

```bash
# 查看完整錯誤日誌
docker logs jobmanager 2>&1 | grep -A10 "ERROR"

# 檢查 JAR 是否正確
docker exec jobmanager ls -lh /opt/flink/lib/flink-runtime-*.jar

# 檢查類是否存在
docker exec jobmanager jar tf /opt/flink/lib/flink-runtime-*.jar | grep MigrationAware
```

## 修改內容總結

### MigrationAwareResourceAllocationStrategy.java

**移除的依賴：**
```java
import com.fasterxml.jackson.databind.ObjectMapper;
```

**新增的方法：**
```java
private Map<String, String> parseSimpleJson(String json) {
    // 簡單的 JSON 解析器，不需要外部依賴
    // 只支持 { "key": "value", ... } 格式
}
```

**修改的代碼：**
```java
// 舊代碼（使用 Jackson）:
ObjectMapper mapper = new ObjectMapper();
Map<String, String> newPlan = mapper.readValue(content, Map.class);

// 新代碼（不需要依賴）:
Map<String, String> newPlan = parseSimpleJson(content);
```

## 為什麼會有這個問題？

Flink runtime 模組在運行時環境中沒有包含 Jackson 的完整依賴。雖然 Flink 內部使用 Jackson，但它被 shaded（重新打包）到不同的包名下，我們無法直接使用。

解決方案是使用簡單的字符串解析，因為我們的 migration plan 格式很簡單（只有 key-value 對）。

## 下一步

修復完成後，按照 `DEPLOYMENT_STEPS.md` 繼續測試 migration 功能。
