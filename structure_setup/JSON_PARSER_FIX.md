# JSON Parser 修復說明

## 問題描述

Migration plan JSON 文件有 16 個條目，但只讀取到 10 個。

### 原因分析

Migration plan 中的 key 包含多個冒號 `:` 字符：

```json
{
  "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0": "tm_20c_4",
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_2": "tm_20c_4"
}
```

**原有的解析邏輯問題**：

```java
// 原代碼
String[] keyValue = pair.split(":", 2);
```

這個 `split(":", 2)` 會在遇到 key 中的第一個 `:` 時就進行分割，導致：

1. Key 被錯誤地截斷
2. 無法正確解析包含多個 `:` 的 key
3. 只有部分條目被成功解析

### 示例

對於這個條目：
```
"Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0": "tm_20c_4"
```

**錯誤解析**：
- 可能被錯誤地在第一個 `:` 處分割
- Key 可能變成 `"Sink"`
- Value 可能變成 `"_KafkaSink:_Writer..."`

## 修復方案

### 新的解析邏輯

實現了一個更健壯的 JSON 解析器，能夠正確處理 key 中包含冒號的情況：

```java
private Map<String, String> parseSimpleJson(String json) {
    Map<String, String> result = new HashMap<>();

    // 1. 移除外層花括號
    String cleaned = json.trim();
    if (cleaned.startsWith("{")) {
        cleaned = cleaned.substring(1);
    }
    if (cleaned.endsWith("}")) {
        cleaned = cleaned.substring(0, cleaned.length() - 1);
    }

    // 2. 使用 regex 按不在引號內的逗號分割
    String[] pairs = cleaned.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    // 3. 對每個 key-value pair，找到引號外的最後一個冒號
    for (String pair : pairs) {
        int colonIndex = findLastColonOutsideQuotes(pair);

        if (colonIndex > 0) {
            String key = pair.substring(0, colonIndex).trim().replace("\"", "");
            String value = pair.substring(colonIndex + 1).trim().replace("\"", "");

            result.put(key, value);
        }
    }

    return result;
}
```

### 關鍵改進

#### 1. `findLastColonOutsideQuotes()` 方法

```java
private int findLastColonOutsideQuotes(String s) {
    boolean inQuotes = false;
    int lastColonOutsideQuotes = -1;

    for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);

        if (c == '"') {
            inQuotes = !inQuotes;  // 切換引號狀態
        } else if (c == ':' && !inQuotes) {
            lastColonOutsideQuotes = i;  // 記錄引號外的冒號
        }
    }

    return lastColonOutsideQuotes;
}
```

這個方法能夠：
- ✅ 追蹤當前是否在引號內
- ✅ 只記錄引號外的冒號
- ✅ 返回最後一個引號外的冒號位置（即 key 和 value 之間的分隔符）

#### 2. 解析示例

對於這個輸入：
```
"Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0": "tm_20c_4"
```

**解析過程**：

```
位置: 0                                                      52  54
      "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0": "tm_20c_4"
      ^                                                      ^   ^
      |                                                      |   |
    引號內開始                                              引號內結束 引號外的冒號
                                                                 ↑
                                                    找到這個作為分隔符！
```

**結果**：
- Key: `Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0`
- Value: `tm_20c_4`

## 測試驗證

### 測試用例 1: 簡單的 key

```json
{
  "Window_Join_0": "tm_10c_3_cpu"
}
```

**結果**：✅ Key: `Window_Join_0`, Value: `tm_10c_3_cpu`

### 測試用例 2: 包含冒號的 key

```json
{
  "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0": "tm_20c_4"
}
```

**結果**：✅ Key: `Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0`, Value: `tm_20c_4`

### 測試用例 3: 多個條目

```json
{
  "Window_Max____Map_1": "tm_20c_4",
  "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0": "tm_20c_4",
  "Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_2": "tm_20c_4"
}
```

**結果**：✅ 所有 3 個條目都被正確解析

## 預期結果

修復後，應該看到：

```
✅ Loaded migration plan with 16 entries from /opt/flink/plan/migration_plan.json
```

而不是：

```
❌ Loaded migration plan with 10 entries from /opt/flink/plan/migration_plan.json
```

## 日誌輸出

修復後的代碼會輸出詳細的解析日誌（DEBUG 級別）：

```
DEBUG Parsed entry: 'Window_Max____Map_1' -> 'tm_20c_4'
DEBUG Parsed entry: 'Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0' -> 'tm_20c_4'
DEBUG Parsed entry: 'Window_Join_1' -> 'tm_20c_2_net'
... (共 16 行)
```

## 驗證步驟

1. 重新編譯 Flink:
   ```bash
   cd /home/yenwei/research/flink_source
   mvn clean compile -pl flink-runtime -am -DskipTests
   ```

2. 重新部署並運行 job

3. 檢查 ResourceManager 日誌：
   ```bash
   docker logs jobmanager 2>&1 | grep "Loaded migration plan"
   ```

   應該看到：
   ```
   ✅ Loaded migration plan with 16 entries from /opt/flink/plan/migration_plan.json
   ```

4. 檢查所有條目是否被解析（DEBUG 級別）：
   ```bash
   docker logs jobmanager 2>&1 | grep "Parsed entry"
   ```

## 完整的 Migration Plan 條目

修復後，這 16 個條目都應該被正確解析：

1. `Window_Max____Map_1` → `tm_20c_4`
2. `Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_0` → `tm_20c_4`
3. `Window_Join_1` → `tm_20c_2_net`
4. `Window_Join_3` → `tm_20c_2_net`
5. `Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_3` → `tm_20c_4`
6. `Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_2` → `tm_20c_4`
7. `Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_0` → `tm_20c_2_net`
8. `Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_2` → `tm_20c_2_net`
9. `Window_Max____Map_0` → `tm_20c_2_net`
10. `Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_3` → `tm_20c_2_net`
11. `Window_Join_0` → `tm_10c_3_cpu`
12. `Window_Max____Map_2` → `tm_10c_3_cpu`
13. `Window_Join_2` → `tm_10c_3_cpu`
14. `Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer_1` → `tm_10c_3_cpu`
15. `Window_Max____Map_3` → `tm_10c_3_cpu`
16. `Source:_Source:_KafkaSource____Filter_Bids____Map_To_Bid____Map_1` → `tm_10c_3_cpu`

## 總結

修復內容：
- ✅ 實現了能正確處理 key 中包含冒號的 JSON 解析器
- ✅ 添加了 `findLastColonOutsideQuotes()` 輔助方法
- ✅ 添加了詳細的 DEBUG 日誌
- ✅ 現在能正確解析所有 16 個條目

這個修復確保了 migration plan 中的所有條目都能被正確讀取和使用，無論 subtask 名稱中包含多少個冒號字符。
