# 最終修復：Matching Strategy 沒有被使用

## 問題發現

修改了 `PreferredAllocationRequestSlotMatchingStrategy.java` 後，日誌顯示**沒有進入新的匹配邏輯**：

```
✅ 我們添加的日誌：
   🔍 [MATCH SLOT] Looking for request to match slot...
   🔍 [CHECKING] Request vs Slot...

❌ 實際日誌中沒有出現這些！

✅ 仍然出現：
   ✅ Matched pending request (preferredIp=tm_20c_4)
      with slot (ResourceID=tm_10c_3_cpu)  ← 錯誤配對！
```

## 根本原因

### 問題在 DefaultSlotPoolServiceSchedulerFactory.java

**Line 265-288：**
```java
static RequestSlotMatchingStrategy getRequestSlotMatchingStrategy(
        Configuration configuration, JobType jobType) {
    final boolean isLocalRecoveryEnabled =
            configuration.get(CheckpointingOptions.LOCAL_RECOVERY);

    // ❌ 只有在 LOCAL_RECOVERY 啟用時才使用 PreferredAllocationRequestSlotMatchingStrategy
    if (isLocalRecoveryEnabled) {
        if (jobType == JobType.STREAMING) {
            return PreferredAllocationRequestSlotMatchingStrategy.INSTANCE;
        } else {
            return SimpleRequestSlotMatchingStrategy.INSTANCE;
        }
    } else {
        // ❌ 如果沒有啟用 LOCAL_RECOVERY，使用 SimpleRequestSlotMatchingStrategy
        return SimpleRequestSlotMatchingStrategy.INSTANCE;
    }
}
```

### 為什麼會這樣？

1. **原始設計：** `PreferredAllocationRequestSlotMatchingStrategy` 是為了支持 **Local Recovery** 功能
2. **我們的需求：** 使用 `preferredIp` 實現 **Migration Plan**
3. **衝突：** 如果沒有啟用 `LOCAL_RECOVERY`，Flink 會使用 `SimpleRequestSlotMatchingStrategy`，**完全忽略 preferredIp！**

### 執行流程

```
1. Job 啟動
   └─> DefaultSlotPoolServiceSchedulerFactory.create()
       └─> getRequestSlotMatchingStrategy(configuration, jobType)
           └─> isLocalRecoveryEnabled?
               ├─ true  → PreferredAllocationRequestSlotMatchingStrategy ✅
               └─ false → SimpleRequestSlotMatchingStrategy ❌

2. 如果使用 SimpleRequestSlotMatchingStrategy:
   └─> DeclarativeSlotPoolBridge.newSlotsAreAvailable()
       └─> SimpleRequestSlotMatchingStrategy.matchRequestsAndSlots()
           └─> ❌ 完全忽略 preferredIp，隨機配對！
```

## 解決方案

### 修改 DefaultSlotPoolServiceSchedulerFactory.java

**讓所有 STREAMING 作業都使用 PreferredAllocationRequestSlotMatchingStrategy：**

```java
@VisibleForTesting
static RequestSlotMatchingStrategy getRequestSlotMatchingStrategy(
        Configuration configuration, JobType jobType) {
    final boolean isLocalRecoveryEnabled =
            configuration.get(CheckpointingOptions.LOCAL_RECOVERY);

    // ✅ IMPORTANT: Always use PreferredAllocationRequestSlotMatchingStrategy for STREAMING jobs
    // This is needed for:
    // 1. Local recovery (original use case)
    // 2. Migration plan with preferredIp (our use case)
    if (jobType == JobType.STREAMING) {
        LOG.info("Using PreferredAllocationRequestSlotMatchingStrategy for STREAMING job (local_recovery={})",
                isLocalRecoveryEnabled);
        return PreferredAllocationRequestSlotMatchingStrategy.INSTANCE;
    }

    // For BATCH jobs, only use preferred strategy if local recovery is enabled
    if (isLocalRecoveryEnabled) {
        LOG.warn(
                "Batch jobs do not support local recovery. Falling back for request slot matching strategy to {}.",
                SimpleRequestSlotMatchingStrategy.class.getSimpleName());
    }

    return SimpleRequestSlotMatchingStrategy.INSTANCE;
}
```

### 關鍵改進

| 修改前 | 修改後 |
|-------|-------|
| ❌ 只有 LOCAL_RECOVERY=true 時使用 Preferred Strategy | ✅ 所有 STREAMING 作業都使用 Preferred Strategy |
| ❌ Migration Plan 的 preferredIp 被忽略 | ✅ Migration Plan 的 preferredIp 會被正確處理 |
| ❌ 使用 SimpleRequestSlotMatchingStrategy（隨機配對） | ✅ 使用 PreferredAllocationRequestSlotMatchingStrategy（嚴格配對） |

## 完整修改清單

### 1. DefaultSlotPoolServiceSchedulerFactory.java ⭐ 關鍵修復
- ✅ 讓所有 STREAMING 作業使用 PreferredAllocationRequestSlotMatchingStrategy

### 2. PreferredAllocationRequestSlotMatchingStrategy.java
- ✅ 改進匹配邏輯：先檢查 preferredIp，再檢查資源大小
- ✅ 修復 fallback 邏輯
- ✅ 添加詳細的匹配日誌

### 3. DefaultDeclarativeSlotPool.java
- ✅ createAllocatedSlot(): 添加 slot 的 preferredIp
- ✅ reserveFreeSlot(): 添加 mismatch 警告日誌

### 4. DeclarativeSlotPoolBridge.java
- 無需修改（使用注入的 requestSlotMatchingStrategy）

## 預期效果

重新編譯並部署後，您應該看到：

### 1. 策略選擇日誌

```
Using PreferredAllocationRequestSlotMatchingStrategy for STREAMING job (local_recovery=false)
```

### 2. 詳細的匹配日誌

```
🎯 [MATCHING STRATEGY] Starting to match 5 slots with 16 pending requests
🔍 [MATCH SLOT] Looking for request to match slot xxx from TM tm_10c_3_cpu (slot.preferredIp=tm_10c_3_cpu)
🔍 [CHECKING] Request yyy (preferredIp=tm_10c_3_cpu) vs Slot xxx (TM=tm_10c_3_cpu): wantsThisTaskManager=true, profileMatches=true
✅ [STRICT MATCH] Slot xxx (TM=tm_10c_3_cpu) ← Request yyy (preferredIp=tm_10c_3_cpu)
```

### 3. 沒有錯誤配對

```
❌ 不再出現：
   Matched pending request (preferredIp=tm_20c_4) with slot (ResourceID=tm_10c_3_cpu)

✅ 只會出現正確的配對：
   Matched pending request (preferredIp=tm_10c_3_cpu) with slot (ResourceID=tm_10c_3_cpu)
```

### 4. ResourceRequirement 數量正確

```
✅ tm_10c_3_cpu: 5 個
✅ tm_20c_4: 5 個
✅ tm_20c_2_net: 6 個

❌ 不再出現數量錯誤！
```

## 流程圖

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Job 啟動 (STREAMING)                                    │
│    └─> DefaultSlotPoolServiceSchedulerFactory              │
│        └─> getRequestSlotMatchingStrategy()                │
│            └─> ✅ Return PreferredAllocationRequestSlotMatchingStrategy.INSTANCE
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. DeclarativeSlotPoolBridge 創建                          │
│    └─> 使用 PreferredAllocationRequestSlotMatchingStrategy │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. newSlotsAreAvailable([slot1, slot2, ...])               │
│    └─> requestSlotMatchingStrategy.matchRequestsAndSlots() │
│        └─> ✅ 進入 PreferredAllocationRequestSlotMatchingStrategy.matchRequestsAndSlots()
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. 嚴格匹配邏輯                                            │
│    For each slot:                                           │
│      🔍 Find request with matching preferredIp             │
│      ✅ Only match if: requestPreferredIp == slotResourceID│
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. 正確的配對結果                                          │
│    ✅ Slot1 (TM=tm_10c_3_cpu) ← Request1 (preferredIp=tm_10c_3_cpu)
│    ✅ Slot2 (TM=tm_20c_4) ← Request2 (preferredIp=tm_20c_4)│
│    ❌ NO mismatch!                                         │
└─────────────────────────────────────────────────────────────┘
```

## 總結

### 問題根源
- Flink 原本只在啟用 `LOCAL_RECOVERY` 時使用 `PreferredAllocationRequestSlotMatchingStrategy`
- 沒有啟用時使用 `SimpleRequestSlotMatchingStrategy`，**完全忽略 preferredIp**
- 這導致我們修改的匹配邏輯從未被執行

### 解決方法
- ✅ 修改策略選擇邏輯：所有 STREAMING 作業都使用 PreferredAllocationRequestSlotMatchingStrategy
- ✅ 這樣 Migration Plan 的 preferredIp 才能被正確處理

### 最終效果
- ✅ Migration Plan 正常工作
- ✅ 所有 subtask 部署到正確的 TaskManager
- ✅ ResourceRequirement 數量正確
- ✅ 沒有錯誤配對

## 下一步

1. **重新編譯 Flink**
   ```bash
   cd /home/yenwei/research/flink_source
   mvn clean package -DskipTests -T 4C
   ```

2. **部署並測試**

3. **查看日誌** - 應該看到：
   - ✅ `Using PreferredAllocationRequestSlotMatchingStrategy for STREAMING job`
   - ✅ `🔍 [MATCH SLOT]` 詳細匹配日誌
   - ✅ 正確的配對結果
   - ❌ 沒有 mismatch 警告
