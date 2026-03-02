# Matching Strategy Fix - 修復 preferredIp 錯誤配對

## 問題描述

即使修改了 `createAllocatedSlot()` 添加 preferredIp，仍然出現錯誤配對：

```
✅ Matched pending request (preferredIp=tm_20c_4)
   with slot (ResourceID=tm_10c_3_cpu)  ❌ 錯誤！
```

## 根本原因

### 問題在 PreferredAllocationRequestSlotMatchingStrategy 的迭代邏輯

**原始代碼（錯誤）：**
```java
// 外層循環：遍歷 SLOTS
while (freeSlotsIterator.hasNext()) {
    PhysicalSlot freeSlot = freeSlotsIterator.next();

    // 內層循環：遍歷 REQUESTS
    while (pendingRequestIterator.hasNext()) {
        PendingRequest pendingRequest = pendingRequestIterator.next();

        if (preferredResourceId != null) {
            if (preferredResourceId.equals(slotResourceId)) {
                // ❌ 問題：只檢查了 ResourceProfile.isMatching()
                // 這個方法只比較 CPU、Memory，不比較 preferredIp！
                if (freeSlot.getResourceProfile().isMatching(...)) {
                    matched = true;
                }
            } else {
                continue;  // 跳過這個 request，繼續檢查下一個
            }
        }
    }
}
```

**為什麼會錯誤配對？**

1. Slot 來自 `tm_10c_3_cpu`
2. 嘗試匹配第一個 request (preferredIp=`tm_20c_4`)
   - `tm_20c_4 != tm_10c_3_cpu` → `continue`
3. 嘗試匹配第二個 request (preferredIp=`tm_10c_3_cpu`)
   - `tm_10c_3_cpu == tm_10c_3_cpu` ✓
   - `ResourceProfile.isMatching()` → **只檢查 CPU、Memory，返回 true** ✓
   - **匹配成功！** ❌ 但這是錯的，因為第二個 request 實際上也想要另一個 TM

**核心問題：**
- `ResourceProfile.isMatching()` **不檢查 preferredIp**
- 即使 request 和 slot 的 preferredIp 不同，只要 CPU、Memory 相同就會匹配

## 解決方案

### 修改：嚴格檢查 preferredIp 匹配

**文件：** `PreferredAllocationRequestSlotMatchingStrategy.java`

```java
// IMPROVED MATCHING LOGIC:
// For each slot, find a request with matching preferredIp (strict mode)

while (freeSlotsIterator.hasNext() && !pendingRequestsWithPreferences.isEmpty()) {
    final PhysicalSlot freeSlot = freeSlotsIterator.next();
    final ResourceID slotResourceId = freeSlot.getTaskManagerLocation().getResourceID();

    LOG.info("🔍 [MATCH SLOT] Looking for request to match slot {} from TM {}",
            freeSlot.getAllocationId(), slotResourceId);

    // Try to find a request that wants this specific TaskManager
    final Iterator<PendingRequest> pendingRequestIterator =
            pendingRequestsWithPreferences.values().iterator();

    while (pendingRequestIterator.hasNext()) {
        final PendingRequest pendingRequest = pendingRequestIterator.next();
        final String requestPreferredIp = pendingRequest.getResourceProfile().getPreferredIp();

        boolean matched = false;

        // ✅ STRICT MODE: Request's preferredIp must match slot's TaskManager ResourceID
        if (requestPreferredIp != null) {
            // ✅ 關鍵：檢查 request 是否想要這個 TaskManager
            boolean wantsThisTaskManager = requestPreferredIp.equals(slotResourceId.toString());

            if (wantsThisTaskManager) {
                // ✅ 然後再檢查資源大小
                boolean profileMatches = freeSlot.getResourceProfile().isMatching(
                        pendingRequest.getResourceProfile());

                if (profileMatches) {
                    LOG.info("✅ [STRICT MATCH] Slot {} (TM={}) ← Request {} (preferredIp={})",
                            freeSlot.getAllocationId(), slotResourceId,
                            pendingRequest.getSlotRequestId(), requestPreferredIp);
                    matched = true;
                }
            }
            // ✅ 如果 request 想要不同的 TaskManager，直接跳過（不匹配）
        }

        if (matched) {
            requestSlotMatches.add(RequestSlotMatch.createFor(pendingRequest, freeSlot));
            pendingRequestIterator.remove();
            freeSlotsIterator.remove();
            break;  // 移到下一個 slot
        }
    }
}
```

### 關鍵改進

#### 1. **嚴格的 preferredIp 檢查**

```java
// ✅ 正確：先檢查 preferredIp 是否匹配
boolean wantsThisTaskManager = requestPreferredIp.equals(slotResourceId.toString());

if (wantsThisTaskManager) {
    // 只有在 TaskManager 匹配時，才檢查資源大小
    boolean profileMatches = freeSlot.getResourceProfile().isMatching(...);
    if (profileMatches) {
        matched = true;
    }
}
```

#### 2. **清晰的日誌**

```java
LOG.info("🔍 [MATCH SLOT] Looking for request to match slot {} from TM {}",
        freeSlot.getAllocationId(), slotResourceId);

LOG.info("🔍 [CHECKING] Request {} (preferredIp={}) vs Slot {} (TM={}): wantsThisTaskManager={}, profileMatches={}",
        pendingRequest.getSlotRequestId(), requestPreferredIp,
        freeSlot.getAllocationId(), slotResourceId,
        wantsThisTaskManager, profileMatches);
```

#### 3. **移除有問題的 fallback 邏輯**

**修改前：**
```java
// 排除所有有 preferredIp 的 slot
final List<PhysicalSlot> slotsWithoutPreferredIp = new ArrayList<>();
for (PhysicalSlot slot : freeSlots.values()) {
    if (slot.getResourceProfile().getPreferredIp() == null) {  // ❌ 現在所有 slot 都有 preferredIp
        slotsWithoutPreferredIp.add(slot);
    }
}
```

**修改後：**
```java
// ✅ 允許剩餘的 slot 用於沒有 preferredIp 的 request
if (!freeSlots.isEmpty() && !unmatchedRequests.isEmpty()) {
    LOG.info("🔄 Fallback to SimpleStrategy for {} requests without preferredIp using {} remaining slots",
            unmatchedRequests.size(), freeSlots.size());
    requestSlotMatches.addAll(
            SimpleRequestSlotMatchingStrategy.INSTANCE.matchRequestsAndSlots(
                    freeSlots.values(), unmatchedRequests));
}

// ✅ 記錄等待中的 request
if (!unmatchedRequestsWithPreferredIp.isEmpty()) {
    LOG.info("⏳ {} pending requests with preferredIp are waiting for slots from specific TaskManagers:",
            unmatchedRequestsWithPreferredIp.size());
    for (PendingRequest request : unmatchedRequestsWithPreferredIp) {
        LOG.info("   - SlotRequestId={}, waiting for TaskManager: {}",
                request.getSlotRequestId(),
                request.getResourceProfile().getPreferredIp());
    }
}
```

## 預期效果

### 修復前（錯誤）

```
🎯 [MATCHING STRATEGY] Starting to match 5 slots with 16 pending requests
[沒有詳細的 MATCH CHECK 日誌]
✅ Matched pending request (preferredIp=tm_20c_4) with slot (ResourceID=tm_10c_3_cpu)  ❌
⚠️ [MISMATCH] Slot was initially matched to preferredIp='tm_10c_3_cpu' but is now being reserved for preferredIp='tm_20c_4'
⚠️ [ADJUST] Adjusting requirements: tm_20c_4: 5→4, tm_10c_3_cpu: 5→6
```

### 修復後（正確）

```
🎯 [MATCHING STRATEGY] Starting to match 5 slots with 16 pending requests
🔍 [MATCH SLOT] Looking for request to match slot xxx from TM tm_10c_3_cpu
🔍 [CHECKING] Request yyy (preferredIp=tm_10c_3_cpu) vs Slot xxx (TM=tm_10c_3_cpu): wantsThisTaskManager=true, profileMatches=true
✅ [STRICT MATCH] Slot xxx (TM=tm_10c_3_cpu) ← Request yyy (preferredIp=tm_10c_3_cpu)  ✅
✅ [STRICT MATCH] Slot zzz (TM=tm_20c_4) ← Request www (preferredIp=tm_20c_4)  ✅
[沒有 MISMATCH 警告]
[ResourceRequirement 數量正確：tm_10c_3_cpu=5, tm_20c_4=5]
```

## 流程圖

```
┌─────────────────────────────────────────────────────────────┐
│ 1. newSlotsAreAvailable([slot1, slot2, ...])               │
│    slot1: TM=tm_10c_3_cpu, ResourceProfile.preferredIp=tm_10c_3_cpu
│    slot2: TM=tm_20c_4, ResourceProfile.preferredIp=tm_20c_4
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. PreferredAllocationRequestSlotMatchingStrategy          │
│    For each slot:                                           │
│      🔍 Find request with matching preferredIp             │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Slot1 (TM=tm_10c_3_cpu):                               │
│    - Check Request1 (preferredIp=tm_20c_4)                │
│      ❌ tm_20c_4 != tm_10c_3_cpu → skip                   │
│    - Check Request2 (preferredIp=tm_10c_3_cpu)            │
│      ✅ tm_10c_3_cpu == tm_10c_3_cpu → match!             │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. 匹配結果                                                │
│    ✅ Slot1 ← Request2 (both tm_10c_3_cpu)                │
│    ✅ Slot2 ← Request3 (both tm_20c_4)                    │
│    ❌ NO mismatch!                                         │
│    ✅ ResourceRequirement 正確！                           │
└─────────────────────────────────────────────────────────────┘
```

## 總結

### 核心問題
- `ResourceProfile.isMatching()` 只比較資源大小，不比較 preferredIp
- 原始的匹配邏輯允許不同 preferredIp 的 request 和 slot 配對

### 解決方法
1. ✅ **嚴格檢查 preferredIp**：`requestPreferredIp.equals(slotResourceId)`
2. ✅ **先檢查 TaskManager，再檢查資源大小**
3. ✅ **改進日誌**：清楚顯示每次匹配的決策過程
4. ✅ **修復 fallback 邏輯**：允許剩餘 slot 用於沒有 preferredIp 的 request

### 預期結果
- ✅ 每個 request 只會匹配到對應 TaskManager 的 slot
- ✅ 沒有 mismatch 警告
- ✅ ResourceRequirement 數量保持正確
- ✅ Migration plan 完美工作
