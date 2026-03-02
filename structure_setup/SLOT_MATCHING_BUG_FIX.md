# Slot Matching Bug Fix - preferredIp Mismatch Issue

## 問題描述

### 觀察到的錯誤

```
✅ Matched pending request SlotRequestId{xxx} (preferredIp=tm_10c_3_cpu)
   with slot yyy (ResourceID=tm_20c_4)
```

**這是錯誤的！** 一個要求 `tm_10c_3_cpu` 的 request 被配對到來自 `tm_20c_4` 的 slot。

### 導致的後果

```
Creating ResourceRequirement with preferredIp: tm_10c_3_cpu, count=4  ❌ 少了 1 個
Creating ResourceRequirement with preferredIp: tm_20c_4, count=6      ❌ 多了 1 個
```

原本應該是：
- `tm_10c_3_cpu`: 5 個
- `tm_20c_4`: 5 個

## 根本原因分析

### 問題 1: Slot 的 ResourceProfile 缺少 preferredIp

在 `DefaultDeclarativeSlotPool.createAllocatedSlot()` 中：

**修改前：**
```java
return new AllocatedSlot(
    slotOffer.getAllocationId(),
    taskManagerLocation,
    slotOffer.getSlotIndex(),
    slotOffer.getResourceProfile(),  // ❌ 沒有包含 TaskManager 信息
    taskManagerGateway);
```

當 slot 被創建時，它的 ResourceProfile **沒有包含它來自哪個 TaskManager** 的信息。

### 問題 2: PreferredAllocationRequestSlotMatchingStrategy 無法正確比對

在 `PreferredAllocationRequestSlotMatchingStrategy.matchRequestsAndSlots()` 中：

```java
// Request 要求: preferredIp=tm_10c_3_cpu
// Slot 來自: ResourceID=tm_20c_4
// Slot 的 ResourceProfile.preferredIp: null ❌

// 由於 slot 的 ResourceProfile 沒有 preferredIp，
// 匹配策略無法正確判斷這是一個錯誤的配對
```

### 問題 3: 錯誤配對觸發 adjustRequirements

當 `DeclarativeSlotPoolBridge.reserveFreeSlot()` 調用 `DefaultDeclarativeSlotPool.reserveFreeSlot()`:

```java
// Slot 原本被匹配為: preferredIp=tm_20c_4
// 但 request 要求: preferredIp=tm_10c_3_cpu

if (!previouslyMatchedResourceProfile.equals(requiredSlotProfile)) {
    // 觸發 adjustRequirements:
    // 1. 減少 tm_10c_3_cpu 需求: 5 → 4
    // 2. 增加 tm_20c_4 需求: 5 → 6
    adjustRequirements(previouslyMatchedResourceProfile, requiredSlotProfile);
}
```

## 解決方案

### 修改 1: createAllocatedSlot - 添加 preferredIp 信息

**文件：** `DefaultDeclarativeSlotPool.java`

```java
private AllocatedSlot createAllocatedSlot(
        SlotOffer slotOffer,
        TaskManagerLocation taskManagerLocation,
        TaskManagerGateway taskManagerGateway) {

    // ✅ 重要：將 TaskManager 的 ResourceID 設置為 slot 的 preferredIp
    // 這樣後續的匹配邏輯才能正確識別 slot 來自哪個 TaskManager
    ResourceProfile slotProfileWithLocation = ResourceProfile.newBuilder(slotOffer.getResourceProfile())
            .setPreferredIp(taskManagerLocation.getResourceID().toString())
            .build();

    return new AllocatedSlot(
            slotOffer.getAllocationId(),
            taskManagerLocation,
            slotOffer.getSlotIndex(),
            slotProfileWithLocation,  // ✅ 使用帶有 preferredIp 的 profile
            taskManagerGateway);
}
```

### 修改 2: PreferredAllocationRequestSlotMatchingStrategy - 改進日誌

**文件：** `PreferredAllocationRequestSlotMatchingStrategy.java`

添加詳細的匹配日誌：

```java
// Request has preferredIp: ONLY match if slot is from that specific TaskManager
if (preferredResourceId != null) {
    boolean resourceIdMatches = preferredResourceId.equals(slotResourceId.toString());
    String slotPreferredIp = freeSlot.getResourceProfile().getPreferredIp();

    LOG.info("🔍 [MATCH CHECK] Request.preferredIp='{}', Slot.ResourceID='{}', Slot.ResourceProfile.preferredIp='{}'",
            preferredResourceId, slotResourceId.toString(), slotPreferredIp);

    if (resourceIdMatches) {
        // ✅ 嚴格匹配：只有當 preferredIp 與 ResourceID 一致時才配對
        boolean profileMatches = freeSlot.getResourceProfile().isMatching(pendingRequest.getResourceProfile());
        if (profileMatches) {
            LOG.info("✅ [STRICT MATCH] Slot {} (TM={}) ← Request {} (preferredIp={})",
                    freeSlot.getAllocationId(), slotResourceId,
                    pendingRequest.getSlotRequestId(), preferredResourceId);
            matched = true;
        }
    } else {
        LOG.debug("⏭️ [SKIP] Request wants '{}' but slot is from '{}'",
                preferredResourceId, slotResourceId.toString());
        continue;
    }
}
```

### 修改 3: reserveFreeSlot - 添加錯誤檢測日誌

**文件：** `DefaultDeclarativeSlotPool.java`

當發生 mismatch 時發出警告：

```java
if (!previouslyMatchedResourceProfile.equals(requiredSlotProfile)) {
    log.warn("⚠️ [MISMATCH] Slot {} was initially matched to preferredIp='{}' but is now being reserved for preferredIp='{}'",
            allocationId,
            previouslyMatchedResourceProfile.getPreferredIp(),
            requiredSlotProfile.getPreferredIp());
    log.warn("⚠️ [MISMATCH] This indicates a matching error in DeclarativeSlotPoolBridge!");
    log.warn("⚠️ [MISMATCH] Slot's TaskManager: {}", allocatedSlot.getTaskManagerLocation().getResourceID());

    // ... adjustRequirements 邏輯
}
```

## 預期效果

### 修復前

```
📦 New slot: AllocationID=xxx, ResourceID=tm_20c_4, ResourceProfile={..., preferredIp=null}
📋 Pending request: preferredIp=tm_10c_3_cpu
✅ Matched (錯誤！)
⚠️ [MISMATCH] Slot was initially matched to preferredIp='tm_20c_4' but is now being reserved for preferredIp='tm_10c_3_cpu'
⚠️ [ADJUST] Adjusting requirements: tm_10c_3_cpu: 5→4, tm_20c_4: 5→6
```

### 修復後

```
📦 New slot: AllocationID=xxx, ResourceID=tm_20c_4, ResourceProfile={..., preferredIp=tm_20c_4}
📋 Pending request: preferredIp=tm_10c_3_cpu
🔍 [MATCH CHECK] Request.preferredIp='tm_10c_3_cpu', Slot.ResourceID='tm_20c_4', Slot.ResourceProfile.preferredIp='tm_20c_4'
⏭️ [SKIP] Request wants 'tm_10c_3_cpu' but slot is from 'tm_20c_4' - skipping
❌ No match (正確！)
```

## 流程圖

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ResourceManager 分配 slot 到 TaskManager                │
│    tm_20c_4 → 5 slots                                       │
│    tm_10c_3_cpu → 5 slots                                   │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. DefaultDeclarativeSlotPool.offerSlots()                 │
│    ✅ 修復：createAllocatedSlot() 設置 preferredIp         │
│    Slot.ResourceProfile.preferredIp = tm_20c_4             │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. DeclarativeSlotPoolBridge.newSlotsAreAvailable()       │
│    ✅ 修復：PreferredAllocationRequestSlotMatchingStrategy │
│    嚴格檢查 Request.preferredIp == Slot.ResourceProfile.preferredIp
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. 配對結果                                                │
│    ✅ Request(preferredIp=tm_20c_4) ← Slot(TM=tm_20c_4)   │
│    ✅ Request(preferredIp=tm_10c_3_cpu) ← Slot(TM=tm_10c_3_cpu)
│    ❌ NO mismatch!                                         │
└─────────────────────────────────────────────────────────────┘
```

## 總結

**核心問題：** Slot 的 ResourceProfile 缺少 TaskManager 位置信息（preferredIp）

**解決方法：** 在創建 AllocatedSlot 時，將 TaskManager 的 ResourceID 設置為 slot 的 preferredIp

**效果：**
1. ✅ PreferredAllocationRequestSlotMatchingStrategy 可以正確比對
2. ✅ 不會發生錯誤的 slot 分配
3. ✅ ResourceRequirement 數量保持正確
4. ✅ Migration plan 正常工作

## 測試驗證

重新編譯部署後，應該看到：
- ✅ 沒有 mismatch 警告
- ✅ ResourceRequirement 數量正確（tm_10c_3_cpu=5, tm_20c_4=5）
- ✅ 所有 subtask 都部署到正確的 TaskManager
- ✅ 沒有額外的 ResourceRequirement 創建
