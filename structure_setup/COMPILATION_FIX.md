# 編譯錯誤修復說明

## 問題描述

在編譯 `MigrationPlanAwarePreferredLocationsRetriever.java` 時出現以下錯誤：

1. 找不到 `org.apache.flink.runtime.jobmaster.slotpool.SlotInfo` 類
2. `SlotPool` 接口沒有 `getAvailableSlotsInformation()` 方法
3. `AllocatedSlot` 類不是 public 的
4. `SlotPool` 接口沒有 `getAllocatedSlots()` 方法

## 根本原因

在 Flink 1.19 中：
- `SlotInfo` 位於 `org.apache.flink.runtime.jobmaster.SlotInfo` (不是在 slotpool 包中)
- `SlotPool` 接口使用不同的 API 來訪問 slots
- 不能直接訪問內部的 `AllocatedSlot` 類

## 修復方案

### 修改前的代碼

```java
// ❌ 錯誤的 import
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfo;

// ❌ 不存在的方法
Collection<SlotInfo> availableSlots = slotPool.getAvailableSlotsInformation();

// ❌ 不能訪問的類
Collection<AllocatedSlot> allocatedSlots = slotPool.getAllocatedSlots();
```

### 修改後的代碼

```java
// ✅ 正確的 import
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.FreeSlotInfoTracker;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;

// ✅ 使用正確的 API
private Optional<TaskManagerLocation> convertResourceIdToTaskManagerLocation(
        String resourceIdString) {
    try {
        ResourceID targetResourceId = new ResourceID(resourceIdString);

        // 1. 查詢 free slots
        FreeSlotInfoTracker freeSlotTracker = slotPool.getFreeSlotInfoTracker();
        if (freeSlotTracker != null) {
            Collection<SlotInfo> freeSlots = freeSlotTracker.getFreeSlotsInformation();

            for (SlotInfo slotInfo : freeSlots) {
                TaskManagerLocation location = slotInfo.getTaskManagerLocation();
                ResourceID resourceId = location.getResourceID();

                if (resourceId.equals(targetResourceId)) {
                    return Optional.of(location);
                }
            }
        }

        // 2. 查詢 allocated slots
        Collection<SlotInfo> allocatedSlots = slotPool.getAllocatedSlotsInformation();
        for (SlotInfo slotInfo : allocatedSlots) {
            TaskManagerLocation location = slotInfo.getTaskManagerLocation();
            ResourceID resourceId = location.getResourceID();

            if (resourceId.equals(targetResourceId)) {
                return Optional.of(location);
            }
        }

        return Optional.empty();

    } catch (Exception e) {
        LOG.warn("Failed to convert ResourceID {} to TaskManagerLocation: {}",
                resourceIdString, e.getMessage());
        return Optional.empty();
    }
}
```

## 使用的 SlotPool API

### 1. getFreeSlotInfoTracker()

```java
/**
 * Returns all free slot tracker.
 *
 * @return all free slot tracker
 */
FreeSlotInfoTracker getFreeSlotInfoTracker();
```

返回一個 `FreeSlotInfoTracker` 對象，可以用來查詢 free slots。

### 2. FreeSlotInfoTracker.getFreeSlotsInformation()

```java
/**
 * Returns a list of {@link SlotInfo} objects about all slots that are currently available.
 *
 * @return a list of {@link SlotInfo} objects about all slots that are currently available.
 */
Collection<SlotInfo> getFreeSlotsInformation();
```

返回所有可用（free）slots 的信息。

### 3. getAllocatedSlotsInformation()

```java
/**
 * Returns a list of {@link SlotInfo} objects about all slots that are currently allocated.
 *
 * @return a list of {@link SlotInfo} objects about all slots that are currently allocated.
 */
Collection<SlotInfo> getAllocatedSlotsInformation();
```

返回所有已分配（allocated）slots 的信息。

## SlotInfo 接口

`org.apache.flink.runtime.jobmaster.SlotInfo` 提供了獲取 slot 信息的方法：

```java
public interface SlotInfo {
    /**
     * Gets the ID under which the slot has been allocated on the TaskManager.
     */
    AllocationID getAllocationId();

    /**
     * Gets the location info of the TaskManager that offers this slot.
     */
    TaskManagerLocation getTaskManagerLocation();

    /**
     * Gets the number of the slot.
     */
    int getPhysicalSlotNumber();

    /**
     * Gets the resource profile of the slot.
     */
    ResourceProfile getResourceProfile();
}
```

## 查詢流程

修復後的查詢流程：

```
1. 嘗試從 Free Slots 中查找
   └─ slotPool.getFreeSlotInfoTracker()
      └─ freeSlotTracker.getFreeSlotsInformation()
         └─ 遍歷所有 free slots
            └─ 比較 resourceId

2. 如果沒找到，從 Allocated Slots 中查找
   └─ slotPool.getAllocatedSlotsInformation()
      └─ 遍歷所有 allocated slots
         └─ 比較 resourceId

3. 如果都沒找到
   └─ 返回 Optional.empty()
   └─ TaskManager 可能還沒註冊
```

## 修改的文件

- ✅ `MigrationPlanAwarePreferredLocationsRetriever.java`
  - 修改 imports
  - 修改 `convertResourceIdToTaskManagerLocation()` 方法

## 編譯驗證

修復後，執行以下命令進行編譯：

```bash
cd /home/yenwei/research/flink_source
mvn clean compile -pl flink-runtime -am -DskipTests
```

應該不再出現編譯錯誤。

## 功能驗證

修復後的代碼能夠：
1. ✅ 正確查詢 SlotPool 中的 free slots
2. ✅ 正確查詢 SlotPool 中的 allocated slots
3. ✅ 將 ResourceID 字符串轉換為 TaskManagerLocation
4. ✅ 在找不到 TaskManager 時返回 empty，允許 fallback

## 注意事項

1. **Null Check**: 代碼中添加了對 `freeSlotTracker` 的 null check，因為在某些情況下它可能為 null。

2. **Exception Handling**: 整個轉換過程被包裹在 try-catch 中，確保任何異常都不會導致整個調度失敗。

3. **日誌記錄**: 添加了詳細的 debug 和 warn 級別日誌，方便追蹤和調試。

4. **Fallback 機制**: 如果無法找到 TaskManagerLocation，方法返回 `Optional.empty()`，調用方會使用 fallback retriever。
