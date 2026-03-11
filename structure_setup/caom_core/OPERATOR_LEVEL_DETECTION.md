# Operator 級別瓶頸檢測

## 概述

`detect_bottleneck()` 方法現在使用 **Operator 級別** 的瓶頸檢測，而不是單個 subtask 級別。

## 核心變更

### 舊邏輯（Subtask 級別）
```
對於每個 subtask:
    if subtask.actual_input_rate > subtask.max_capacity:
        標記該 subtask 為瓶頸
```

**問題**：即使某個 operator 整體有足夠容量，個別 subtask 的數據傾斜也可能被誤判為瓶頸。

### 新邏輯（Operator 級別）
```
對於每個 operator:
    total_actual = sum(所有 subtasks 的 actual_input_rate)
    total_capacity = sum(所有 subtasks 的 max_capacity)

    if total_actual > total_capacity:
        標記該 operator 為瓶頸
        標記該 operator 的所有 subtasks 為瓶頸
```

**優勢**：
1. 更準確地識別真正的瓶頸 operator
2. 避免數據傾斜的誤判
3. 遷移時考慮整個 operator 的所有 subtasks

## 詳細流程

### Step C: 計算 Max Capacity
對每個 subtask 計算其最大處理容量：
```python
max_capacity = observed_rate + ((T_bp + T_idle) / T_busy) × observed_rate
```

### Step D: Operator 級別瓶頸檢測
```python
for each operator:
    # 1. 加總所有 subtasks 的指標
    total_actual_input_rate = Σ(subtask.actual_input_rate)
    total_max_capacity = Σ(subtask.max_capacity)

    # 2. Operator 級別判斷
    if total_actual_input_rate > total_max_capacity:
        # 3. 標記該 operator 的所有 subtasks 為瓶頸
        for each subtask in operator:
            subtask.is_bottleneck = True
            add to bottleneck_subtasks list
```

## 範例

### 場景 1: Operator 級別瓶頸
```
Window_Max operator (4 subtasks):
  - subtask_0: actual=1000, capacity=900  (個別過載)
  - subtask_1: actual=800,  capacity=900  (個別正常)
  - subtask_2: actual=1100, capacity=900  (個別過載)
  - subtask_3: actual=900,  capacity=900  (個別正常)

  Total: actual=3800, capacity=3600  ❌ 總和過載

結果: 所有 4 個 subtasks 都被標記為瓶頸
```

### 場景 2: 數據傾斜但整體正常
```
Window_Join operator (4 subtasks):
  - subtask_0: actual=1500, capacity=1000  (個別過載 - 數據傾斜)
  - subtask_1: actual=500,  capacity=1000  (個別正常)
  - subtask_2: actual=600,  capacity=1000  (個別正常)
  - subtask_3: actual=400,  capacity=1000  (個別正常)

  Total: actual=3000, capacity=4000  ✅ 總和正常

結果: 不被標記為瓶頸 (這是數據傾斜問題，不是容量問題)
```

## 報告格式更新

### 新增欄位
```python
{
    "task_name": "Window_Max____Map",
    "status": "🔴 BOTTLENECK",
    "total_actual_rate": 30848.01,      # 新增：總實際輸入速率
    "total_max_capacity": 28000.50,     # 新增：總最大容量
    "avg_actual_rate": 7712.00,         # 平均值（保留）
    "avg_max_capacity": 7000.13,        # 平均值（保留）
    "bottleneck_count": 4,              # 被標記為瓶頸的 subtask 數量
    "is_bottleneck_operator": True,     # 新增：該 operator 是否為瓶頸
    ...
}
```

### 監控輸出範例
```
當前狀態 (Operator 級別):
  🔴 BOTTLENECK Window_Max____Map:
    - 總實際輸入速率 (Total Actual): 30848.01 rec/s
    - 總最大處理容量 (Total Max): 28000.50 rec/s
    - ⚠️ 過載程度: 10.2% (所有 4 個 subtask 將被遷移)
    - 平均速率: 7712.00 rec/s (Actual) / 7000.13 rec/s (Max)
    - Busy: 215 ms/s, Backpressure: 0 ms/s

  🟢 NORMAL Window_Join:
    - 總實際輸入速率 (Total Actual): 31371.42 rec/s
    - 總最大處理容量 (Total Max): 57967.85 rec/s
    - 平均速率: 7842.86 rec/s (Actual) / 14491.96 rec/s (Max)
    - Busy: 288 ms/s, Backpressure: 64 ms/s
```

## 遷移策略影響

### 遷移決策
當一個 operator 被標記為瓶頸時：
1. **所有 subtasks** 都會被加入 `bottleneck_subtasks` 列表
2. `generate_migration_plan()` 會為這些 subtasks 生成遷移計畫
3. 使用 **Heavy-First** 策略：按 `actual_input_rate` 降序排列
4. 每個 subtask 會被遷移到負載最低的 TaskManager

### 範例遷移輸出
```
🔍 檢測到 4 個瓶頸 Subtask (Heavy-First Order)

📋 計畫遷移: Window_Max____Map_2
   從: tm_10c_3_cpu (CPU: 1.0) -> 到: tm_20c_1 (CPU: 1.5)
   目標 host: tm-20c-1, Normalized Load: 0.00
   Slots: 1/6
   實際輸入: 7758.09 rec/s, 最大容量: 7000.13 rec/s
   過載程度: 10.8%

📋 計畫遷移: Window_Max____Map_3
   從: tm_20c_2_net (CPU: 1.5) -> 到: tm_20c_1 (CPU: 1.5)
   ...
```

## 優點總結

1. **更準確**：基於 operator 整體容量判斷，而非個別 subtask
2. **避免誤判**：數據傾斜不會被誤判為容量瓶頸
3. **整體遷移**：瓶頸 operator 的所有 subtasks 一起遷移，保持一致性
4. **清晰報告**：顯示總和與平均值，便於理解 operator 狀態

## 與 Migration Plan 的整合

`generate_migration_plan()` 仍然會接收所有瓶頸 subtasks 的列表，但現在這個列表是基於 operator 級別判斷生成的：

```python
# detector.py 內部
self._bottleneck_subtasks = [
    (subtask_id, actual_rate, max_capacity)
    for each bottleneck operator
        for each subtask in operator
]
```

這確保了遷移計畫考慮整個 operator 的需求，而不是零散地遷移個別 subtasks。
