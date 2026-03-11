# Propose.py - Subtask State 大小追蹤

## 概述

已將所有 subtask state 大小追蹤功能完整整合到 `propose.py` 中，用於你的研究方法實現。

## 完整修改列表

### 1. **新增 `format_bytes()` 工具函數** (lines 8-18)

```python
def format_bytes(bytes_value):
    """將字節轉換為人類可讀的格式"""
    # 自動轉換為 B, KB, MB, GB, TB
```

**功能**：將原始字節數值格式化為人類可讀格式
- 0 → "0 B"
- 1048576 → "1.00 MB"
- 1320702444 → "1.23 GB"

### 2. **新增 `get_subtask_state_sizes()` 方法** (lines 84-111)

```python
def get_subtask_state_sizes(self):
    """
    查詢每個 subtask 的狀態大小（用於評估遷移代價）
    返回: { "task_name": { subtask_index: state_size_bytes, ... }, ... }
    """
```

**功能**：
- 嘗試多種 Prometheus 指標查詢狀態大小
- 優先級順序：
  1. `flink_taskmanager_job_task_operator_currentStateSize`
  2. `flink_taskmanager_job_task_operator_lastCheckpointSize`
  3. 其他備選指標
- 成功時輸出：`✅ 使用指標 [metric_name] 獲取狀態大小`

### 3. **在 `detect_bottleneck()` 中集成** (line 127)

```python
# 獲取狀態大小（用於遷移代價評估）
state_size_map = self.get_subtask_state_sizes()
```

**整合點**：在查詢完所有性能指標後，額外查詢狀態大小信息

### 4. **擴展 `task_info` 數據結構** (lines 150-166)

```python
# 獲取狀態大小（bytes）
state_size = state_size_map.get(task_name, {}).get(idx, 0)

task_info[subtask_id] = {
    "task_name": task_name,
    "subtask_index": idx,
    "T_busy": T_busy,
    "T_bp": T_bp,
    "T_idle": T_idle,
    "observed_rate": observed_rate,
    "actual_input_rate": 0.0,
    "max_capacity": 0.0,
    "is_bottleneck": False,
    "state_size": state_size  # 新增：bytes
}
```

**新增欄位**：每個 subtask 的 `task_info` 現在包含 `state_size` (bytes)

### 5. **在 `generate_migration_plan()` 中使用** (lines 451-468)

```python
# Sort bottleneck subtasks by actual_input_rate (descending - heavy first)
# Also collect state size for migration cost evaluation
bottleneck_list = []
for subtask_id, actual_rate in overloaded_subtasks:
    if hasattr(self, '_task_info') and subtask_id in self._task_info:
        task_detail = self._task_info[subtask_id]
        bottleneck_list.append((
            subtask_id,
            task_detail['actual_input_rate'],
            task_detail['T_busy'],
            task_detail.get('state_size', 0)  # 狀態大小 (bytes)
        ))
```

**改動**：bottleneck_list 現在包含 4 個元素（增加 state_size）

### 6. **處理循環中添加 state_size** (lines 481-492)

```python
# Process bottleneck subtasks (heavy first)
for subtask_id, actual_rate, busy_time, state_size in bottleneck_list:
    # ...

    # Migration cost based on state size (for display purposes)
    state_size_mb = state_size / (1024 * 1024) if state_size > 0 else 0
```

**改動**：循環變量增加 `state_size`，並計算 MB 單位

### 7. **顯示遷移代價** (lines 540-547)

```python
if state_size > 0:
    print(f"   💾 State 大小: {format_bytes(state_size)} (遷移代價)")
```

**顯示位置**：在每個遷移計畫的詳細信息中添加狀態大小

## 輸出範例

### 檢測階段
```
✅ 使用指標 flink_taskmanager_job_task_operator_currentStateSize 獲取狀態大小
```

### 遷移計畫階段
```
📋 計畫遷移: Window_Max____Map_2
   從: tm_10c_3_cpu (CPU: 1.0) -> 到: tm_20c_1 (CPU: 1.5)
   目標 host: tm-20c-1, Normalized Load: 0.15
   Slots: 1/6
   實際輸入: 7758.09 rec/s, 最大容量: 7000.13 rec/s
   過載程度: 10.8%
   💾 State 大小: 256.45 MB (遷移代價)

📋 計畫遷移: Window_Join_3
   從: tm_20c_2_net (CPU: 1.5) -> 到: tm_20c_1 (CPU: 1.5)
   目標 host: tm-20c-1, Normalized Load: 0.35
   Slots: 2/6
   實際輸入: 8142.86 rec/s, 最大容量: 7491.96 rec/s
   過載程度: 8.7%
   💾 State 大小: 1.23 GB (遷移代價)
```

## 與 detector.py 的區別

| 特性 | detector.py | propose.py |
|------|-------------|------------|
| 用途 | Baseline 方法 | 你的研究方法 |
| State 追蹤 | ✅ 已實現 | ✅ 已實現 |
| 實現方式 | 完全相同 | 完全相同 |
| 獨立性 | 獨立文件 | 獨立文件 |

兩個文件的 state 追蹤實現完全相同，可以獨立運行。

## 研究應用場景

### 1. **遷移代價評估**
```python
# 在你的研究中可以基於 state_size 做決策
if state_size > threshold:
    # 大狀態 subtask 的特殊處理
    pass
```

### 2. **成本效益分析**
```python
migration_cost = estimate_migration_time(state_size)
performance_gain = (actual_rate - max_capacity) * recovery_time

if performance_gain > migration_cost:
    # 遷移值得
    approve_migration()
```

### 3. **優先級排序**
```python
# 可以基於狀態大小和性能提升綜合排序
priority_score = performance_gain / (state_size + epsilon)
```

## 調試與驗證

### 檢查狀態指標是否可用

在 Prometheus 中查詢：
```
flink_taskmanager_job_task_operator_currentStateSize
```

或使用 curl：
```bash
curl 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_operator_currentStateSize'
```

### 常見問題

**Q: 狀態大小顯示為 0 B？**
A: 可能原因：
- Operator 是無狀態的（如 Map、Filter）
- 指標未啟用或不存在
- Flink 版本不支持該指標

**Q: 無法獲取狀態大小？**
A: 系統會優雅降級：
- 不影響瓶頸檢測
- 不影響遷移計畫生成
- 只是不顯示狀態大小信息

## 未來擴展方向

### 1. 基於狀態的智能遷移
```python
def smart_migration_decision(subtask_id, state_size, performance_gain):
    """基於狀態大小和性能提升的智能決策"""
    if state_size > LARGE_STATE_THRESHOLD:
        # 大狀態需要更高的性能提升才值得遷移
        required_gain = state_size * 0.001  # 調整係數
        return performance_gain > required_gain
    else:
        # 小狀態更容易遷移
        return performance_gain > 0
```

### 2. 遷移時間預估
```python
def estimate_downtime(state_size):
    """預估遷移造成的服務中斷時間"""
    savepoint_time = state_size / SAVEPOINT_SPEED
    recovery_time = state_size / RECOVERY_SPEED
    overhead = BASE_OVERHEAD
    return savepoint_time + recovery_time + overhead
```

### 3. 批量遷移優化
```python
def optimize_batch_migration(bottleneck_subtasks):
    """優化批量遷移：先遷移小狀態的"""
    sorted_by_state = sorted(bottleneck_subtasks,
                            key=lambda x: x['state_size'])
    # 小狀態優先，減少總體中斷時間
    return sorted_by_state
```

## 使用建議

1. **開發階段**：使用 `propose.py` 實現和測試你的研究方法
2. **對比實驗**：保持 `detector.py` 作為 baseline 進行對比
3. **論文撰寫**：可以引用狀態大小作為遷移代價的量化指標
4. **實驗分析**：記錄不同狀態大小下的遷移效果

## 總結

propose.py 現在完整支援：
- ✅ 自動查詢 subtask 狀態大小
- ✅ 在遷移計畫中顯示狀態信息
- ✅ 人類可讀的格式化輸出
- ✅ 優雅的錯誤處理
- ✅ 與原有功能無縫整合

可以在你的研究中靈活使用這些信息進行遷移決策優化！
