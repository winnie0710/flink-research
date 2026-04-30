# 研究背景（碩士論文）

## 系統定位

**自適應資源優化系統（Adaptive Resource Optimization System）**

針對 Flink 1.19 邊緣運算 / 異質資源環境，在不增加總體資源的前提下，
動態調整 Job 中特定 Subtask 的擺放位置，以降低延遲、提升吞吐量、減少重啟成本。

系統架構遵循自律計算的 **MAPE-K 迴圈**：

```
Monitor → Analyze → Plan → Execute
   ↑                           |
   └───────────────────────────┘ (feedback loop)
```

---

## 研究環境

- **串流框架**：Apache Flink 1.19.0 + Java 11
- **監控系統**：Prometheus（拉取各節點與 Flink metrics）
- **部署環境**：邊緣運算叢集，節點具有異質性
  - CPU 核心數差異大（算力不均）
  - 網路頻寬與延遲不穩定，節點間差異顯著
  - 節點可能動態加入或離線
- **資源限制**：不可增加節點數量或 slot 總數
- **專案目錄**：`/research/structure_setup/`

---

## Baseline（原論文方法，已完善）

- **位置**：`/research/structure_setup/caom_core/baseline.py`
- **狀態**：✅ 已完善，作為對照組使用，**不需要修改**
- **運作方式**：
  1. 以 **Operator 層級**做瓶頸判斷（BFS 產能還原法）
  2. 將該 Operator 內的**所有 Subtask** 全部列入遷移清單
  3. 用干擾係數排序 Subtask，嘗試遷移到資源更充足的 TaskManager

- **侷限性**（本研究要超越的地方，不是要修復）：
  - BFS 產能還原法在高 backpressure 環境下輸入速率估算**失真**
  - 此失真導致 **Source 算子永遠不會被判定為瓶頸**
  - 以 Operator 為單位遷移，健康的 Subtask 也被一起遷移，Savepoint 狀態體積大、中斷時間長

---

## 本研究方法（propose_v6）

- **位置**：`/research/structure_setup/caom_core/propose_v6.py`
- **狀態**：🔴 `bottleneck_detector` 模組有很大優化空間，**目前無法有效找到瓶頸**

---

## 本研究的核心改進

### 改進一：Subtask 粒度的精準瓶頸偵測

不搬動整個 Operator，只遷移「真正生病」的特定 Subtask。

**理由**：同一個 Operator 內的不同 Subtask 面臨的壓力並不相同，原因可能是：
- **Data Skew**：某個 key 的資料量遠大於其他 key，導致負責該 key 的 Subtask 過載
- **算力不均**：不同 Subtask 所在的 TaskManager CPU 能力不同

精準定位後，只遷移真正的問題 Subtask，大幅減少 Savepoint 狀態體積與系統中斷時間。

### 改進二：瓶頸類型診斷（計算瓶頸 vs 網路瓶頸）

找到瓶頸 Subtask 後，進一步區分其根本原因：

| 瓶頸類型 | 特徵指標 | 遷移策略 |
|---------|---------|---------|
| **計算瓶頸** | CPU 忙碌率高、outputBufferUsage 低 | 遷移到 CPU 算力更強的 TM |
| **網路瓶頸** | outputBufferUsage 高、CPU 不高 | 啟動**拓撲感知親和性演算法**，遷移到網路距離近的 TM |

### 改進三：拓撲感知親和性演算法（針對網路瓶頸）

針對網路瓶頸的 Subtask，在選擇遷移目的地時，加入網路拓撲距離作為評分因子。

- **`calculate_topology_affinity`** 的 hash 部分尚有優化空間（待改進）
- 目標：讓上下游通訊頻繁的 Subtask 盡量落在同一個 TM 或網路距離近的節點

---

## MAPE-K 各階段現況

### Monitor
透過 Prometheus 收集以下指標（**Subtask 粒度**）：
- `T_busy`：CPU 忙碌率
- `T_bp`：反壓時間（backpressure ratio）
- `outPoolUsage`：輸出緩衝區水位
- 實體網路流量
- 其他 Prometheus 指標

> ⚠️ **重要提醒**：backpressure（`T_bp`）只是一個**現象**，不代表處理能力不足。
> 高 backpressure 可能是下游瓶頸傳播的結果，不能單獨作為瓶頸判斷依據。

### Analyze / Plan（核心開發區域，位於 `propose_v6.py`）

**`bottleneck_detector`（🔴 重點優化目標）：**
- 目前無法有效找到瓶頸，需要大幅改進
- 必須能找到**當前可見的瓶頸**（明顯的 CPU 或網路瓶頸）
- 也要嘗試偵測**遷移後可能浮現的潛在瓶頸**（原本被下游反壓壓制、實際上也有問題的算子）
- ⚠️ backpressure 只是現象，不代表該節點是根本瓶頸，需配合其他指標綜合判斷

**瓶頸定位後的流程（目標設計）：**
- 從瓶頸 Operator 中，進一步識別出真正生病的特定 Subtask（而非整個 Operator）
- 診斷瓶頸類型（計算瓶頸 vs 網路瓶頸）
- 計算遷移收益與目的地評分
- 生成 `migration_plan.json`

### Execute（已完善）
- 系統讀取 `migration_plan.json`
- 透過 **Flink Savepoint 機制**重新配置 Subtask 位置
- 將 Subtask 放到指定的 TaskManager 上

---

## 優化目標（三個，需同時考慮）

1. **降低處理延遲（Latency）**：解決資料積壓與緩衝區回堵
2. **提升吞吐量（Throughput）**：在實體頻寬受限下，最大化系統處理上限
3. **減少重啟成本**：在相同時間段內，減少遷移觸發的次數（避免頻繁 Savepoint）

---

## 演算法設計原則

**避免固定閾值**：所有判斷條件要有統計或理論依據，讓論文答辯時能清楚解釋每個決策。

可接受的替代方案：
- **IQR / Tukey fence**：統計學標準異常偵測（1.5×IQR 有 Tukey 1977 文獻依據）
- **Z-score**：與自身歷史基準比較（2σ = 95.4% 信賴區間）
- **相對比較**：與同類 Subtask 的中位數比較（比例意義清楚）
- **若必須使用固定數字**：需說明來源（統計理論、官方文件、或系統物理限制）

---

## 評估方式

- **本研究方法**：`/research/structure_setup/caom_core/propose_v6.py`
- **對照組（Baseline）**：`/research/structure_setup/caom_core/baseline.py`
- **評估指標**：
  - Job 端到端 latency 改善幅度
  - 系統吞吐量變化
  - 遷移觸發次數（Savepoint 次數）
  - 瓶頸偵測準確率（precision / recall / F1）
  - 節點負載標準差（均衡程度）

---

## 待解決問題（開發中）

| 問題 | 位置 | 狀態 |
|------|------|------|
| `bottleneck_detector` 無法有效找到瓶頸 | `propose_v6.py` | 🔴 重點優化 |
| 潛在瓶頸預測（遷移後浮現的新瓶頸） | `propose_v6.py` Analyze 模組 | 🔴 重點優化 |
| `calculate_topology_affinity` hash 優化 | `propose_v6.py` Plan 模組 | 🟡 設計中 |

---

@references/bottleneck-patterns.md
@references/heterogeneous-scheduling.md
