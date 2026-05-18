import requests
# import numpy as np
import argparse
import json
import time
import csv
import os
import subprocess
import re
import math

def format_bytes(bytes_value):
    if bytes_value == 0:
        return "0 B"
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    value = float(bytes_value)
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def match_vertex_to_task_name(vertex_name, task_name):
    def normalize(s):
        s = s.replace('-', '_').replace(' ', '_')
        s = re.sub(r'_+', '_', s)
        return s.lower().strip('_')

    norm_task = normalize(task_name)
    clean_vertex = vertex_name.split('(')[0].strip() if '(' in vertex_name else vertex_name

    if '->' in vertex_name:
        parts = [p.strip().split('(')[0].strip() for p in vertex_name.split('->')]
        norm_parts = [normalize(p) for p in parts if p.strip()]
        if all(p and p in norm_task for p in norm_parts):
            return True

    norm_vertex = normalize(clean_vertex)
    if norm_vertex and norm_vertex in norm_task:
        return True

    norm_task_readable = normalize(task_name.replace('_', ' '))
    if norm_vertex and norm_vertex in norm_task_readable:
        return True

    return False


JOB_CONFIG = {
    "q4": {
        "job_name": "Nexmark_Q4_Isolated__Benchmark_Driver_",
        "target_order": ["Source", "Join", "Max", "Avg", "Sink"],
        "entry_class": "com.github.nexmark.flink.BenchmarkIsoQ4",
        "queries_arg": "q4-isolated",
    },
    "q5": {
        "job_name": "Nexmark_Q5_Isolated__Migration_Test_",
        "target_order": ["Source", "Hop", "Window_Auction", "Window_Max"],
        "entry_class": "com.github.nexmark.flink.BenchmarkIsoQ5",
        "queries_arg": "q5-isolated",
    },
    "q7": {
        "job_name": "Nexmark_Q7_Isolated__Benchmark_Driver_",
        "target_order": ["Source", "Window_Max", "Window_Join", "Sink"],
        "entry_class": "com.github.nexmark.flink.BenchmarkIsoQ7",
        "queries_arg": "q7-isolated",
    },
}


class FlinkDetector:
    def __init__(self, query_type="q7", output_id="t16",
                 prometheus_url="http://localhost:9090",
                 flink_rest_url="http://localhost:8081",
                 migration_plan_path="/home/yenwei/research/structure_setup/plan/migration_plan.json",
                 savepoint_dir="file:///opt/flink/savepoints",
                 migration_record_path=None,
                 job_config=None):
        self.base_url = prometheus_url
        self.flink_rest_url = flink_rest_url
        self.migration_plan_path = migration_plan_path
        self.savepoint_dir = savepoint_dir
        self.last_migration_time = 0
        self.migration_cooldown = 300  # 冷卻時間 5 分鐘，避免頻繁重啟
        self._bottleneck_subtasks = []  # CAOM detection results
        self._task_info = {}  # Task information from CAOM detection

        # --- CAOM Section III-C: Optimized Migration Start Time ---
        self._source_rate_history = []       # 歷史 Source 實際資料產生率 (records/s)
        self._history_max_len = 300          # 最多保留 300 筆（對應論文預測集大小 C 的上限）
        self._pending_migration_plan = None  # 已備妥、等待最佳時機觸發的遷移計畫
        self._migration_scheduled = False    # 是否已進入「等待最佳時間」排程狀態
        self._migration_ready_time = None    # 進入排程狀態的時刻 (time.time())
        self._migration_wait_timeout = 120   # 最長等待秒數，超過即強制執行遷移

        cfg = JOB_CONFIG[query_type]
        self.query_type = query_type
        self.target_order = cfg["target_order"]

        output_dir = f"/home/yenwei/research/structure_setup/output/{output_id}/"
        os.makedirs(output_dir, exist_ok=True)
        self.log_file    = os.path.join(output_dir, f"metrics_{query_type}_baseline.csv")
        self.detail_log  = os.path.join(output_dir, f"migration_details_{query_type}_baseline.csv")
        self.migration_record_path = (migration_record_path
                                      or os.path.join(output_dir, "migration_record.txt"))

        print(f"[Baseline] Monitoring query : {query_type}")
        print(f"[Baseline] Metrics CSV      : {self.log_file}")
        print(f"[Baseline] Details CSV      : {self.detail_log}")

        # Job configuration for auto-restart from savepoint
        self.job_config = job_config or {
            "container": "jobmanager",
            "entry_class": cfg["entry_class"],
            "parallelism": 4,
            "jar_path": "/opt/flink/usrlib/nexmark.jar",
            "nexmark_conf_dir": "/opt/nexmark",
            "program_args": [
                "--queries", cfg["queries_arg"],
                "--location", "/opt/nexmark",
                "--suite-name", "100m",
                "--category", "oa",
                "--kafka-server", "kafka:9092",
                "--submit-only"
            ]
        }

    def query_metric_by_task(self, query):
        """
        向 Prometheus 查詢原始數據，並依照 Task Name 分組
        回傳格式: { "TaskName": { subtask_index: value, ... }, ... }
        """
        try:
            response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query})
            data = response.json()

            if data['status'] != 'success':
                print(f"❌ 查詢失敗: {data.get('error')}")
                return {}

            results = data['data']['result']
            task_map = {}

            for r in results:
                # 取得必要標籤
                task_name = r['metric'].get('task_name', 'Unknown')
                idx = int(r['metric'].get('subtask_index', -1))
                val = float(r['value'][1])

                if idx != -1:
                    if task_name not in task_map:
                        task_map[task_name] = {}
                    task_map[task_name][idx] = val

            return task_map

        except Exception as e:
            print(f"⚠️ 連線錯誤: {e}")
            return {}

    def detect_bottleneck(self):
        """
        CAOM Bottleneck Detection: Identify all potential bottleneck operators in a single pass
        Uses backpressure recovery to calculate actual input rates and max processing capacity
        """
        # Query all required metrics
        busy_data_map = self.query_metric_by_task('flink_taskmanager_job_task_busyTimeMsPerSecond')
        bp_data_map = self.query_metric_by_task('flink_taskmanager_job_task_backPressuredTimeMsPerSecond')
        idle_data_map = self.query_metric_by_task('flink_taskmanager_job_task_idleTimeMsPerSecond')
        rate_data_map = self.query_metric_by_task('flink_taskmanager_job_task_numRecordsInPerSecond')
        # 除了 In，也要抓取 Out 指標
        rate_in_map = self.query_metric_by_task('flink_taskmanager_job_task_numRecordsInPerSecond')
        rate_out_map = self.query_metric_by_task('flink_taskmanager_job_task_numRecordsOutPerSecond')

        # 在 job 仍在執行時查詢 checkpoint state size。
        # 必須在 task_info 建立前完成：trigger_migration 執行 stop_job_with_savepoint 後
        # Flink REST API 就查不到 checkpoint 了，屆時 get_subtask_state_sizes() 只會回傳 {}。
        state_size_map = self.get_subtask_state_sizes()

        if not busy_data_map:
            return []

        # Build task structure and topology
        task_info = {}
        for task_name in busy_data_map.keys():
            subtasks_busy = busy_data_map.get(task_name, {})
            subtasks_bp = bp_data_map.get(task_name, {})
            subtasks_idle = idle_data_map.get(task_name, {})
            subtasks_rate = rate_data_map.get(task_name, {})

            for idx in subtasks_busy.keys():
                T_busy = subtasks_busy.get(idx, 0) / 1000.0  # Convert to seconds
                T_bp = subtasks_bp.get(idx, 0) / 1000.0
                T_idle = subtasks_idle.get(idx, 0) / 1000.0
                # 邏輯：如果是 Source 算子，優先使用 numRecordsOut；否則使用 numRecordsIn
                if "source" in task_name.lower():
                    observed_rate = rate_out_map.get(task_name, {}).get(idx, 0)
                else:
                    observed_rate = rate_in_map.get(task_name, {}).get(idx, 0)

                subtask_id = f"{task_name}_{idx}"

                # 從 state_size_map 查找此 subtask 的狀態大小
                # 先用 match_vertex_to_task_name 模糊比對，找不到再退回精確比對
                state_size = 0
                for vertex_name, vertex_states in state_size_map.items():
                    if match_vertex_to_task_name(vertex_name, task_name):
                        state_size = vertex_states.get(idx, 0)
                        break
                if state_size == 0:
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
                    "state_size": state_size,
                }

        # Group tasks by operator type (needed before Step A)
        operator_groups = {}
        for subtask_id, info in task_info.items():
            task_name = info["task_name"]
            if task_name not in operator_groups:
                operator_groups[task_name] = []
            operator_groups[task_name].append(subtask_id)

        # Order operators by pipeline position
        ordered_operators = []
        for keyword in self.target_order:
            for op_name in operator_groups.keys():
                if keyword in op_name and op_name not in ordered_operators:
                    ordered_operators.append(op_name)
        for op_name in operator_groups.keys():
            if op_name not in ordered_operators:
                ordered_operators.append(op_name)

        # Step A: Recover actual input rate for each Source subtask,
        # then sum to operator-level total_actual for the source operator.
        for op_name in ordered_operators:
            if "Source" not in op_name:
                continue
            for subtask_id in operator_groups[op_name]:
                info = task_info[subtask_id]
                T_busy = info["T_busy"]
                T_bp = info["T_bp"]
                observed_rate = info["observed_rate"]
                if T_busy > 0:
                    # λ̂_j = λ_j × (1 + T_bp / T_busy)
                    info["actual_input_rate"] = observed_rate * (1 + T_bp / T_busy)
                else:
                    info["actual_input_rate"] = observed_rate

        # Step B: Operator-level BFS propagation downstream.
        # For each downstream operator:
        #   1. total_actual[upstream] = Σ actual_input_rate of upstream subtasks
        #   2. total_observed[current] = Σ observed_rate of current subtasks
        #   3. total_observed[upstream] = Σ observed_rate of upstream subtasks
        #   4. total_actual[current] = total_actual[upstream]
        #                              × (total_observed[current] / total_observed[upstream])
        #   5. Distribute proportionally to each subtask by its observed_rate share.
        for i in range(1, len(ordered_operators)):
            upstream_op = ordered_operators[i - 1]
            current_op = ordered_operators[i]

            upstream_subtasks = operator_groups[upstream_op]
            current_subtasks = operator_groups[current_op]

            upstream_total_actual = sum(task_info[st]["actual_input_rate"] for st in upstream_subtasks)
            upstream_total_observed = sum(task_info[st]["observed_rate"] for st in upstream_subtasks)
            current_total_observed = sum(task_info[st]["observed_rate"] for st in current_subtasks)

            if upstream_total_observed > 0:
                # Operator-level actual input rate for current op
                current_total_actual = upstream_total_actual * (current_total_observed / upstream_total_observed)
            else:
                current_total_actual = current_total_observed

            # Distribute back to each subtask proportionally
            for subtask_id in current_subtasks:
                obs = task_info[subtask_id]["observed_rate"]
                if current_total_observed > 0:
                    task_info[subtask_id]["actual_input_rate"] = current_total_actual * (obs / current_total_observed)
                else:
                    task_info[subtask_id]["actual_input_rate"] = obs

        # Step C: Calculate max capacity for each subtask
        for subtask_id, info in task_info.items():
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            T_idle = info["T_idle"]
            observed_rate = info["observed_rate"]

            # Calculate max processing capacity: λ^a = λ + ((T_bp + T_idle) / T_busy) × λ
            if T_busy > 0 and observed_rate > 0:
                max_capacity = observed_rate + ((T_bp + T_idle) / T_busy) * observed_rate
            else:
                max_capacity = observed_rate

            info["max_capacity"] = max_capacity
            info["is_bottleneck"] = False  # Will be set at operator level

        # Step D: Identify bottlenecks at OPERATOR level (not individual subtask level)
        bottleneck_subtasks = []
        bottleneck_operators = set()

        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]

            # Sum actual_input_rate and max_capacity across all subtasks of this operator
            total_actual_input_rate = sum(task_info[st]["actual_input_rate"] for st in subtasks)
            total_max_capacity = sum(task_info[st]["max_capacity"] for st in subtasks)
            avg_busy = sum(task_info[st]["T_busy"] for st in subtasks) / len(subtasks)
            avg_idle = sum(task_info[st]["T_idle"] for st in subtasks) / len(subtasks)

            # Operator-level bottleneck check: total_actual_input_rate > total_max_capacity
            # and avg_idle < 0.8  先不加 效果應該會變差
            if total_actual_input_rate > total_max_capacity and total_max_capacity > 0 :
                bottleneck_operators.add(op_name)
                print(f"Operator '{op_name}' is a bottleneck with total_actual_input_rate={total_actual_input_rate} and total_max_capacity={total_max_capacity}")

                # Mark ALL subtasks of this operator as bottleneck
                for subtask_id in subtasks:
                    task_info[subtask_id]["is_bottleneck"] = True
                    actual_rate = task_info[subtask_id]["actual_input_rate"]
                    max_cap = task_info[subtask_id]["max_capacity"]
                    bottleneck_subtasks.append((subtask_id, actual_rate, max_cap))

        # Generate report
        report_list = []
        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]

            # Aggregate statistics for the operator (use TOTAL not average for bottleneck detection)
            bottleneck_count = sum(1 for st in subtasks if task_info[st]["is_bottleneck"])
            total_actual_rate = sum(task_info[st]["actual_input_rate"] for st in subtasks)
            total_max_capacity = sum(task_info[st]["max_capacity"] for st in subtasks)
            avg_actual_rate = total_actual_rate / len(subtasks) if subtasks else 0
            avg_max_capacity = total_max_capacity / len(subtasks) if subtasks else 0
            max_busy = max(task_info[st]["T_busy"] * 1000 for st in subtasks)  # Convert back to ms
            max_bp = max(task_info[st]["T_bp"] * 1000 for st in subtasks)

            # Status determination (based on operator-level bottleneck detection)
            if op_name in bottleneck_operators:
                status = "🔴 BOTTLENECK"
            elif max_busy > 700:
                status = "🟠 HIGH_LOAD"
            elif max_bp > 500:
                status = "🟡 BACKPRESSURED"
            else:
                status = "🟢 NORMAL"

            report_list.append({
                "task_name": op_name,
                "status": status,
                "bottleneck_count": bottleneck_count,
                "total_actual_rate": round(total_actual_rate, 2),
                "total_max_capacity": round(total_max_capacity, 2),
                "avg_actual_rate": round(avg_actual_rate, 2),
                "avg_max_capacity": round(avg_max_capacity, 2),
                "max_busy": round(max_busy, 1),
                "max_bp": round(max_bp, 1),
                "subtasks": subtasks,
                "is_bottleneck_operator": op_name in bottleneck_operators
            })

        # Store bottleneck info for migration planning
        self._bottleneck_subtasks = bottleneck_subtasks
        self._task_info = task_info

        subtask_locations = self.get_subtask_locations()
        self._write_log(task_info, subtask_locations)

        # CAOM Section III-C: 記錄本輪 Source 實際產生率至歷史序列
        self._record_source_rate(task_info)

        return report_list

    # =========================================================================
    # CAOM Section III-C: Optimized Operator Migration Start Time
    # =========================================================================

    def _record_source_rate(self, task_info):
        """
        從 task_info 取出所有 Source subtask 的 actual_input_rate 加總，
        記錄至歷史序列 _source_rate_history。
        對應論文：Source 的 actual rate 已在 detect_bottleneck() Step A 以
        公式 (10)(11) 計算完畢，此處只負責彙整存入時間序列。
        """
        source_rates = []
        for subtask_id, info in task_info.items():
            if "source" in info.get("task_name", "").lower():
                source_rates.append(info.get("actual_input_rate", 0.0))

        if source_rates:
            total_source_rate = sum(source_rates)
            self._source_rate_history.append(total_source_rate)
            if len(self._source_rate_history) > self._history_max_len:
                self._source_rate_history.pop(0)
            print(f"📈 [CAOM-Timing] Source 實際產生率已記錄: "
                  f"{total_source_rate:.1f} rec/s "
                  f"(歷史筆數: {len(self._source_rate_history)}/{self._history_max_len})")

    def _predict_source_rates(self, horizon_sec, sample_interval_sec):
        """
        用指數平滑（含線性趨勢修正）預測未來 horizon_sec 秒的 Source 資料產生率。

        history 中每一筆代表 sample_interval_sec 秒的觀測平均值；
        輸出序列以「秒」為單位（長度 = horizon_sec），方便後續以秒為粒度搜尋。
        趨勢斜率換算成「每秒變化量」，不受 sample_interval_sec 大小影響。

        對應論文 Section III-C：採用混合時間序列方法（exponential smoothing +
        趨勢項）預測 D_k。若環境已安裝 statsmodels，可替換為 Holt-Winters
        或 ARIMA 以獲得更高精度；此處實作不依賴額外套件，適合冷啟動場景。

        回傳: list[float]，長度為 horizon_sec（每個元素對應 1 秒的預測產生率）
        """
        history = self._source_rate_history
        n_hist = len(history)

        if n_hist < 5:
            # 歷史資料不足：以最近觀測值填充（論文冷啟動處理）
            last_val = history[-1] if history else 0.0
            return [last_val] * horizon_sec

        # ── 指數平滑（alpha = 0.3，偏向歷史穩定值）──────────────────────────
        alpha = 0.3
        smoothed = history[0]
        for val in history[1:]:
            smoothed = alpha * val + (1.0 - alpha) * smoothed

        # ── 線性趨勢：換算成「每秒」斜率，消除 sample_interval_sec 的影響 ──
        # 每筆 history 代表 sample_interval_sec 秒，所以 n 筆跨越 n*interval 秒
        trend_window = min(10, n_hist)
        recent = history[-trend_window:]
        total_sec = max((trend_window - 1) * sample_interval_sec, 1)
        trend_per_sec = (recent[-1] - recent[0]) / total_sec

        # ── 逐秒預測：趨勢隨秒數指數衰減，避免長期外推失真 ─────────────────
        predictions = []
        base = smoothed
        for sec in range(horizon_sec):
            base = base + trend_per_sec * (0.9 ** sec)
            predictions.append(max(0.0, base))

        return predictions

    def _estimate_migration_duration(self):
        """
        估算遷移持續時間 T_Ω（秒）。

        對應論文公式 (3)：
          T_Ω = max_i(serialize_i) + max_i(deserialize_i)
                + 2 * Σ Ser(S_i) / B0
                + σ(S_total)

        此處以「state size → 網路傳輸時間」為主要項，
        序列化/反序列化與對齊開銷以固定經驗值代替，
        適用於無法在線上精確量測各項的場景。
        """
        NETWORK_BW_BYTES_PER_SEC = 6.25 * 1024 * 1024  # B0: 50 Mbps = 6.25 MB/s
        SERIALIZE_OVERHEAD_SEC   = 2.0                  # max(serialize) + max(deserialize) 合計
        ALIGNMENT_OVERHEAD_SEC   = 1.0                  # σ(S_total)：state 一致性對齊開銷

        # 累加所有瓶頸 subtask 的 state size（bytes）
        total_state_bytes = 0
        if self._bottleneck_subtasks and self._task_info:
            for subtask_id, _, _ in self._bottleneck_subtasks:
                total_state_bytes += self._task_info.get(subtask_id, {}).get("state_size", 0)

        if total_state_bytes > 0:
            # 2 * Σ Ser(S_i) / B0：上傳 + 下載各一次
            transfer_sec = (2.0 * total_state_bytes) / NETWORK_BW_BYTES_PER_SEC
            estimated = SERIALIZE_OVERHEAD_SEC + transfer_sec + ALIGNMENT_OVERHEAD_SEC
        else:
            # 無 state 資訊時退回保守估計
            estimated = SERIALIZE_OVERHEAD_SEC + ALIGNMENT_OVERHEAD_SEC

        # 至少 3 秒，避免低估導致在高峰期開始遷移
        result = max(3.0, estimated)
        print(f"⏱️  [CAOM-Timing] 遷移時間估算: {result:.1f}s "
              f"(state={format_bytes(total_state_bytes)}, "
              f"transfer={total_state_bytes / NETWORK_BW_BYTES_PER_SEC * 2:.1f}s)")
        return result

    def _find_optimal_migration_start(self, migration_duration_sec, monitor_interval_sec):
        """
        以「秒」為搜尋單位，在預測窗口內找最佳遷移起始時間。

        搜尋步長固定為 SEARCH_STEP_SEC 秒（與 monitor_interval_sec 無關），
        使決策粒度不受監控週期長短影響，可精確對準資料率谷底。

        對應論文 Section III-C 公式 (8) 與 Theorem 1：
          min Σ_{k=s}^{e} D_{S,k}   (s.t. e = s + T_Ω)

        其中第二項積壓量（公式 5）：
          Σ_{k<s} max(0, D_{S,k} - Δ)
        代表等待期間超過任務消費能力而積壓的資料量。

        參數:
          migration_duration_sec : 遷移持續的估算秒數（= T_Ω）
          monitor_interval_sec   : 歷史資料的取樣週期（秒），只影響預測精度，
                                   不影響搜尋步長

        回傳: (best_sec, best_cost, immediate_cost)
          best_sec      : 最佳起始秒數（0 = 立即遷移）
          best_cost     : 最佳起始時間的遷移成本（records 累積量）
          immediate_cost: 立即遷移（sec=0）的成本（作為基準）
        """
        SEARCH_STEP_SEC = 5   # 搜尋步長：每次往後 5 秒，粒度精細又不過度計算

        if len(self._source_rate_history) < 5:
            return 0, float('inf'), float('inf')

        # 預測窗口：至少覆蓋 3 倍遷移時長，最少 120 秒
        # 對應 Theorem 1：C 需大於 (T_x1 + 1)(1 + (Δ-λmin)/(λmax-Δ))^2
        horizon_sec = max(int(migration_duration_sec * 3), 120)
        predicted_rates = self._predict_source_rates(horizon_sec, monitor_interval_sec)

        # Δ：任務在非瓶頸狀態下的最大資料消費率（records/s）
        # 用所有 subtask max_capacity 的最小值作為整個管線的消費瓶頸
        delta = 0.0
        if self._task_info:
            caps = [info.get("max_capacity", 0.0)
                    for info in self._task_info.values()
                    if info.get("max_capacity", 0.0) > 0]
            delta = min(caps) if caps else 0.0

        dur = int(migration_duration_sec)

        def compute_cost(start_sec):
            """
            計算在 start_sec 秒後開始遷移的總成本 L_i（公式 5）。
            第一項：遷移期間 [start, start+dur] 累積的 Source 產生量
            第二項：等待期間 [0, start) 超出消費能力的積壓量
            """
            end_sec = min(start_sec + dur, len(predicted_rates) - 1)
            migration_data = sum(predicted_rates[start_sec:end_sec + 1])
            pre_backlog    = sum(max(0.0, r - delta)
                                 for r in predicted_rates[:start_sec])
            return migration_data + pre_backlog

        # 立即遷移（sec=0）的成本作為基準
        immediate_cost = compute_cost(0)

        # 線性搜尋最佳起始秒（對應論文的 fundamental search algorithm）
        best_sec  = 0
        best_cost = immediate_cost
        searchable_end = len(predicted_rates) - dur

        for start_sec in range(SEARCH_STEP_SEC, max(searchable_end, 1), SEARCH_STEP_SEC):
            cost = compute_cost(start_sec)
            if cost < best_cost:
                best_cost = cost
                best_sec  = start_sec

        print(f"🔍 [CAOM-Timing] 最佳起始搜尋結果:")
        print(f"   預測窗口    : {horizon_sec}s，搜尋步長 {SEARCH_STEP_SEC}s")
        print(f"   Δ (消費率) : {delta:.1f} rec/s")
        print(f"   立即遷移成本: {immediate_cost:.1f} records")
        print(f"   最佳起始時間: +{best_sec}s")
        print(f"   最佳遷移成本: {best_cost:.1f} records")

        return best_sec, best_cost, immediate_cost

    def _log_timing_decision(self, best_sec, best_cost, immediate_cost,
                             improvement_ratio, decision, monitor_interval_sec):
        """
        將 CAOM 時間選擇決策記錄至 detail_log CSV，方便實驗分析。
        best_sec 直接是秒數（非步數），不需要再乘以 monitor_interval_sec。
        """
        try:
            detail_exists = os.path.isfile(self.detail_log)
            with open(self.detail_log, "a", newline="") as f:
                writer = csv.writer(f)
                if not detail_exists:
                    writer.writerow(["timestamp", "decision_step", "subtask_id",
                                     "priority_rank", "from_tm", "to_tm", "decision_reason"])
                writer.writerow([
                    time.time(),
                    "TimingDecision",
                    "ALL_BOTTLENECKS",
                    "",
                    "",
                    "",
                    (f"CAOM_TIMING: decision={decision}, "
                     f"best_sec={best_sec}s, "
                     f"immediate_cost={immediate_cost:.1f}, "
                     f"best_cost={best_cost:.1f}, "
                     f"improvement={improvement_ratio * 100:.1f}%")
                ])
        except Exception as e:
            print(f"⚠️ 時間決策記錄失敗: {e}")

    def _write_log(self, task_info, subtask_locations):
        """將本輪所有 subtask 狀態寫入 metrics CSV log。"""
        file_exists = os.path.isfile(self.log_file)
        curr_time   = time.time()
        with open(self.log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp", "subtask_id", "current_tm",
                    "status", "cause"
                ])
            for sid, info in task_info.items():
                is_bn  = info.get("is_bottleneck", False)
                status = "CAOM_Bottleneck" if is_bn else "Healthy"
                cause  = "CAOM_BOTTLENECK"  if is_bn else "NONE"
                writer.writerow([
                    curr_time,
                    sid,
                    subtask_locations.get(sid, "unknown"),
                    status,
                    cause,
                ])

    def get_taskmanager_info(self):
        """
        查詢所有 TaskManager 的資訊和當前負載，包含 CPU 容量限制。

        [Fix Bug 1 & 2] 改用 sum(busyTimeMsPerSecond) by resource_id 取代 avg，
        使 tm_load_tracker 與單一 subtask 的 cpu_demand（T_busy × 1000）語義一致：
        兩者都代表 ms/s 的加總，而非平均值。

        返回: { resource_id: {"host": "...", "current_load": float, "cpu_limit": float} }
        """
        try:
            # CPU capacity mapping based on docker-compose.yml
            # Note: Prometheus metrics use underscore format (tm_20c_1) not hyphen (tm-20c-1)
            cpu_capacity_map = {
                "tm_20c_1": 2.0,
                "tm_20c_2": 2.0,
                "tm_20c_3": 2.0,
                "tm_20c_4": 1.0,
                "tm_20c_5": 1.0
            }

            # [Fix Bug 1 & 2] 使用 sum 而非 avg，讓 current_load 代表
            # 該 TM 上所有 subtask busyTimeMsPerSecond 的總和（ms/s）。
            # 這樣 pre-deduct 和 add-back 時直接加減 cpu_demand（同為 ms/s 總和）
            # 才是語義正確的操作，不會產生規模不符導致的負值。
            query = 'sum(flink_taskmanager_job_task_busyTimeMsPerSecond) by (resource_id, tm_id, host)'
            response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query})
            data = response.json()

            tm_info = {}

            if data['status'] == 'success':
                for r in data['data']['result']:
                    # 優先使用 resource_id，如果沒有則使用 tm_id
                    resource_id = r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')
                    host = r['metric'].get('host', 'unknown')

                    # 修正 IP 格式：如果包含下劃線，替換為點號
                    if '_' in host and not '.' in host:
                        host = host.replace('_', '.')

                    current_load = float(r['value'][1])

                    # Map resource_id to CPU capacity
                    cpu_limit = cpu_capacity_map.get(resource_id, 1.0)  # Default to 1.0 if unknown

                    tm_info[resource_id] = {
                        "host": host,
                        "current_load": current_load,
                        "cpu_limit": cpu_limit
                    }

            # Step 2: Add any missing TMs from cpu_capacity_map with zero load
            # This ensures idle/registered TMs are included as migration targets
            for resource_id, cpu_limit in cpu_capacity_map.items():
                if resource_id not in tm_info:
                    # Try multiple common metrics to verify TM exists
                    metric_queries = [
                        f'flink_taskmanager_Status_Flink_Memory_Managed_Total{{resource_id="{resource_id}"}}',
                        f'flink_taskmanager_Status_Shuffle_Netty_UsedMemory{{resource_id="{resource_id}"}}',
                        f'up{{job="taskmanager", resource_id="{resource_id}"}}'
                    ]

                    found = False
                    for query_specific in metric_queries:
                        try:
                            resp = requests.get(f"{self.base_url}/api/v1/query", params={'query': query_specific}, timeout=2)
                            specific_data = resp.json()
                            print(f"ℹ️ 檢查 TaskManager {resource_id} 的 Prometheus 指標")

                            if specific_data['status'] == 'success' and specific_data['data']['result']:
                                # TM is registered and reporting metrics
                                host = specific_data['data']['result'][0]['metric'].get('host', resource_id)
                                if '_' in host and not '.' in host:
                                    host = host.replace('_', '.')

                                tm_info[resource_id] = {
                                    "host": host,
                                    "current_load": 0.0,  # No tasks running, zero load
                                    "cpu_limit": cpu_limit
                                }
                                print(f"ℹ️ 發現空閒 TaskManager: {resource_id} (CPU: {cpu_limit})")
                                found = True
                                break
                        except Exception as e:
                            # Try next metric
                            continue

                    if not found:
                        print(f"⚠️ TaskManager {resource_id} 未在 Prometheus 中找到，可能尚未註冊")

            if not tm_info:
                print(f"❌ 未找到任何 TaskManager")

            return tm_info

        except Exception as e:
            print(f"⚠️ 獲取 TaskManager 資訊失敗: {e}")
            return {}

    def get_job_vertex_mapping(self, job_id):
        try:
            response = requests.get(f"{self.flink_rest_url}/jobs/{job_id}", timeout=5)
            data = response.json()
            vertex_mapping = {}
            if 'vertices' in data:
                for vertex in data['vertices']:
                    vertex_mapping[vertex.get('name', 'Unknown')] = vertex.get('id')
            return vertex_mapping
        except Exception as e:
            print(f"⚠️ 獲取 Vertex 映射失敗: {e}")
            return {}

    def get_latest_checkpoint_id(self, job_id):
        try:
            response = requests.get(f"{self.flink_rest_url}/jobs/{job_id}/checkpoints", timeout=5)
            data = response.json()
            # Flink returns "completed": null when no checkpoint has finished yet.
            # Use .get() chain to avoid AttributeError on the null value.
            completed = data.get('latest', {}).get('completed')
            if completed is not None:
                checkpoint_id = completed.get('id')
                if checkpoint_id:
                    print(f"✅ 找到最新 Checkpoint ID: {checkpoint_id}")
                    return checkpoint_id
            return None
        except Exception as e:
            print(f"⚠️ 獲取 Checkpoint ID 失敗: {e}")
            return None

    def get_subtask_state_sizes(self):
        try:
            running_jobs = self.get_running_jobs()
            if not running_jobs:
                return {}
            job_id = running_jobs[0]
            print(f"📊 正在查詢 Job {job_id} 的狀態大小...")
            checkpoint_id = self.get_latest_checkpoint_id(job_id)
            if not checkpoint_id:
                return {}
            vertex_mapping = self.get_job_vertex_mapping(job_id)
            if not vertex_mapping:
                return {}
            print(f"📋 找到 {len(vertex_mapping)} 個 Operators")
            state_size_map = {}
            for vertex_name, vertex_id in vertex_mapping.items():
                try:
                    url = (f"{self.flink_rest_url}/jobs/{job_id}/checkpoints"
                           f"/details/{checkpoint_id}/subtasks/{vertex_id}")
                    response = requests.get(url, timeout=5)
                    if response.status_code != 200:
                        continue
                    data = response.json()
                    if 'subtasks' in data:
                        subtasks = data['subtasks']
                        print(f"      Subtasks: {len(subtasks)}")
                        for subtask in subtasks:
                            idx = subtask.get('index', -1)
                            size = subtask.get('state_size', 0) or subtask.get('checkpointed_size', 0)
                            if idx != -1:
                                state_size_map.setdefault(vertex_name, {})[idx] = size
                        if vertex_name in state_size_map:
                            total = sum(state_size_map[vertex_name].values())
                            print(f"   ✅ {vertex_name}: {len(state_size_map[vertex_name])} subtasks, "
                                  f"總計 {format_bytes(total)}")
                except Exception:
                    continue
            if state_size_map:
                print(f"✅ 成功從 Checkpoint {checkpoint_id} 獲取狀態大小")
            return state_size_map
        except Exception as e:
            print(f"⚠️ 獲取狀態大小失敗: {e}")
            return {}

    def get_subtask_locations(self):
        """
        查詢每個 subtask 當前所在的 TaskManager resource ID
        返回: { "task_name_0": "tm-10c-3-cpu", ... }
        """
        try:
            query = 'flink_taskmanager_job_task_busyTimeMsPerSecond'
            response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query})
            data = response.json()

            if data['status'] != 'success':
                return {}

            subtask_locations = {}
            for r in data['data']['result']:
                task_name = r['metric'].get('task_name', 'Unknown')
                subtask_index = r['metric'].get('subtask_index', '-1')
                # 優先使用 resource_id，如果沒有則使用 tm_id，最後才用 host
                resource_id = r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')

                subtask_id = f"{task_name}_{subtask_index}"
                subtask_locations[subtask_id] = resource_id

            return subtask_locations

        except Exception as e:
            print(f"⚠️ 獲取 Subtask 位置失敗: {e}")
            return {}

    def get_taskmanager_network_usage(self):
        """
        查詢每個 TaskManager 的當前網路吞吐量（bytes/s），
        以各 TM 上所有 subtask 的 numBytesOutPerSecond 加總為代理指標。
        返回: { resource_id: bytes_per_second }
        """
        try:
            query = 'sum(flink_taskmanager_job_task_numBytesOutPerSecond) by (resource_id, tm_id, host)'
            response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query})
            data = response.json()

            network_usage = {}
            if data['status'] == 'success':
                for r in data['data']['result']:
                    resource_id = r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')
                    network_usage[resource_id] = float(r['value'][1])

            return network_usage

        except Exception as e:
            print(f"⚠️ 獲取網路使用量失敗: {e}")
            return {}

    def print_subtask_status(self):
        """
        印出每個 Subtask 的即時指標表格，包含：
        Busy(ms/s)、BP(ms/s)、Idle(ms/s)、In(MB/s)、inPoolUsage、outPoolUsage、TaskManager。
        欄位順序與 propose_v8.py 一致，以便跨版本對照比較。
        """
        busy_map     = self.query_metric_by_task('flink_taskmanager_job_task_busyTimeMsPerSecond')
        bp_map       = self.query_metric_by_task('flink_taskmanager_job_task_backPressuredTimeMsPerSecond')
        idle_map     = self.query_metric_by_task('flink_taskmanager_job_task_idleTimeMsPerSecond')
        rate_map     = self.query_metric_by_task('flink_taskmanager_job_task_numBytesInPerSecond')
        in_pool_map  = self.query_metric_by_task('flink_taskmanager_job_task_buffers_inPoolUsage')
        out_pool_map = self.query_metric_by_task('flink_taskmanager_job_task_buffers_outPoolUsage')
        subtask_locations = self.get_subtask_locations()

        if not busy_map:
            print("⚠️ 無法取得監控數據")
            return

        col_w = [80, 10, 10, 10, 12, 10, 11, 20]
        header = (f"{'Subtask ID':<{col_w[0]}}"
                  f"{'Busy(ms/s)':>{col_w[1]}}"
                  f"{'BP(ms/s)':>{col_w[2]}}"
                  f"{'Idle(ms/s)':>{col_w[3]}}"
                  f"{'In(MB/s)':>{col_w[4]}}"
                  f"{'inPool':>{col_w[5]}}"
                  f"{'outPool':>{col_w[6]}}"
                  f"{'TaskManager':>{col_w[7]}}")
        print("\n" + "=" * sum(col_w))
        print(header)
        print("=" * sum(col_w))

        # 按 target_order 關鍵字排序，其餘附在最後
        ordered_names = []
        for keyword in self.target_order:
            for name in busy_map:
                if keyword.lower() in name.lower() and name not in ordered_names:
                    ordered_names.append(name)
        for name in busy_map:
            if name not in ordered_names:
                ordered_names.append(name)

        for task_name in ordered_names:
            for idx in sorted(busy_map[task_name].keys()):
                subtask_id = f"{task_name}_{idx}"
                rate_mb    = rate_map.get(task_name, {}).get(idx, 0.0) / (1024 * 1024)
                print(f"{subtask_id:<{col_w[0]}}"
                      f"{busy_map.get(task_name, {}).get(idx, 0.0):>{col_w[1]}.1f}"
                      f"{bp_map.get(task_name, {}).get(idx, 0.0):>{col_w[2]}.1f}"
                      f"{idle_map.get(task_name, {}).get(idx, 0.0):>{col_w[3]}.1f}"
                      f"{rate_mb:>{col_w[4]}.3f}"
                      f"{in_pool_map.get(task_name, {}).get(idx, 0.0):>{col_w[5]}.3f}"
                      f"{out_pool_map.get(task_name, {}).get(idx, 0.0):>{col_w[6]}.3f}"
                      f"{subtask_locations.get(subtask_id, 'unknown'):>{col_w[7]}}")
        print("=" * sum(col_w) + "\n")

    def generate_migration_plan(self, overloaded_subtasks=None):
        """
        Baseline Migration Planner: CAOM + Neptune + WASP/Amnis

        Step 1 (CAOM):   Entire bottleneck operator's subtasks enter migration set Ω
                         (CAOM flaw: healthy subtasks included, causing unnecessary downtime)
        Step 2 (Neptune): Sort Ω by interference score = (cpu_norm + net_norm)^n, descending
                          (Neptune flaw: blends CPU and network into one opaque score)
        Step 3 (WASP+Amnis):
          - WASP hard filter: available_slots >= 1  AND
                              (current_network + subtask_network) <= alpha * network_capacity
          - Amnis objective:  argmin(C_v / O_v) among candidates
                              C_v = sum(busyMs on TM) + subtask_cpu_demand  (both in ms/s)
                              O_v = cpu_limit * MAX_SLOTS_PER_TM * 1000     (ms/s ceiling)

        [Fix Bug 1 & 2]
          tm_load_tracker 初始化自 sum(busyTimeMsPerSecond) per TM（已在
          get_taskmanager_info 修正為 sum query），與 cpu_demand（T_busy×1000）
          同單位，pre-deduct / add-back 不再產生不合理負值。
          O_v 改為 cpu_limit × MAX_SLOTS_PER_TM × 1000，代表 TM 可承擔的
          busy ms/s 總上限，使 C_v/O_v 比率有實際物理意義。

        [Fix Bug 4]
          net_demand 改用每個 subtask 的 numBytesOutPerSecond（bytes/s），
          與 network_capacity_map 及 tm_net_tracker 同單位，移除依賴全叢集
          宏觀比率換算的 records_to_bytes_scale，避免瓶頸期係數爆炸導致
          WASP 濾除所有候選節點。

        Returns: { "subtask_id": "target_resource_id", ... }
        """
        # ── Build migration set Ω from CAOM detection results ──────────────────
        if overloaded_subtasks is None:
            if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
                print("⚠️ 未檢測到瓶頸，無需產生遷移計畫")
                return None
            overloaded_subtasks = [(subtask_id, actual_rate)
                                   for subtask_id, actual_rate, max_cap in self._bottleneck_subtasks]

        tm_info = self.get_taskmanager_info()
        current_locations = self.get_subtask_locations()
        network_usage = self.get_taskmanager_network_usage()

        if not tm_info:
            print("⚠️ 無法獲取 TaskManager 資訊，無法產生遷移計畫")
            return None

        if not overloaded_subtasks:
            print("⚠️ 未檢測到過載的 subtask，無需產生遷移計畫")
            return None

        # ── Constants ──────────────────────────────────────────────────────────
        MAX_SLOTS_PER_TM = 5
        NEPTUNE_N = 3          # Neptune interference score amplifier exponent
        WASP_ALPHA = 0.9       # WASP bandwidth threshold ratio

        # Network capacity map: outbound bandwidth ceiling (bytes/s)
        # tm_20c_2_net and tm_20c_4 are network-constrained in docker-compose.yml
        network_capacity_map = {
            "tm_20c_1":     int(37.5 * 1024 * 1024),   # 300 Mbit/s = 37.5 MB/s
            "tm_20c_2":     int(18.75 * 1024 * 1024),  # 150 Mbit/s = 18.75 MB/s   ← network-constrained TM
            "tm_20c_3":     int(18.75 * 1024 * 1024),  # 150 Mbit/s = 18.75 MB/s   ← network-constrained TM
            "tm_20c_4":     int(37.5 * 1024 * 1024),   # 300 Mbit/s = 37.5 MB/s
            "tm_20c_5":     int(37.5 * 1024 * 1024),   # 300 Mbit/s = 37.5 MB/s
        }

        # ── Initialize slot occupancy ──────────────────────────────────────────
        slot_occupancy = {rid: 0 for rid in tm_info.keys()}
        for subtask_id, resource_id in current_locations.items():
            if resource_id in slot_occupancy:
                slot_occupancy[resource_id] += 1

        print(f"\n📊 初始 Slot 佔用情況:")
        for rid, count in slot_occupancy.items():
            cpu_limit = tm_info[rid]['cpu_limit']
            net_used = network_usage.get(rid, 0) / (1024 * 1024)
            net_cap = network_capacity_map.get(rid, int(37.5 * 1024 * 1024)) / (1024 * 1024)
            print(f"   {rid}: {count}/{MAX_SLOTS_PER_TM} slots, CPU: {cpu_limit}, "
                  f"Net: {net_used:.1f}/{net_cap:.0f} MB/s")

        # ── [Fix Bug 4] 查詢每個 subtask 的 numBytesOutPerSecond ───────────────
        # 直接使用 bytes/s，與 network_capacity_map 同單位，不需要跨量綱換算。
        bytes_out_map = self.query_metric_by_task(
            'flink_taskmanager_job_task_numBytesOutPerSecond'
        )

        # ── Step 2: Neptune — compute interference scores and sort Ω ──────────
        # CPU demand proxy  : T_busy (ms/s, range 0–1000)
        # [Fix Bug 4] Network demand proxy: numBytesOutPerSecond (bytes/s) per subtask
        migration_candidates = []
        for subtask_id, actual_rate in overloaded_subtasks:
            if hasattr(self, '_task_info') and subtask_id in self._task_info:
                info = self._task_info[subtask_id]
                task_name = info['task_name']
                idx = info['subtask_index']
                cpu_demand = info['T_busy'] * 1000       # ms/s
                # [Fix Bug 4] 直接取該 subtask 的 bytes/s，不再透過宏觀比率換算
                net_demand_bytes = bytes_out_map.get(task_name, {}).get(idx, 0.0)
            else:
                cpu_demand = actual_rate
                net_demand_bytes = 0.0
            migration_candidates.append({
                'subtask_id': subtask_id,
                'actual_rate': actual_rate,
                'cpu_demand': cpu_demand,
                'net_demand': net_demand_bytes,   # bytes/s
            })

        # Normalize CPU and network demands to [0, 1] range
        max_cpu = max((c['cpu_demand'] for c in migration_candidates), default=1.0) or 1.0
        max_net = max((c['net_demand'] for c in migration_candidates), default=1.0) or 1.0

        for c in migration_candidates:
            cpu_norm = c['cpu_demand'] / max_cpu
            net_norm = c['net_demand'] / max_net
            # Neptune interference score: blended normalized demand, amplified by exponent n
            # Flaw: CPU bottleneck vs network bottleneck are indistinguishable after blending
            c['interference_score'] = pow(cpu_norm + net_norm, NEPTUNE_N)

        # Sort descending by interference score (highest-interference subtasks migrated first)
        migration_candidates.sort(key=lambda x: x['interference_score'], reverse=True)
        bottleneck_subtask_ids = {c['subtask_id'] for c in migration_candidates}

        print(f"\n🔍 [Neptune] {len(migration_candidates)} 個 Subtask 已按干擾分數排序 (n={NEPTUNE_N})")
        for c in migration_candidates:
            print(f"   {c['subtask_id']}: score={c['interference_score']:.4f} "
                  f"(cpu_demand={c['cpu_demand']:.1f}, net_demand_bytes={c['net_demand']:.2f})")

        # ── Initialize migration plan: everyone stays put initially ───────────
        migration_plan = {}
        for subtask_id, current_resource_id in current_locations.items():
            migration_plan[subtask_id] = current_resource_id

        # Track mutable state: load and network usage per TM
        # [Fix Bug 1 & 2] tm_load_tracker 現在存的是 sum(busyMs) per TM（ms/s 總和），
        # 和 cpu_demand 同單位，pre-deduct / add-back 才有意義。
        tm_load_tracker = {rid: info['current_load'] for rid, info in tm_info.items()}
        # [Fix Bug 4] tm_net_tracker 使用 bytes/s，與 net_demand（bytes/s）同單位
        tm_net_tracker = {rid: network_usage.get(rid, 0.0) for rid in tm_info.keys()}

        # ── Backpressure-aware traffic inflation ──────────────────────────────
        # Under heavy backpressure, Prometheus traffic metrics collapse to near-zero,
        # causing bandwidth validation to underestimate actual load after restart.
        # Inflate by min(1/(1-avg_bp), 5.0) when avg_bp > 0.6.
        if hasattr(self, '_task_info') and self._task_info:
            bp_vals = [info.get('T_bp', 0.0) for info in self._task_info.values()]
            avg_bp = sum(bp_vals) / len(bp_vals) if bp_vals else 0.0
        else:
            avg_bp = 0.0
        bp_mult = min(1.0 / max(1.0 - avg_bp, 0.01), 5.0) if avg_bp > 0.6 else 1.0
        if bp_mult > 1.0:
            print(f"\n⚠️ 系統反壓 avg_bp={avg_bp:.2%}，流量估算倍率 ×{bp_mult:.2f}")
            for rid in tm_net_tracker:
                tm_net_tracker[rid] *= bp_mult

        # ── Hard operator count limit for narrow-BW TMs ───────────────────────
        # Window_Join (Q7) needs ~12 MB/s/subtask; Window_Auction_Count (Q5)
        # needs ~32 MB/s/subtask. Narrow TMs (≤20 MB/s) cannot safely host >1
        # Window_Join or any Window_Auction_Count subtask.
        NARROW_BW_THRESHOLD = int(20 * 1024 * 1024)
        HEAVY_OP_LIMITS = {"Window_Join": 1, "Window_Auction_Count": 0}
        narrow_tms = {rid for rid, cap in network_capacity_map.items()
                      if cap <= NARROW_BW_THRESHOLD}
        heavy_op_count = {rid: 0 for rid in tm_info}
        if hasattr(self, '_task_info'):
            bottleneck_ids = {c['subtask_id'] for c in migration_candidates}
            for sid, loc_rid in current_locations.items():
                if sid in bottleneck_ids or loc_rid not in heavy_op_count:
                    continue
                task_nm = self._task_info.get(sid, {}).get('task_name', '')
                for kw in HEAVY_OP_LIMITS:
                    if kw in task_nm:
                        heavy_op_count[loc_rid] += 1
                        break

        # ── [Fix pre-deduct → lazy deduct] ────────────────────────────────────
        # 廢除 pre-deduct（統一提前扣除）。
        # 原 pre-deduct 問題：把所有瓶頸 subtask 的負載從源 TM 統一扣除後，
        # 源 TM 的 tracker 變成極低甚至負值，導致後續 subtask 被選回源 TM
        # 時造成「磁鐵效應」——空出來的 TM 反而最吸引人。
        # 改為 lazy deduct：只有在「確認分配到某 TM」之後才更新 tracker，
        # 包括從源 TM 扣除（確認離開）和對目標 TM 加入（確認進入），
        # 讓每一步的 tracker 狀態都反映真實的漸進分配結果。

        # ── Step 3: WASP filter + Amnis argmin(C_v / O_v) selection ──────────
        for candidate in migration_candidates:
            subtask_id = candidate['subtask_id']
            current_resource_id = current_locations.get(subtask_id, 'unknown')

            if current_resource_id not in tm_info:
                print(f"⚠️ {subtask_id} 的 resource_id {current_resource_id} 不在 TM 列表中，跳過")
                continue

            subtask_cpu_load = candidate['cpu_demand']       # ms/s
            # 乘以 bp_mult 補償反壓期間 Prometheus 指標崩塌導致的嚴重低估
            subtask_net_bytes = candidate['net_demand'] * bp_mult   # bytes/s

            best_node = None
            best_congestion_ratio = float('inf')

            task_nm_wasp = ''
            if hasattr(self, '_task_info') and candidate['subtask_id'] in self._task_info:
                task_nm_wasp = self._task_info[candidate['subtask_id']].get('task_name', '')

            for rid, info in tm_info.items():
                # ── WASP hard constraint 1: available computing slots ──────
                if slot_occupancy[rid] >= MAX_SLOTS_PER_TM:
                    continue

                # ── WASP hard constraint 2: bandwidth threshold ────────────
                # [Fix Bug 4] subtask_net_bytes 已是 bytes/s，與 net_cap 同單位，直接比較
                net_cap = network_capacity_map.get(rid, int(37.5 * 1024 * 1024))
                if (tm_net_tracker[rid] + subtask_net_bytes) > WASP_ALPHA * net_cap:
                    continue

                # ── WASP hard constraint 3: heavy operator count on narrow TMs ──
                heavy_blocked = False
                for kw, max_cnt in HEAVY_OP_LIMITS.items():
                    if kw in task_nm_wasp and rid in narrow_tms:
                        if heavy_op_count.get(rid, 0) >= max_cnt:
                            heavy_blocked = True
                            break
                if heavy_blocked:
                    continue

                # ── Amnis objective: argmin(C_v / O_v) ───────────────────
                # C_v: TM 上現有 busy 總量 + 即將放入的 subtask busy 需求（ms/s）
                # O_v: [Fix 修正三] 動態容量上限 = cpu_limit × (已用slots+1) × 1000
                #   用「放入後的 slot 數」而非固定 MAX_SLOTS_PER_TM 當分母，
                #   使快滿的 TM 的 O_v 自然縮小、C_v/O_v 相對偏高，
                #   Amnis 會傾向把新 subtask 導向更空閒的 TM。
                C_v = tm_load_tracker[rid] + subtask_cpu_load
                O_v = info['cpu_limit'] * (slot_occupancy[rid] + 1) * 1000.0
                congestion_ratio = C_v / O_v if O_v > 0 else float('inf')
                print(f"   {subtask_id} -> {rid}: C_v={C_v:.3f}, O_v={O_v:.3f}, congestion_ratio={congestion_ratio:.3f} ")

                if congestion_ratio < best_congestion_ratio:
                    best_congestion_ratio = congestion_ratio
                    best_node = rid

            if best_node:
                migration_plan[subtask_id] = best_node

                # [Fix lazy deduct] 確認分配後才更新 tracker：
                # 1. 目標 TM 加入新 subtask 的負載
                slot_occupancy[best_node] += 1
                tm_load_tracker[best_node] += subtask_cpu_load
                tm_net_tracker[best_node] += subtask_net_bytes
                # 2. 源 TM 扣除離開的 subtask 負載（只有真正換 TM 才扣）
                if best_node != current_resource_id and current_resource_id in tm_load_tracker:
                    slot_occupancy[current_resource_id] = max(0, slot_occupancy[current_resource_id] - 1)
                    tm_load_tracker[current_resource_id] -= subtask_cpu_load
                    tm_net_tracker[current_resource_id] -= subtask_net_bytes
                # Update heavy_op_count for the destination TM
                for kw in HEAVY_OP_LIMITS:
                    if kw in task_nm_wasp:
                        heavy_op_count[best_node] = heavy_op_count.get(best_node, 0) + 1
                        break

                if hasattr(self, '_task_info') and subtask_id in self._task_info:
                    info = self._task_info[subtask_id]
                    max_cap = info['max_capacity']
                    overload_pct = ((candidate['actual_rate'] - max_cap) / max_cap * 100) if max_cap > 0 else 0
                    print(f"📋 [WASP+Amnis] 遷移: {subtask_id}")
                    print(f"   從: {current_resource_id} -> 到: {best_node} "
                          f"(CPU: {tm_info[best_node]['cpu_limit']})")
                    print(f"   C_v/O_v={best_congestion_ratio:.3f}, "
                          f"Slots: {slot_occupancy[best_node]}/{MAX_SLOTS_PER_TM}")
                    print(f"   過載程度: {overload_pct:.1f}%, "
                          f"干擾分數: {candidate['interference_score']:.4f}")
                else:
                    print(f"📋 [WASP+Amnis] 遷移: {subtask_id} -> {best_node} "
                          f"(C_v/O_v={best_congestion_ratio:.3f})")
            else:
                # WASP/Amnis 無法找到符合硬性限制的節點（排擠效應）
                # 退路策略：嘗試原位；若原位 slot 也不足，則隨機放到任一有空位的 TM
                if slot_occupancy.get(current_resource_id, 0) < MAX_SLOTS_PER_TM:
                    fallback_node = current_resource_id
                    fallback_reason = "原位 slot 充足，維持原位"
                else:
                    fallback_node = next(
                        (rid for rid, cnt in slot_occupancy.items() if cnt < MAX_SLOTS_PER_TM),
                        None
                    )
                    fallback_reason = f"原位 slot 已滿，隨機放到 {fallback_node}" if fallback_node else "所有 TM slot 已滿，無法安置"

                if fallback_node:
                    migration_plan[subtask_id] = fallback_node
                    slot_occupancy[fallback_node] += 1
                    tm_load_tracker[fallback_node] += subtask_cpu_load
                    tm_net_tracker[fallback_node] += subtask_net_bytes
                    print(f"⚠️ [WASP+Amnis] 無符合條件節點: {subtask_id} → {fallback_reason}")
                else:
                    print(f"❌ [WASP+Amnis] 無法安置: {subtask_id}，所有 TM slot 已滿")

        print(f"\n📊 最終 Slot 佔用情況:")
        for rid, count in slot_occupancy.items():
            cpu_limit = tm_info[rid]['cpu_limit']
            C_v = tm_load_tracker[rid]
            O_v = cpu_limit * max(count, 1) * 1000.0  # 動態 O_v，與選擇時一致
            print(f"   {rid}: {count}/{MAX_SLOTS_PER_TM} slots, "
                  f"C_v/O_v={C_v/O_v:.3f} (cpu_limit={cpu_limit})")

        print(f"\n✅ 遷移計畫包含 {len(migration_plan)} 個 subtask")
        migrated_count = sum(
            1 for sid in bottleneck_subtask_ids
            if migration_plan.get(sid) != current_locations.get(sid)
        )
        print(f"   需要遷移: {migrated_count} 個")
        print(f"   維持原位: {len(migration_plan) - migrated_count} 個")

        return migration_plan

    def write_migration_plan(self, migration_plan):
        """
        將遷移計畫寫入 JSON 檔案
        """
        try:
            # 確保目錄存在
            os.makedirs(os.path.dirname(self.migration_plan_path), exist_ok=True)

            with open(self.migration_plan_path, 'w') as f:
                json.dump(migration_plan, f, indent=2)

            print(f"✅ 遷移計畫已寫入: {self.migration_plan_path}")
            return True
        except Exception as e:
            print(f"❌ 寫入遷移計畫失敗: {e}")
            return False

    def get_running_jobs(self):
        """
        獲取所有正在運行的 Flink Job
        """
        try:
            response = requests.get(f"{self.flink_rest_url}/jobs")
            data = response.json()

            running_jobs = []
            for job in data.get('jobs', []):
                if job['status'] == 'RUNNING':
                    running_jobs.append(job['id'])

            return running_jobs
        except Exception as e:
            print(f"⚠️ 獲取 Job 列表失敗: {e}")
            return []

    def stop_job_with_savepoint(self, job_id):
        """
        使用 Savepoint 停止 Job
        返回 savepoint 路徑
        """
        try:
            url = f"{self.flink_rest_url}/jobs/{job_id}/stop"
            payload = {
                "targetDirectory": self.savepoint_dir,
                "drain": False
            }

            print(f"🛑 停止 Job {job_id} 並建立 Savepoint...")
            response = requests.post(url, json=payload)
            data = response.json()

            # 獲取 trigger ID
            trigger_id = data.get('request-id')

            # 輪詢等待 savepoint 完成
            max_wait = 120  # 最多等待 2 分鐘
            start_time = time.time()

            while time.time() - start_time < max_wait:
                status_url = f"{self.flink_rest_url}/jobs/{job_id}/savepoints/{trigger_id}"
                status_response = requests.get(status_url)
                status_data = status_response.json()

                if status_data['status']['id'] == 'COMPLETED':
                    savepoint_path = status_data['operation']['location']
                    print(f"✅ Savepoint 完成: {savepoint_path}")
                    return savepoint_path

                time.sleep(2)

            print(f"⚠️ Savepoint 超時")
            return None

        except Exception as e:
            print(f"❌ 停止 Job 失敗: {e}")
            return None

    def submit_job_from_savepoint(self, savepoint_path):
        """
        從 Savepoint 重新提交 Job (使用 docker exec 執行 flink run 命令)
        """
        try:
            # 構建 flink run 命令參數
            config = self.job_config
            program_args_str = " ".join(config["program_args"])

            # 構建完整的 docker exec 命令
            flink_cmd = (
                f"export NEXMARK_CONF_DIR={config['nexmark_conf_dir']} && "
                f"/opt/flink/bin/flink run "
                f"-s {savepoint_path} "
                f"-d "
                f"-c {config['entry_class']} "
                f"-p {config['parallelism']} "
                f"{config['jar_path']} "
                f"{program_args_str}"
            )

            docker_cmd = [
                "docker", "exec", "-i", config["container"],
                "bash", "-c", flink_cmd
            ]

            print(f"🚀 從 Savepoint 重新提交 Job...")
            print(f"   執行命令: {' '.join(docker_cmd)}")

            # 執行命令（增加超時時間到 60 秒，因為從 savepoint 恢復需要時間）
            process = subprocess.Popen(docker_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)

            print(f"🚀 已發送提交指令，正在確認 Job 狀態...")

            # 2. 輪詢 Flink REST API 確認是否有新 Job 變成 RUNNING
            # 主動去詢問 Flink REST API
            max_poll_time = 60
            poll_start = time.time()

            while time.time() - poll_start < max_poll_time:
                running_jobs = self.get_running_jobs()
                if running_jobs:
                    print(f"✅ 偵測到 Job 已進入 RUNNING 狀態 (耗時: {time.time() - poll_start:.1f}s)")
                    # 立刻終止本地 docker exec process 與容器內的 Nexmark driver，
                    # 防止 MetricReporter 在 ~16s 後自動 cancel 新 job
                    try:
                        process.kill()
                    except Exception:
                        pass
                    try:
                        subprocess.run(
                            ["docker", "exec", config["container"],
                             "pkill", "-f", config["entry_class"]],
                            capture_output=True, timeout=5
                        )
                        print(f"✅ Nexmark driver 已終止，防止 MetricReporter 干擾")
                    except Exception:
                        pass
                    return running_jobs[0]
                time.sleep(1)

            # 3. 如果輪詢超時，再檢查一次 process 是否報錯
            stdout, stderr = process.communicate(timeout=5)
            if process.returncode != 0:
                print(f"❌ 命令執行失敗: {stderr}")
                return None
            return True

        except Exception as e:
            print(f"❌ 提交失敗: {e}")
            return None

        except subprocess.TimeoutExpired:
            print(f"⚠️ 重新提交 Job 超時 (超過 60 秒)")
            print(f"   💡 Job 可能已經成功提交，請檢查 Flink Web UI")
            print(f"   💡 如果 Job 正在運行，可以忽略此錯誤")
            # 即使超時也返回 True，因為 Job 可能已經成功提交
            return True
        except Exception as e:
            print(f"❌ 重新提交 Job 失敗: {e}")
            return None


    def _wait_for_slots_freed(self, expected_slots: int, timeout: int = 30) -> bool:
        """
        [FIX-9] 等待全叢集 freeSlots 總數達到預期值。

        設計依據：
          Flink 的 Job 狀態到達 CANCELED/FINISHED 的時間點，
          早於各 TM 實際將 Slot 從 ALLOCATED 改回 FREE 的時間點。
          若立刻呼叫 flink run，JobManager 在 Slot offer 視窗內
          看不到足夠的 freeSlots，會跳過未回報的 TM（尤其弱節點）。

        判斷依據（/taskmanagers REST API）：
          total_free = Σ tm.freeSlots（所有 TM）
          expected_slots = 本次停止 Job 所持有的 Slot 總數（= parallelism）

        回傳：
          True  = freeSlots 已達預期（安全重啟）
          False = 超時（繼續執行但有風險，呼叫端負責記錄警告）
        """
        print(f"⏳ [FIX-9] 等待叢集 freeSlots 回到 {expected_slots} 個（超時 {timeout}s）...")
        deadline = time.time() + timeout
        last_free = -1
        while time.time() < deadline:
            try:
                resp = requests.get(f"{self.flink_rest_url}/taskmanagers", timeout=5)
                tms = resp.json().get("taskmanagers", [])
                total_free = sum(tm.get("freeSlots", 0) for tm in tms)
                if total_free != last_free:
                    print(f"   freeSlots={total_free} / {expected_slots}")
                    last_free = total_free
                if total_free >= expected_slots:
                    print(f"✅ [FIX-9] freeSlots 已達 {total_free}，Slot 釋放完成")
                    return True
            except Exception as e:
                print(f"   ⚠️ 查詢 /taskmanagers 失敗: {e}")
            time.sleep(1)
        print(f"⚠️ [FIX-9] 等待 Slot 釋放超時（{timeout}s），freeSlots={last_free}，強制繼續")
        return False

    def _wait_for_all_tms_have_free_slot(self, timeout: int = 20) -> bool:
        """
        [FIX-9] 確認每一個已知 TM 都回報至少 1 個 freeSlot。

        設計依據：
          _wait_for_slots_freed 確認總量夠了，但仍可能有某個弱節點
          的 freeSlots 還是 0（例如 tm_20c_5 單核且剛才 CPU 飽和，
          JVM GC 導致回應延遲）。
          JobManager 送出 slot request 後，若某 TM 在 offer 視窗內
          沒有回報 freeSlot，就會被完全跳過，導致該 TM 上的
          Subtask 沒有位置可以啟動，Job 啟動失敗。

        回傳：
          True  = 所有 TM 都有 freeSlot
          False = 超時（有 TM 沒有回報 freeSlot，呼叫端負責記錄警告）
        """
        print(f"⏳ [FIX-9] 確認每個 TM 都有 freeSlot（超時 {timeout}s）...")
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = requests.get(f"{self.flink_rest_url}/taskmanagers", timeout=5)
                tms = resp.json().get("taskmanagers", [])
                if not tms:
                    time.sleep(1)
                    continue
                not_ready = [
                    (tm.get("id", "?"), tm.get("freeSlots", 0))
                    for tm in tms if tm.get("freeSlots", 0) == 0
                ]
                if not not_ready:
                    print(f"✅ [FIX-9] 所有 {len(tms)} 個 TM 均有 freeSlot，可以安全重啟")
                    return True
                print(f"   等待 TM freeSlot: 尚未就緒={[tm_id for tm_id, _ in not_ready]}")
            except Exception as e:
                print(f"   ⚠️ 查詢 /taskmanagers 失敗: {e}")
            time.sleep(1)
        print(f"⚠️ [FIX-9] 等待所有 TM 就緒超時（{timeout}s），強制繼續（有風險）")
        return False

    # 在 FlinkDetector 類別中新增一個輔助方法來檢查 Job 是否已消失 移除硬編碼的 time.sleep(5)
    def wait_for_job_termination(self, job_id, max_wait_sec=30):
        """輪詢 REST API 確保 Job 已經不在 RUNNING 狀態，釋放資源"""
        start_wait = time.perf_counter()
        while time.perf_counter() - start_wait < max_wait_sec:
            try:
                running_jobs = self.get_running_jobs()
                if job_id not in running_jobs:
                    print(f"✅ Job {job_id} 已確認停止並釋放資源")
                    return True
            except Exception:
                pass
            time.sleep(0.5) # 每 0.5 秒檢查一次，遠比 sleep(5) 快
        return False

    # 觸發完整的遷移流程
    def trigger_migration(self, migration_plan, job_id=None, auto_restart=True):
        current_time = time.time()
        if current_time - self.last_migration_time < self.migration_cooldown:
            remaining = self.migration_cooldown - (current_time - self.last_migration_time)
            print(f"⏳ 遷移冷卻中，剩餘 {remaining:.0f} 秒")
            return False

        if not self.write_migration_plan(migration_plan):
            return False

        if job_id is None:
            running_jobs = self.get_running_jobs()
            if not running_jobs: return False
            job_id = running_jobs[0]

        # 遷移前確認 job 仍在 RUNNING，避免對已 FINISHED 的 job 取 savepoint
        try:
            resp = requests.get(f"{self.flink_rest_url}/jobs/{job_id}")
            current_state = resp.json().get('state', '')
            if current_state != 'RUNNING':
                print(f"⚠️ Job {job_id} 狀態為 {current_state}，跳過遷移")
                return False
        except Exception as e:
            print(f"⚠️ 無法確認 job 狀態: {e}")
        # 遷移前先記錄當前 subtask 位置（遷移後位置由 get_subtask_locations 再取）
        current_locs = self.get_subtask_locations()

        # --- 遷移計時開始 ---
        # 紀錄絕對時間戳記 (Epoch time)，用於與 latency_monitor 對齊
        migration_event_timestamp = time.time()
        start_migration = time.perf_counter()

        # 1. Stop with Savepoint
        stop_start = time.perf_counter()
        savepoint_path = self.stop_job_with_savepoint(job_id)
        stop_end = time.perf_counter()
        if not savepoint_path: return False

        # 2. 動態等待資源釋放 (優化點：取代原本的 sleep(5))
        wait_start = time.perf_counter()
        print("⏳ 動態檢查資源釋放情況...")
        self.wait_for_job_termination(job_id)
        wait_end = time.perf_counter()

        # ── [FIX-9] 確認所有 TM Slot 實際釋放後再重啟 ──────────────────────
        # wait_for_job_termination 只確認 Job 狀態機到達終態，
        # 但 TM Slot 釋放是非同步的，弱節點（單核 TM）在高負載後
        # 可能因 JVM GC 導致 Slot 回報延遲，被 JobManager 跳過。
        _parallelism = self.job_config.get("parallelism", 4)
        slots_ok = self._wait_for_slots_freed(
            expected_slots=_parallelism,
            timeout=30
        )
        if not slots_ok:
            print("⚠️ [FIX-9] Slot 釋放等待超時，仍繼續（可能有部分 TM 未就緒）")

        all_tms_ok = self._wait_for_all_tms_have_free_slot(timeout=20)
        if not all_tms_ok:
            print("⚠️ [FIX-9] 有 TM 未回報 freeSlot，仍繼續（有 Subtask 落點失敗風險）")
        # ── [FIX-9] END ─────────────────────────────────────────────────────

        # 3. 重新啟動 Job
        restart_start = time.perf_counter()
        new_job_id = None
        if auto_restart:
            new_job_id = self.submit_job_from_savepoint(savepoint_path)
        restart_end = time.perf_counter()

        # --- 遷移計時結束 ---
        end_migration = time.perf_counter()

        # 計算各階段耗時 (秒)
        savepoint_latency = stop_end - stop_start
        wait_latency = wait_end - wait_start
        restart_latency = restart_end - restart_start
        total_downtime = end_migration - start_migration

        # === 紀錄至 CSV (用於實驗分析) ===
        log_file = "/home/yenwei/research/structure_setup/output/caom_migration_performance.csv"
        file_exists = os.path.isfile(log_file)
        with open(log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["event_timestamp", "total_downtime", "savepoint_time", "resource_wait_time", "restart_time", "job_id"])
            writer.writerow([migration_event_timestamp, total_downtime, savepoint_latency, wait_latency, restart_latency, job_id])

        print("\n" + "="*40)
        print(f"📊 CAOM 遷移分析 (中斷時間: {total_downtime:.3f}s)")
        print(f"🔹 Savepoint: {savepoint_latency:.3f}s | Wait: {wait_latency:.3f}s | Restart: {restart_latency:.3f}s")
        print("="*40)

        # ── 寫入整合遷移記錄 ──
        try:
            # state_size 已在 detect_bottleneck() 建立 _task_info 時存入，
            # 直接從 _task_info 讀取，不再於 savepoint 後重新呼叫 API（此時 checkpoint 已不可查）
            bottleneck_cause_map = {}
            for subtask_id, actual_rate, max_cap in self._bottleneck_subtasks:
                overload_pct = ((actual_rate - max_cap) / max_cap * 100) if max_cap > 0 else 0
                bottleneck_cause_map[subtask_id] = f"CAOM_OVERLOAD actual={actual_rate:.1f} cap={max_cap:.1f} ({overload_pct:.1f}%)"
            with open(self.migration_record_path, "a") as rec:
                rec.write("\n" + "=" * 70 + "\n")
                rec.write(f"遷移時間: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(migration_event_timestamp))}\n")
                rec.write(f"Job ID (遷移前): {job_id}\n")
                rec.write(f"Job ID (遷移後): {new_job_id or 'N/A'}\n")
                rec.write("-" * 70 + "\n")
                rec.write(f"中斷時間總計: {total_downtime:.3f}s\n")
                rec.write(f"  Savepoint : {savepoint_latency:.3f}s\n")
                rec.write(f"  Wait      : {wait_latency:.3f}s\n")
                rec.write(f"  Restart   : {restart_latency:.3f}s\n")
                rec.write("-" * 70 + "\n")
                rec.write("Subtask 遷移詳情:\n")
                rec.write(f"  {'Subtask ID':<55} {'Before':<12} {'After':<12} {'State Size':>12} {'Bottleneck Cause'}\n")
                rec.write(f"  {'-'*55} {'-'*12} {'-'*12} {'-'*12} {'-'*30}\n")
                for sid, target_tm in migration_plan.items():
                    from_tm = current_locs.get(sid, "unknown")
                    to_tm   = target_tm
                    cause   = bottleneck_cause_map.get(sid, "N/A")
                    if from_tm != target_tm:
                        state_bytes = self._task_info.get(sid, {}).get("state_size", 0)
                        state_str = format_bytes(state_bytes) if state_bytes else "N/A"
                        rec.write(f"  {sid:<55} {from_tm:<12} {to_tm:<12} {state_str:>12} {cause}\n")
                rec.write("=" * 70 + "\n")
            print(f"📝 遷移記錄已寫入: {self.migration_record_path}")
        except Exception as e:
            print(f"⚠️ 寫入遷移記錄失敗: {e}")

        if new_job_id:
            self.last_migration_time = current_time
            return True
        return False

    def auto_detect_and_migrate(self, busy_threshold=None, skew_threshold=None,
                                monitor_interval_sec=5):
        """
        CAOM 完整自動偵測與遷移流程（含 Section III-C 最佳時間選擇）。

        執行順序：
          1. 若已有排程中的遷移計畫（_migration_scheduled=True），
             重新評估是否已到達最佳時間窗口，到則執行，否則繼續等待。
          2. 否則執行正常偵測（detect_bottleneck）。
          3. 偵測到瓶頸後，評估最佳遷移起始時間：
             - 歷史資料充足：計算立即遷移成本 vs 最佳時間成本
               * 改善幅度 > IMPROVEMENT_THRESHOLD 且 best_sec > 0：排程延遲遷移
               * 否則：立即遷移
             - 歷史資料不足（冷啟動）：立即遷移

        參數:
          monitor_interval_sec : 主迴圈每輪的監控間隔（秒），決定歷史資料取樣粒度。
                                 搜尋步長固定為 5 秒，與此值無關。
        """
        IMPROVEMENT_THRESHOLD = 0.05   # 最低改善門檻：5%，低於此不值得延遲等待
        MIN_HISTORY_FOR_PREDICT = 10   # 至少需要這麼多筆歷史才啟動時間選擇

        # ── 階段 1: 若已有排程中的計畫，檢查是否到達最佳時機 ─────────────────
        if self._migration_scheduled and self._pending_migration_plan:
            elapsed = time.time() - self._migration_ready_time
            print(f"\n⏳ [CAOM-Timing] 已在排程等待中 ({elapsed:.0f}s / "
                  f"max {self._migration_wait_timeout}s)")

            # 重新估算目前最佳時間（每輪更新預測）
            migration_duration_sec = self._estimate_migration_duration()
            best_sec, best_cost, immediate_cost = self._find_optimal_migration_start(
                migration_duration_sec, monitor_interval_sec
            )

            # 條件：best_sec == 0（現在是最佳時機）或已超過最長等待時間
            if best_sec == 0 or elapsed >= self._migration_wait_timeout:
                reason = ("到達最佳低負載時間窗口"
                          if best_sec == 0
                          else f"等待超時 ({elapsed:.0f}s ≥ {self._migration_wait_timeout}s)，強制執行")
                print(f"🚀 [CAOM-Timing] {reason}，開始執行遷移！")
                self._log_timing_decision(best_sec, best_cost, immediate_cost,
                                          0.0, f"EXECUTE({reason})", monitor_interval_sec)
                self._migration_scheduled    = False
                plan = self._pending_migration_plan
                self._pending_migration_plan = None
                return self.trigger_migration(plan)
            else:
                print(f"   繼續等待，預計再等 {best_sec}s "
                      f"(best_cost={best_cost:.1f} vs immediate={immediate_cost:.1f})")
                return False

        # ── 階段 2: 正常偵測流程 ──────────────────────────────────────────────
        reports = self.detect_bottleneck()

        if not reports:
            print("⚠️ 無法獲取監控數據")
            return False

        if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
            print("✅ 未檢測到瓶頸，系統運行正常")
            # 瓶頸消失：取消任何排程中的遷移計畫（狀態重置）
            if self._migration_scheduled:
                print("ℹ️  [CAOM-Timing] 瓶頸已消失，取消排程中的遷移計畫")
                self._migration_scheduled    = False
                self._pending_migration_plan = None
            return False

        print("\n" + "=" * 100)
        print("CAOM 檢查叢集狀態...")
        self.print_subtask_status()

        print(f"\n🔥 CAOM 檢測到 {len(self._bottleneck_subtasks)} 個瓶頸 Subtask:")
        for subtask_id, actual_rate, max_capacity in self._bottleneck_subtasks:
            overload_pct = ((actual_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0
            print(f"   - {subtask_id}: 實際輸入 {actual_rate:.2f} rec/s > "
                  f"最大容量 {max_capacity:.2f} rec/s (過載 {overload_pct:.1f}%)")

        # ── 產生遷移計畫 ──────────────────────────────────────────────────────
        migration_plan = self.generate_migration_plan()
        if not migration_plan:
            return False
        if not self.write_migration_plan(migration_plan):
            return False

        # ── 記錄決策過程至 migration_details CSV ─────────────────────────────
        detail_exists     = os.path.isfile(self.detail_log)
        event_time        = time.time()
        current_locations = self.get_subtask_locations()

        bytes_out_map = self.query_metric_by_task(
            'flink_taskmanager_job_task_numBytesOutPerSecond'
        )
        migration_candidates = []
        for subtask_id, actual_rate, max_cap in self._bottleneck_subtasks:
            info = self._task_info.get(subtask_id, {})
            task_name = info.get('task_name', '')
            idx = info.get('subtask_index', 0)
            net_demand_bytes = bytes_out_map.get(task_name, {}).get(idx, 0.0)
            migration_candidates.append({
                "subtask_id":  subtask_id,
                "actual_rate": actual_rate,
                "cpu_demand":  info.get("T_busy", 0) * 1000,
                "net_demand":  net_demand_bytes,
            })
        max_cpu = max((c["cpu_demand"] for c in migration_candidates), default=1.0) or 1.0
        max_net = max((c["net_demand"] for c in migration_candidates), default=1.0) or 1.0
        NEPTUNE_N = 3
        for c in migration_candidates:
            cpu_norm = c["cpu_demand"] / max_cpu
            net_norm = c["net_demand"] / max_net
            c["interference_score"] = pow(cpu_norm + net_norm, NEPTUNE_N)
        migration_candidates.sort(key=lambda x: x["interference_score"], reverse=True)

        with open(self.detail_log, "a", newline="") as f:
            writer = csv.writer(f)
            if not detail_exists:
                writer.writerow(["timestamp", "decision_step", "subtask_id",
                                 "priority_rank", "from_tm", "to_tm", "decision_reason"])
            for subtask_id, actual_rate, max_cap in self._bottleneck_subtasks:
                overload_pct = ((actual_rate - max_cap) / max_cap * 100) if max_cap > 0 else 0
                writer.writerow([event_time, "Diagnosis", subtask_id, "",
                                 current_locations.get(subtask_id, "unknown"), "",
                                 f"CAOM_BOTTLENECK: actual={actual_rate:.1f} > cap={max_cap:.1f} ({overload_pct:.1f}% overload)"])
            for rank, c in enumerate(migration_candidates, start=1):
                writer.writerow([event_time, "Prioritization", c["subtask_id"], rank,
                                 current_locations.get(c["subtask_id"], "unknown"), "",
                                 f"Neptune_score={c['interference_score']:.4f} (Rank {rank}), net_demand_bytes={c['net_demand']:.1f}"])
            for subtask_id, target_tm in migration_plan.items():
                original_tm = current_locations.get(subtask_id, "unknown")
                if target_tm != original_tm:
                    writer.writerow([event_time, "Assignment", subtask_id, "",
                                     original_tm, target_tm,
                                     f"{target_tm} selected: WASP+Amnis argmin(C_v/O_v)"])

        # ── 階段 3: CAOM Section III-C 最佳遷移時間評估 ───────────────────────
        print(f"\n{'='*60}")
        print(f"📊 [CAOM Section III-C] 最佳遷移時間評估")
        print(f"{'='*60}")

        n_history = len(self._source_rate_history)
        if n_history < MIN_HISTORY_FOR_PREDICT:
            # 歷史資料不足（冷啟動）：立即遷移
            print(f"   歷史資料不足 ({n_history}/{MIN_HISTORY_FOR_PREDICT} 筆)，"
                  f"執行立即遷移（冷啟動）")
            self._log_timing_decision(0, 0, 0, 0.0,
                                      "IMMEDIATE_COLDSTART", monitor_interval_sec)
            return self.trigger_migration(migration_plan)

        migration_duration_sec = self._estimate_migration_duration()
        best_sec, best_cost, immediate_cost = self._find_optimal_migration_start(
            migration_duration_sec, monitor_interval_sec
        )

        # 改善幅度（相對於立即遷移）
        if immediate_cost > 0:
            improvement_ratio = (immediate_cost - best_cost) / immediate_cost
        else:
            improvement_ratio = 0.0

        print(f"\n{'─'*60}")
        print(f"   歷史資料筆數  : {n_history} 筆（取樣間隔 {monitor_interval_sec}s）")
        print(f"   遷移預計耗時  : {migration_duration_sec:.1f}s")
        print(f"   立即遷移成本  : {immediate_cost:.1f} records")
        print(f"   最佳時間成本  : {best_cost:.1f} records (+{best_sec}s 後)")
        print(f"   預期改善幅度  : {improvement_ratio * 100:.1f}% "
              f"(門檻: {IMPROVEMENT_THRESHOLD * 100:.0f}%)")
        print(f"{'─'*60}")

        if best_sec > 0 and improvement_ratio > IMPROVEMENT_THRESHOLD:
            # 延遲遷移：排程等待最佳時機
            print(f"⏸️  [CAOM-Timing] 排程延遲遷移，等待 {best_sec}s 後的低負載窗口")
            print(f"   預期節省: {immediate_cost - best_cost:.1f} records "
                  f"({improvement_ratio * 100:.1f}%)")
            self._log_timing_decision(best_sec, best_cost, immediate_cost,
                                      improvement_ratio, "SCHEDULED_DELAY", monitor_interval_sec)
            self._pending_migration_plan = migration_plan
            self._migration_scheduled    = True
            self._migration_ready_time   = time.time()
            return False   # 本輪不執行遷移，等下一輪重入

        else:
            # 立即遷移：現在是最佳或改善幅度不顯著
            reason = ("改善幅度不足" if improvement_ratio <= IMPROVEMENT_THRESHOLD
                      else "現在即為最佳時機")
            print(f"🚀 [CAOM-Timing] {reason}，立即執行遷移")
            self._log_timing_decision(best_sec, best_cost, immediate_cost,
                                      improvement_ratio, f"IMMEDIATE({reason})", monitor_interval_sec)
            return self.trigger_migration(migration_plan)