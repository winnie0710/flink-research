import requests
import statistics
import csv
import json
import time
import os
import subprocess
import argparse

"""
propose_v8.py — 基於 propose_v7.py 完整重寫瓶頸偵測核心

核心改變：
  1. 完全移除 BFS actual_input_rate 反推（v4/v7 的 Step A / Step B）
     根本原因：高 bp 時 observed × (1 + T_bp/T_busy) 嚴重失真，
     導致 total_actual > total_max 誤判下游算子（如 Window_Auction_Count）
     為瓶頸，實際上瓶頸在上游的 TM 網路飽和。

  2. 兩層偵測永遠並行執行，結果獨立累積後 merge
     不再有「找到顯性就跳過隱性」的邏輯，避免連續遷移。

  3. 所有判定只用直接可觀測指標
     busy / bp / idle / obs_in / obs_out / outPool / inPool / TM 流量
     無固定閾值；所有數字邊界來自 IQR Tukey fence 或有明確文獻依據。

瓶頸原因代碼：
  NETWORK_TM_SATURATED  → 1a：TM 實體頻寬已達 IQR fence 閾值
  NETWORK_BOTTLENECK    → 1b：outPool 高且下游 inPool 低，自身是窄點
  CPU_BOTTLENECK        → 1c：Z_busy 統計異常且 T_busy > T_bp
  LATENT_FLOW           → 2a：相鄰算子流量守恆異常，下游積壓
  LATENT_CPU            → 2b：u_eff IQR straggler，有效利用率顯著超出同儕
  LATENT_SKEW           → 2c：obs_in 顯著低於同儕但 busy 高，資料傾斜或資源不足
  BACKPRESSURE_VICTIM   → 非瓶頸，受上游反壓（不進入遷移清單）
"""

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
    import re

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


def _quantile_linear(sorted_data, p):
    """
    線性插值百分位數（0 <= p <= 1），與 R/numpy 預設 linear 方法相容。
    """
    n = len(sorted_data)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_data[0]
    idx = p * (n - 1)
    lo = int(idx)
    hi = lo + 1
    if hi >= n:
        return sorted_data[lo]
    frac = idx - lo
    return sorted_data[lo] + frac * (sorted_data[hi] - sorted_data[lo])


class FlinkPropose:

    def __init__(self, query_type="q7", output_id="t16",
                 prometheus_url="http://localhost:9090",
                 flink_rest_url="http://localhost:8081",
                 migration_plan_path="/home/yenwei/research/structure_setup/plan/migration_plan.json",
                 savepoint_dir="file:///opt/flink/savepoints",
                 job_config=None):
        self.base_url = prometheus_url
        self.flink_rest_url = flink_rest_url
        self.migration_plan_path = migration_plan_path
        self.savepoint_dir = savepoint_dir
        self.last_migration_time = 0
        self.migration_cooldown = 300
        self._bottleneck_subtasks = []
        self._task_info = {}
        self._ordered_operators = []

        cfg = JOB_CONFIG[query_type]
        self.query_type = query_type
        self.job_name = cfg["job_name"]
        self.target_order = cfg["target_order"]

        output_dir = f"/home/yenwei/research/structure_setup/output/{output_id}/"
        os.makedirs(output_dir, exist_ok=True)
        self.log_file = os.path.join(output_dir, f"metrics_{query_type}.csv")
        self.detail_log = os.path.join(output_dir, f"migration_details_{query_type}.csv")

        print(f"[FlinkPropose v8] Monitoring job : {self.job_name}")
        print(f"[FlinkPropose v8] Metrics CSV    : {self.log_file}")
        print(f"[FlinkPropose v8] Details CSV    : {self.detail_log}")

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
        self.tm_bandwidth_map = {
            "tm_20c_1": int(37.5 * 1024 * 1024),
            "tm_20c_2": int(18.75 * 1024 * 1024),
            "tm_20c_3": int(18.75 * 1024 * 1024),
            "tm_20c_4": int(37.5 * 1024 * 1024),
            "tm_20c_5": int(37.5 * 1024 * 1024),
        }
        self.default_bandwidth = int(37.5 * 1024 * 1024)

    # ══════════════════════════════════════════════════════════════════════════
    # Prometheus / Flink REST 查詢工具
    # ══════════════════════════════════════════════════════════════════════════

    def query_metric_by_task(self, query):
        try:
            response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query})
            data = response.json()
            if data['status'] != 'success':
                print(f"❌ 查詢失敗: {data.get('error')}")
                return {}
            results = data['data']['result']
            task_map = {}
            for r in results:
                task_name = r['metric'].get('task_name', 'Unknown')
                idx = int(r['metric'].get('subtask_index', -1))
                val = float(r['value'][1])
                if idx != -1:
                    task_map.setdefault(task_name, {})[idx] = val
            return task_map
        except Exception as e:
            print(f"⚠️ 連線錯誤: {e}")
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
            if 'latest' in data and 'completed' in data['latest']:
                checkpoint_id = data['latest']['completed'].get('id')
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
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/query",
                params={'query': 'flink_taskmanager_job_task_busyTimeMsPerSecond'}
            )
            data = response.json()
            if data['status'] != 'success':
                return {}
            subtask_locations = {}
            for r in data['data']['result']:
                task_name = r['metric'].get('task_name', 'Unknown')
                subtask_index = r['metric'].get('subtask_index', '-1')
                resource_id = r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')
                subtask_locations[f"{task_name}_{subtask_index}"] = resource_id
            return subtask_locations
        except Exception as e:
            print(f"⚠️ 獲取 Subtask 位置失敗: {e}")
            return {}

    # ══════════════════════════════════════════════════════════════════════════
    # detect_bottleneck — v8 主入口
    # ══════════════════════════════════════════════════════════════════════════

    def detect_bottleneck(self):
        """
        兩層並行瓶頸偵測主入口。
        回傳 report_list（格式與 v7 相同供 main loop 使用）。
        副作用：更新 self._bottleneck_subtasks / self._task_info / self._ordered_operators
        """
        # Phase 0：指標收集與前處理
        task_info, operator_groups, ordered_operators, subtask_locations = \
            self._collect_metrics_and_build_topology()

        if not task_info:
            return []

        self._ordered_operators = ordered_operators
        self._task_info = task_info

        # Phase 1：TM 網路狀態（兩層共用）
        tm_out_saturated, saturation_rates = self._compute_tm_network_saturation(
            task_info, subtask_locations
        )

        # Phase 1：兩層並行，永遠都跑
        print("\n" + "=" * 100)
        print("STEP 1: 顯性瓶頸偵測（不依賴 bp 反推）")
        print("=" * 100)
        explicit_set = self._detect_explicit(
            task_info, operator_groups, ordered_operators, subtask_locations, tm_out_saturated
        )

        print("\n" + "=" * 100)
        print("STEP 2: 隱性瓶頸偵測（穩態流量 + u_eff straggler + 相對吞吐量）")
        print("=" * 100)
        implicit_set = self._detect_implicit(
            task_info, operator_groups, ordered_operators
        )

        # Phase 2：合併去重
        all_bottlenecks = self._merge_and_deduplicate(task_info, explicit_set, implicit_set)

        # Phase 3：回寫 task_info、建 report、寫 log
        # 把 saturation_rate 回填給 task_info，供 get_prioritized_list 的 d_overload 使用
        for sid, info in task_info.items():
            tm_id = subtask_locations.get(sid, "unknown")
            info["_tm_sat_rate"] = saturation_rates.get(tm_id, 0.0)

        report_list = self._build_report(
            task_info, operator_groups, ordered_operators, all_bottlenecks
        )

        self._bottleneck_subtasks = all_bottlenecks
        self._write_log(task_info, subtask_locations)

        return report_list

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 0：指標收集與拓撲建立
    # ──────────────────────────────────────────────────────────────────────────

    def _collect_metrics_and_build_topology(self):
        """
        向 Prometheus 抓取所有指標，組裝 task_info，建立算子拓撲順序。
        回傳：(task_info, operator_groups, ordered_operators, subtask_locations)
        """
        busy_data_map = self.query_metric_by_task('flink_taskmanager_job_task_busyTimeMsPerSecond')
        bp_data_map   = self.query_metric_by_task('flink_taskmanager_job_task_backPressuredTimeMsPerSecond')
        idle_data_map = self.query_metric_by_task('flink_taskmanager_job_task_idleTimeMsPerSecond')
        rate_in_map   = self.query_metric_by_task('flink_taskmanager_job_task_numBytesInPerSecond')
        rate_out_map  = self.query_metric_by_task('flink_taskmanager_job_task_numBytesOutPerSecond')
        out_pool_map  = self.query_metric_by_task(
            'max_over_time(flink_taskmanager_job_task_buffers_outPoolUsage[15s])'
        )
        in_pool_map   = self.query_metric_by_task(
            'max_over_time(flink_taskmanager_job_task_buffers_inPoolUsage[15s])'
        )
        state_size_map    = self.get_subtask_state_sizes()
        subtask_locations = self.get_subtask_locations()

        if not busy_data_map:
            return {}, {}, [], {}

        task_info = {}
        for task_name, subtasks_busy in busy_data_map.items():
            subtasks_bp   = bp_data_map.get(task_name, {})
            subtasks_idle = idle_data_map.get(task_name, {})

            for idx in subtasks_busy:
                T_busy = subtasks_busy.get(idx, 0) / 1000.0
                T_bp   = subtasks_bp.get(idx, 0)   / 1000.0
                T_idle = subtasks_idle.get(idx, 0)  / 1000.0
                obs_in  = rate_in_map.get(task_name, {}).get(idx, 0.0)
                obs_out = rate_out_map.get(task_name, {}).get(idx, 0.0)
                out_pool = out_pool_map.get(task_name, {}).get(idx, 0.0)
                in_pool  = in_pool_map.get(task_name, {}).get(idx, 0.0)

                # 狀態大小（用於遷移代價計算）
                state_size = 0
                for vertex_name, vertex_states in state_size_map.items():
                    if match_vertex_to_task_name(vertex_name, task_name):
                        state_size = vertex_states.get(idx, 0)
                        break
                if state_size == 0:
                    state_size = state_size_map.get(task_name, {}).get(idx, 0)

                # max_capacity 只用直接觀測值，供 evaluate_migration_trigger 使用
                # 公式：若有 T_busy 時間，理論上可以把 bp + idle 的時間也用來處理
                T_busy_safe  = max(T_busy, 0.001)
                primary_obs  = obs_out if "source" in task_name.lower() else obs_in
                max_capacity = ((1.0 + (T_bp + T_idle) / T_busy_safe) * primary_obs
                                if T_busy_safe > 0 and primary_obs > 0 else primary_obs)

                subtask_id = f"{task_name}_{idx}"
                task_info[subtask_id] = {
                    "task_name":            task_name,
                    "subtask_index":        idx,
                    "T_busy":               T_busy,
                    "T_bp":                 T_bp,
                    "T_idle":               T_idle,
                    "observed_input_rate":  obs_in,
                    "observed_output_rate": obs_out,
                    # observed_rate 保留供 generate_migration_plan 流量估算用
                    "observed_rate":        obs_out if "source" in task_name.lower() else obs_in,
                    "out_pool_usage":       out_pool,
                    "in_pool_usage":        in_pool,
                    "max_capacity":         max_capacity,
                    "state_size":           state_size,
                    "is_bottleneck":        False,
                    "bottleneck_cause":     None,
                    "also_implicit":        False,
                    "_tm_sat_rate":         0.0,
                }

        # 按 target_order 關鍵字排序算子
        operator_groups = {}
        for sid, info in task_info.items():
            operator_groups.setdefault(info["task_name"], []).append(sid)

        ordered_operators = []
        for keyword in self.target_order:
            for op in operator_groups:
                if keyword.lower() in op.lower() and op not in ordered_operators:
                    ordered_operators.append(op)
        for op in operator_groups:
            if op not in ordered_operators:
                ordered_operators.append(op)

        return task_info, operator_groups, ordered_operators, subtask_locations

    # ──────────────────────────────────────────────────────────────────────────
    # TM 網路飽和計算（兩層共用）
    # ──────────────────────────────────────────────────────────────────────────

    def _compute_tm_network_saturation(self, task_info, subtask_locations):
        """
        計算各 TM 出口流量飽和率，並以 IQR Tukey fence 動態決定飽和閾值。

        閾值設計（準則一 - 來源可解釋）：
          IQR Tukey fence（Tukey 1977）識別叢集中異常高的飽和率。
          上界 0.95：物理近飽和前兆。
          下界 0.85：TCP 在 85% 飽和時因 ACK 延遲出現非線性吞吐量衰退
                     （Intel 網路效能白皮書建議保留 15% 緩衝）。

        回傳：
          tm_out_saturated : { tm_id: bool }
          saturation_rates : { tm_id: float }
        """
        tm_traffic = {}
        for sid, info in task_info.items():
            tm_id = subtask_locations.get(sid, "unknown")
            if tm_id == "unknown":
                continue
            tm_traffic.setdefault(tm_id, {"in": 0.0, "out": 0.0})
            tm_traffic[tm_id]["in"]  += info["observed_input_rate"]
            tm_traffic[tm_id]["out"] += info["observed_output_rate"]

        saturation_rates = {}
        for tm_id, traffic in tm_traffic.items():
            limit = self.tm_bandwidth_map.get(tm_id, self.default_bandwidth)
            saturation_rates[tm_id] = traffic["out"] / limit if limit > 0 else 0.0

        if len(saturation_rates) >= 2:
            vals = sorted(saturation_rates.values())
            q1 = _quantile_linear(vals, 0.25)
            q3 = _quantile_linear(vals, 0.75)
            iqr = q3 - q1
            sat_threshold = max(min(q3 + 1.5 * iqr, 0.95), 0.85) if iqr > 0 else 0.90
        else:
            sat_threshold = 0.90

        tm_out_saturated = {}
        print(f"\n📊 [TM 網路狀態] 飽和閾值={sat_threshold:.2%}:")
        for tm_id, rate in saturation_rates.items():
            limit = self.tm_bandwidth_map.get(tm_id, self.default_bandwidth)
            tm_out_saturated[tm_id] = (rate >= sat_threshold)
            icon = "🔴" if tm_out_saturated[tm_id] else "🟢"
            print(f"   {tm_id}: {tm_traffic[tm_id]['out']/1e6:.2f}MB/s {icon} "
                  f"(Limit: {limit/1e6:.1f}MB/s, Sat={rate:.2%})")

        return tm_out_saturated, saturation_rates

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 1a/b/c：顯性瓶頸偵測
    # ──────────────────────────────────────────────────────────────────────────

    def _detect_explicit(self, task_info, operator_groups, ordered_operators, subtask_locations, tm_out_saturated):
        """
        顯性瓶頸偵測：三個子判斷均不依賴 bp 反推。

        1a. TM 網路飽和 → NETWORK_TM_SATURATED
            「subtask 所在 TM 出口流量超過 IQR fence 閾值，實體頻寬成為硬限制」

        1b. outPool 因果鏈 → NETWORK_BOTTLENECK（排除受害者）
            「outPool ≥ 0.7 且下游 inPool 中位數 ≤ 0.5，代表下游消化正常，
              自身輸出側是窄點；若下游 inPool > 0.5 則為受害者，排除」

        1c. Z-score busy → CPU_BOTTLENECK（排除受害者）
            「T_busy 在跨全局 subtask 分佈中超過 1-sigma（前 ~16% 最忙），
              且 T_busy > T_bp（忙碌非因等待下游），代表 CPU 是瓶頸根源」

        回傳：[(subtask_id, cause, d_overload), ...]
        """
        # 1c 前置：全局 Z-score 統計量
        all_busy = [info["T_busy"] for info in task_info.values()]
        mu_busy  = sum(all_busy) / len(all_busy) if all_busy else 0.0

        def _std(vals, mean_val):
            return (sum((x - mean_val) ** 2 for x in vals) / len(vals)) ** 0.5 if len(vals) >= 2 else 0.0

        sigma_busy  = _std(all_busy, mu_busy)
        SIGMA_MIN   = 0.001   # σ 過小代表負載均勻，Z-score 無鑑別力
        Z_THRESHOLD = 1.0     # 1-sigma：前 ~16% 最忙碌，統計學標準

        print(f"\n  [1c Z-score 統計量] μ_busy={mu_busy*1000:.1f}ms  σ_busy={sigma_busy*1000:.1f}ms")
        if sigma_busy < SIGMA_MIN:
            print("  ⚠️  σ_busy 過小（負載均勻），1c Z-score 判斷停用")

        # 1b 前置：建立 operator 下游對照表
        # 必須用 ordered_operators（已按 target_order 正確排序），
        # 不能用 operator_groups.keys()，其插入順序取決於 Prometheus 回傳順序，
        # 不穩定，導致 downstream_of 可能對應到錯誤的下游算子（如 HOP→Sink 而非 HOP→WAC）。
        downstream_of = {ordered_operators[i]: ordered_operators[i + 1]
                         for i in range(len(ordered_operators) - 1)}

        explicit_set = []

        for sid, info in task_info.items():
            T_busy   = info["T_busy"]
            T_bp     = info["T_bp"]
            out_pool = info["out_pool_usage"]
            tm_id    = subtask_locations.get(sid, "unknown")
            op_name  = info["task_name"]

            # ── 1a：TM 層級網路飽和 ─────────────────────────────────────────
            if tm_out_saturated.get(tm_id, False):
                explicit_set.append((sid, "NETWORK_TM_SATURATED",
                                     info.get("_tm_sat_rate", 1.0)))
                print(f"  [1a NET_TM] {sid}: TM {tm_id} 飽和")
                continue

            # ── 1b：outPool 因果鏈 ───────────────────────────────────────────
            # outPool ≥ 0.7：輸出緩衝進入高水位。
            # 0.7 的依據：TCP 流控在 buffer 70% 時出現 ACK 延遲（RFC 3517 接收窗口機制）。
            if out_pool >= 0.7:
                downstream_op = downstream_of.get(op_name)
                if downstream_op:
                    ds_subtasks = operator_groups.get(downstream_op, [])
                    ds_infos = [task_info[dst] for dst in ds_subtasks if dst in task_info]

                    if ds_infos:
                        # ── 受害者判定（同時滿足以下任一條件才視為受害者）────────
                        #
                        # 條件 A：下游 inPool 的最大值 > 0.5
                        #   意義：下游至少有一個 subtask 的輸入緩衝積壓，
                        #         代表下游整體消化不了，本 subtask 是受害者。
                        #   改用 max 而非 median，因為資料傾斜場景下部分 subtask
                        #   inPool=1.0（積壓）而其他 subtask inPool=0.0（空閒），
                        #   中位數被拉低導致誤判（實測案例：Window_Auction_Count
                        #   inPool=[1.0, 0.0, 0.0, 0.188]，中位數=0.094 但 max=1.0）。
                        #
                        # 條件 B：下游算子存在 CPU 滿載的 subtask（T_busy > 0.85）
                        #   意義：即使 inPool 中位數低，下游仍有 subtask 在滿載，
                        #         這解釋了為何整個算子吞吐量上不來，本 subtask 是受害者。
                        #   0.85 的依據：M/M/1 排隊理論在 ρ=0.85 時平均隊長 ρ/(1-ρ)≈5.7，
                        #                即將進入飽和積壓狀態。
                        #
                        # 條件 C：本 subtask 自身 T_bp 顯著高於 T_busy
                        #   意義：等待時間遠超處理時間，本 subtask 的資源明顯是充裕的，
                        #         阻塞來自外部（下游施壓），是典型受害者特徵。
                        ds_max_inpool  = max(d["in_pool_usage"]  for d in ds_infos)
                        ds_max_busy    = max(d["T_busy"]         for d in ds_infos)
                        is_victim = (
                                ds_max_inpool > 0.5        # 條件 A
                                or ds_max_busy > 0.85      # 條件 B
                                or T_bp > T_busy * 2.0     # 條件 C：反壓時間是處理時間的 2 倍以上
                        )

                        if is_victim:
                            info["bottleneck_cause"] = "BACKPRESSURE_VICTIM"
                            print(f"  [1b VICTIM] {sid}: outPool={out_pool:.2f}，"
                                  f"下游 max_inPool={ds_max_inpool:.2f}/"
                                  f"max_busy={ds_max_busy*1000:.0f}ms，"
                                  f"T_bp={T_bp*1000:.0f}ms/T_busy={T_busy*1000:.0f}ms，"
                                  f"為受害者")
                            continue
                        else:
                            explicit_set.append((sid, "NETWORK_BOTTLENECK", out_pool))
                            print(f"  [1b NETWORK] {sid}: outPool={out_pool:.2f}，"
                                  f"下游 max_inPool={ds_max_inpool:.2f}/"
                                  f"max_busy={ds_max_busy*1000:.0f}ms，自身是窄點")
                            continue
                # 無下游（Sink 前）或下游 inPool 查不到 → 自身問題
                explicit_set.append((sid, "NETWORK_BOTTLENECK", out_pool))
                print(f"  [1b NETWORK] {sid}: outPool={out_pool:.2f}（無下游，自身是窄點）")
                continue

            # ── 1c：Z-score busy 異常 ────────────────────────────────────────
            if sigma_busy >= SIGMA_MIN:
                Z_busy = (T_busy - mu_busy) / sigma_busy
                if Z_busy > Z_THRESHOLD and T_busy > T_bp:
                    free_time  = max(1.0 - T_bp, 0.001)
                    u_eff      = min(T_busy / free_time, 1.0)
                    d_overload = max(T_busy, u_eff)
                    explicit_set.append((sid, "CPU_BOTTLENECK", d_overload))
                    print(f"  [1c CPU] {sid}: Z_busy={Z_busy:.2f}>{Z_THRESHOLD}，"
                          f"T_busy={T_busy*1000:.0f}ms > T_bp={T_bp*1000:.0f}ms")

        print(f"\n  ✅ 顯性瓶頸共 {len(explicit_set)} 個")
        return explicit_set

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 2a/b/c：隱性瓶頸偵測
    # ──────────────────────────────────────────────────────────────────────────

    def _detect_implicit(self, task_info, operator_groups, ordered_operators):
        """
        隱性瓶頸偵測：三個子判斷均不依賴 bp 反推，永遠執行。

        2a. 穩態流量守恆差異 → LATENT_FLOW
            「相鄰算子間 flow_ratio = dn_obs_in / up_obs_out；
              有效 pair（up_out > 0）的 flow_ratio 低於 IQR 下界 fence
              代表下游積壓；Kafka Source obs_out=0 的 pair 自動跳過」

        2b. u_eff 同算子 Z-score straggler → LATENT_CPU
            「u_eff = T_busy/(1-T_bp) 排除反壓後的真實 CPU 壓力；
              改用同算子 Z-score（而非 IQR Tukey fence），
              因為 IQR 在雙峰分佈（如兩個 subtask busy=0.99、兩個 busy=0.45）
              時 IQR 橫跨整個分佈，fence 被推到 > 1.0 而永遠無法命中；
              Z_u > 0.9（約前 18% 異常值）且 T_busy > 0.3s 代表真實過載」

        2c. 處理效率 Z-score → LATENT_SKEW
            「efficiency = obs_in / T_busy（每單位 busy 時間的吞吐量）；
              Z_eff < -0.9 代表處理效率顯著低於同儕，
              可同時偵測資料傾斜（obs_in 高但 efficiency 低）
              和資源不足（obs_in 正常但 efficiency 低，即 T_busy 異常高）；
              改用 efficiency 而非 obs_in 直接比較，是因為 obs_in 均勻時
              60% 中位數門檻永遠不會觸發（如 WAC 場景）」

        Z_THRESHOLD_IMPLICIT = 0.9 的依據：
          與 1c 顯性偵測的 Z=1.0 區分（顯性門檻更嚴格），
          隱性偵測允許較低門檻以捕捉未達顯性條件的過載徵兆；
          0.9-sigma 對應約 81.6% 置信度，統計學上屬合理異常偵測範圍。

        回傳：[(subtask_id, cause, d_overload), ...]
        """
        # 隱性偵測專用 Z-score 門檻（低於顯性 1c 的 1.0，以捕捉較早期的過載信號）
        Z_THRESHOLD_IMPLICIT = 0.9
        SIGMA_MIN_U   = 0.05   # u_eff 的 sigma 過小代表算子整體很均勻，停用 Z-score
        SIGMA_MIN_EFF = 0.5    # efficiency 的 sigma 過小（單位 MB/s），停用 Z-score

        def _std(vals, mean_val):
            return (sum((x - mean_val) ** 2 for x in vals) / len(vals)) ** 0.5 \
                if len(vals) >= 2 else 0.0

        # ── 2a：穩態流量守恆差異 ─────────────────────────────────────────────
        # 跳過 up_obs_out = 0 的 pair（Kafka Source 不透過 network channel 輸出，
        # numBytesOut 永遠回報 0，強行計算 ratio 會得到 inf 而干擾 IQR）。
        flow_ratios = []
        for i in range(len(ordered_operators) - 1):
            up_op   = ordered_operators[i]
            down_op = ordered_operators[i + 1]
            up_out  = sum(task_info[st]["observed_output_rate"]
                          for st in operator_groups.get(up_op, []) if st in task_info)
            dn_in   = sum(task_info[st]["observed_input_rate"]
                          for st in operator_groups.get(down_op, []) if st in task_info)
            if up_out > 0:
                flow_ratios.append((down_op, dn_in / up_out))
            else:
                print(f"  [2a SKIP] {up_op} → {down_op}: up_obs_out=0，跳過（Source 無 network output）")

        # IQR 下界 fence（Tukey 1977）：低於此值代表下游積壓
        if len(flow_ratios) >= 2:
            ratio_vals = sorted(r for _, r in flow_ratios)
            q1_r = _quantile_linear(ratio_vals, 0.25)
            q3_r = _quantile_linear(ratio_vals, 0.75)
            iqr_r = q3_r - q1_r
            low_fence = q1_r - 1.5 * iqr_r if iqr_r > 0 else q1_r * 0.5
        else:
            low_fence = 0.0

        print(f"\n  [2a 流量守恆] 有效 pair 數={len(flow_ratios)}，low_fence={low_fence:.3f}")
        latent_flow_ops = set()
        for down_op, ratio in flow_ratios:
            print(f"    {down_op}: flow_ratio={ratio:.4f}")
            if ratio < low_fence:
                latent_flow_ops.add(down_op)
                print(f"  [2a LATENT_FLOW] operator '{down_op}': "
                      f"flow_ratio={ratio:.3f} < fence={low_fence:.3f}")

        # ── 2b：u_eff 同算子 Z-score straggler ───────────────────────────────
        # 改用 Z-score 而非 IQR Tukey fence，原因：
        #   IQR 在雙峰分佈（部分 subtask 接近 1.0、部分接近 0.45）時，
        #   IQR 橫跨整個值域（Q3-Q1 ≈ 0.55），fence = Q3 + 1.5×IQR ≈ 1.82，
        #   超過 u_eff 的物理上限 1.0，任何 subtask 都無法命中。
        #   Z-score 對雙峰分佈的區分能力更穩定。
        print(f"\n  [2b u_eff Z-score straggler]")
        latent_cpu_sids = set()

        for op_name, subtasks in operator_groups.items():
            if len(subtasks) < 2:
                continue
            u_eff_list = []
            for st in subtasks:
                t_busy = task_info[st]["T_busy"]
                t_bp   = task_info[st]["T_bp"]
                free   = max(1.0 - t_bp, 0.001)
                u_eff_list.append((st, min(t_busy / free, 1.0)))

            u_vals  = [u for _, u in u_eff_list]
            mu_u    = sum(u_vals) / len(u_vals)
            sigma_u = _std(u_vals, mu_u)

            for st, u_eff in u_eff_list:
                t_busy = task_info[st]["T_busy"]
                t_bp   = task_info[st]["T_bp"]
                Z_u    = (u_eff - mu_u) / sigma_u if sigma_u >= SIGMA_MIN_U else 0.0
                # 條件：Z_u 統計顯著 + T_busy 夠高（排除空閒 subtask 因分母小而 u_eff 高）
                # T_busy > 0.3 的依據：300ms/s 代表至少 30% 時間在處理，
                #   低於此值的 subtask 即使 u_eff 高也屬正常波動。
                if Z_u > Z_THRESHOLD_IMPLICIT and t_busy > 0.3:
                    latent_cpu_sids.add(st)
                    print(f"  [2b LATENT_CPU] {st}: u_eff={u_eff:.3f} "
                          f"Z_u={Z_u:.3f}>{Z_THRESHOLD_IMPLICIT}，"
                          f"T_busy={t_busy*1000:.0f}ms>300ms")

        # ── 2c：處理效率 Z-score → LATENT_SKEW ────────────────────────────────
        # efficiency = obs_in / T_busy（每秒 busy 時間能處理多少 bytes）
        # 物理意義：相同資料量下，efficiency 低的 subtask 需要更多 CPU 時間，
        #   代表其處理能力弱（資源不足）或需要處理額外負荷（資料傾斜造成更複雜的 key）。
        # 改用 efficiency 而非直接比較 obs_in，因為 obs_in 均勻分佈時
        #   obs_in < 60%×median 永遠不成立（如 WAC 場景各 subtask obs_in 相差 < 0.5%）。
        print(f"\n  [2c 處理效率 Z-score]")
        latent_skew_sids = set()

        for op_name, subtasks in operator_groups.items():
            if len(subtasks) < 2:
                continue
            eff_list = []
            for st in subtasks:
                t_busy = task_info[st]["T_busy"]
                obs_in = task_info[st]["observed_input_rate"]
                # T_busy 極小時跳過（Sink 等空閒算子，分母趨近 0 會爆炸）
                if t_busy < 0.01:
                    eff_list.append((st, None))
                    continue
                eff_list.append((st, obs_in / t_busy))

            valid = [(st, e) for st, e in eff_list if e is not None]
            if len(valid) < 2:
                continue

            eff_vals = [e for _, e in valid]
            mu_e    = sum(eff_vals) / len(eff_vals)
            sigma_e = _std(eff_vals, mu_e)

            for st, eff in valid:
                t_busy = task_info[st]["T_busy"]
                Z_e    = (eff - mu_e) / sigma_e if sigma_e >= SIGMA_MIN_EFF else 0.0
                # Z_e 負值且顯著 → efficiency 低於同儕 → 處理能力弱
                # T_busy > 0.3 同 2b，排除空閒 subtask
                if Z_e < -Z_THRESHOLD_IMPLICIT and t_busy > 0.3:
                    latent_skew_sids.add(st)
                    print(f"  [2c LATENT_SKEW] {st}: "
                          f"efficiency={eff:.2f} Z_eff={Z_e:.3f}<-{Z_THRESHOLD_IMPLICIT}，"
                          f"T_busy={t_busy*1000:.0f}ms>300ms")

        # ── 組裝 implicit_set ─────────────────────────────────────────────────
        # 優先順序：LATENT_CPU > LATENT_FLOW > LATENT_SKEW
        # 2b 和 2c 同時命中同一 subtask 取 LATENT_CPU（CPU 過載是更具體的原因）
        implicit_set = []
        for sid, info in task_info.items():
            op_name   = info["task_name"]
            t_bp      = info["T_bp"]
            t_busy    = info["T_busy"]
            free_time = max(1.0 - t_bp, 0.001)
            u_eff     = min(t_busy / free_time, 1.0)

            in_latent_flow = op_name in latent_flow_ops
            in_latent_cpu  = sid in latent_cpu_sids
            in_latent_skew = sid in latent_skew_sids

            if in_latent_cpu:
                # 2b 命中（無論是否也命中 2a/2c，原因取最具體的 CPU）
                implicit_set.append((sid, "LATENT_CPU", u_eff))
            elif in_latent_flow:
                implicit_set.append((sid, "LATENT_FLOW", u_eff))
            elif in_latent_skew:
                implicit_set.append((sid, "LATENT_SKEW", u_eff))

        print(f"\n  ✅ 隱性瓶頸共 {len(implicit_set)} 個")
        return implicit_set

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 2：合併去重
    # ──────────────────────────────────────────────────────────────────────────

    def _merge_and_deduplicate(self, task_info, explicit_set, implicit_set):
        """
        合併規則（同一 subtask_id 的衝突處理）：
          - 原因取 explicit（顯性有直接物理證據，更具體）
          - d_overload 取兩者較大值（保留最嚴重的過載程度）
          - 在 task_info 標記 also_implicit=True 供 log 用

        回傳：[(subtask_id, cause, d_overload), ...]
        """
        result = {}

        for sid, cause, d_overload in explicit_set:
            result[sid] = [sid, cause, d_overload]
            task_info[sid]["is_bottleneck"]    = True
            task_info[sid]["bottleneck_cause"] = cause

        for sid, cause, d_overload in implicit_set:
            if sid in result:
                result[sid][2] = max(result[sid][2], d_overload)
                task_info[sid]["also_implicit"] = True
            else:
                result[sid] = [sid, cause, d_overload]
                task_info[sid]["is_bottleneck"]    = True
                task_info[sid]["bottleneck_cause"] = cause

        merged = [(r[0], r[1], r[2]) for r in result.values()]

        print(f"\n🔥 合併後共 {len(merged)} 個瓶頸 subtask:")
        for sid, cause, d_overload in sorted(merged, key=lambda x: -x[2]):
            also = " (+隱性)" if task_info[sid].get("also_implicit") else ""
            print(f"   {cause:<25} {sid}  d_overload={d_overload:.3f}{also}")

        return merged

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 3：Report 建立 + Log 寫入
    # ──────────────────────────────────────────────────────────────────────────

    def _build_report(self, task_info, operator_groups, ordered_operators, all_bottlenecks):
        """
        依算子彙整 report_list，格式與 v7 相同。
        status 判定優先順序：BOTTLENECK+LATENT > BOTTLENECK > LATENT > HIGH_LOAD > BACKPRESSURED > NORMAL
        """
        bottleneck_sids = {sid for sid, _, _ in all_bottlenecks}

        report_list = []
        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]

            explicit_count = sum(
                1 for st in subtasks
                if st in bottleneck_sids and not task_info[st].get("bottleneck_cause", "").startswith("LATENT")
            )
            latent_count = sum(
                1 for st in subtasks
                if st in bottleneck_sids and task_info[st].get("bottleneck_cause", "").startswith("LATENT")
            )
            cpu_count = sum(
                1 for st in subtasks if task_info[st].get("bottleneck_cause") == "CPU_BOTTLENECK"
            )
            network_count = sum(
                1 for st in subtasks
                if task_info[st].get("bottleneck_cause") in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED")
            )
            max_busy = max(task_info[st]["T_busy"] * 1000 for st in subtasks)
            max_bp   = max(task_info[st]["T_bp"]   * 1000 for st in subtasks)

            if explicit_count > 0 and latent_count > 0:
                status = "🔴 BOTTLENECK+LATENT"
            elif explicit_count > 0:
                status = "🔴 BOTTLENECK"
            elif latent_count > 0:
                status = "🟠 LATENT_BOTTLENECK"
            elif max_busy > 700:
                status = "🟠 HIGH_LOAD"
            elif max_bp > 500:
                status = "🟡 BACKPRESSURED"
            else:
                status = "🟢 NORMAL"

            report_list.append({
                "task_name":               op_name,
                "status":                  status,
                "bottleneck_count":        explicit_count + latent_count,
                "cpu_bottleneck_count":    cpu_count,
                "network_bottleneck_count": network_count,
                "latent_bottleneck_count": latent_count,
                "max_busy":                round(max_busy, 1),
                "max_bp":                  round(max_bp, 1),
                "subtasks":                subtasks,
            })

        return report_list

    def _write_log(self, task_info, subtask_locations):
        """將本輪所有 subtask 狀態寫入 CSV log。"""
        _CAUSE_TO_STATUS = {
            "CPU_BOTTLENECK":       "CPU_Bottleneck",
            "NETWORK_BOTTLENECK":   "NET_Bottleneck",
            "NETWORK_TM_SATURATED": "NET_Bottleneck",
            "LATENT_FLOW":          "Latent_Bottleneck",
            "LATENT_CPU":           "Latent_Bottleneck",
            "LATENT_SKEW":          "Latent_Bottleneck",
            "BACKPRESSURE_VICTIM":  "Victim",
        }
        file_exists = os.path.isfile(self.log_file)
        curr_time   = time.time()
        with open(self.log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp", "subtask_id", "current_tm",
                    "status", "cause", "also_implicit"
                ])
            for sid, info in task_info.items():
                cause  = info.get("bottleneck_cause") or "NONE"
                status = _CAUSE_TO_STATUS.get(cause, "Healthy")
                writer.writerow([
                    curr_time,
                    sid,
                    subtask_locations.get(sid, "unknown"),
                    status,
                    cause,
                    info.get("also_implicit", False),
                ])

    # ══════════════════════════════════════════════════════════════════════════
    # 優先級排序
    # ══════════════════════════════════════════════════════════════════════════

    def calculate_topology_impact(self, subtask_id):
        ordered = getattr(self, "_ordered_operators", [])
        max_depth = len(ordered) - 1
        if max_depth <= 0:
            return 0.0
        task_name = self._task_info.get(subtask_id, {}).get("task_name", "")
        try:
            depth = ordered.index(task_name)
        except ValueError:
            return 0.0
        return depth / max_depth

    def get_prioritized_list(self, filtered_ids):
        """
        多維度優先級排序：Priority_Score = D_overload × (1 + R_impact)

        D_overload 依原因類型（全部使用直接觀測指標，不依賴 actual_input_rate）：
          NETWORK_TM_SATURATED → TM 飽和率（saturation_rates 回填的 _tm_sat_rate）
          NETWORK_BOTTLENECK   → out_pool_usage
          CPU_BOTTLENECK       → max(T_busy, u_eff)
          LATENT_*             → u_eff
        """
        if not filtered_ids:
            return []

        entries = []
        print(f"\n🎯 多維度優先級計算 (Priority = D_overload × (1 + R_impact)):")
        print(f"{'Subtask ID':<60} {'Cause':<25} {'D_overload':<12} {'R_impact':<10} {'Priority':<10}")
        print("=" * 103)

        for sid in filtered_ids:
            if sid not in self._task_info:
                continue
            info      = self._task_info[sid]
            cause     = info.get("bottleneck_cause", "CPU_BOTTLENECK")
            T_busy    = info["T_busy"]
            T_bp      = info["T_bp"]
            out_pool  = info["out_pool_usage"]
            free_time = max(1.0 - T_bp, 0.001)
            u_eff     = min(T_busy / free_time, 1.0)

            if cause == "NETWORK_TM_SATURATED":
                d_overload = info.get("_tm_sat_rate", 1.0)
            elif cause == "NETWORK_BOTTLENECK":
                d_overload = out_pool
            elif cause == "CPU_BOTTLENECK":
                d_overload = max(T_busy, u_eff)
            else:
                # LATENT_FLOW / LATENT_CPU / LATENT_SKEW
                d_overload = u_eff

            r_impact       = self.calculate_topology_impact(sid)
            priority_score = d_overload * (1.0 + r_impact)

            entries.append({
                "subtask_id": sid, "cause": cause,
                "d_overload": d_overload, "r_impact": r_impact,
                "priority_score": priority_score,
            })
            print(f"{sid:<60} {cause.replace('_BOTTLENECK',''):<25} "
                  f"{d_overload:>10.3f}  {r_impact:>8.2f}  {priority_score:>8.3f}")

        entries.sort(key=lambda x: x["priority_score"], reverse=True)
        prioritized_list = [(e["subtask_id"], e["priority_score"]) for e in entries]
        print(f"\n📊 優先級排序完成: {len(prioritized_list)} 個 Subtask")
        return prioritized_list

    # ══════════════════════════════════════════════════════════════════════════
    # 遷移觸發評估
    # ══════════════════════════════════════════════════════════════════════════

    def evaluate_migration_trigger(self, bottleneck_subtasks):
        """
        Job 級別全局評估：時間領域成本效益分析。
        比較「遷移停機時間 C_mig」與「不遷移累積延遲 C_stay」。

        U_safe 改為動態：以瓶頸 subtask 所屬算子同儕的 u_eff 中位數為健康基準，
        不再使用固定 0.80，符合準則一（無來源固定閾值）。
        """
        if not bottleneck_subtasks:
            return False, "無任何瓶頸，系統健康"

        max_stress_task = max(bottleneck_subtasks, key=lambda x: x[2])
        subtask_id, cause, u_eff = max_stress_task

        info = self._task_info.get(subtask_id, {})
        state_size_bytes = info.get("state_size", 0.0)
        state_size_mb    = state_size_bytes / (1024 * 1024)

        T_fixed      = 4.0    # Flink Savepoint/Restore 基礎停機時間（秒）
        BW_internal  = 150.0  # 叢集內部狀態傳輸頻寬假設（MB/s）
        C_mig = T_fixed + (state_size_mb / BW_internal)

        # 動態 U_safe：以同算子所有 subtask 的 u_eff 中位數作為健康基準
        task_name = info.get("task_name", "")
        peer_sids = [
            sid for sid, si in self._task_info.items()
            if si.get("task_name") == task_name
        ]
        if len(peer_sids) >= 2:
            peer_u_effs = []
            for ps in peer_sids:
                pi = self._task_info[ps]
                ft = max(1.0 - pi["T_bp"], 0.001)
                peer_u_effs.append(min(pi["T_busy"] / ft, 1.0))
            U_safe = statistics.median(peer_u_effs)
        else:
            U_safe = 0.5   # 無同儕時使用中點作為保守基準

        W_eval  = 30.0  # 預測窗口（秒），代表下一次偵測週期
        C_stay  = max(0, u_eff - U_safe) * W_eval

        print(f"\n  [TRIGGER 時間成本分析] 基準瓶頸: {subtask_id}")
        print(f"  - 停機代價 (C_mig) : {C_mig:.2f} 秒 (包含 {state_size_mb:.1f}MB 狀態傳輸)")
        print(f"  - U_safe (同儕中位數 u_eff): {U_safe:.3f}")
        print(f"  - 延遲代價 (C_stay): {C_stay:.2f} 秒 (u_eff={u_eff:.3f}, 窗口={W_eval}s)")

        if C_stay > C_mig:
            print(f"  ✅ [TRIGGER 成立] C_stay ({C_stay:.2f}s) > C_mig ({C_mig:.2f}s)，觸發遷移")
            return True, "C_stay > C_mig"
        else:
            print(f"  ❌ [TRIGGER 拒絕] C_mig ({C_mig:.2f}s) >= C_stay ({C_stay:.2f}s)，維持現狀")
            return False, "C_mig >= C_stay"

    # ══════════════════════════════════════════════════════════════════════════
    # 拓撲親和力 + 遷移計畫生成（完整保留 v7 邏輯）
    # ══════════════════════════════════════════════════════════════════════════

    def get_neighbors(self, subtask_id):
        task_name = "_".join(subtask_id.rsplit("_", 1)[0].split("_"))
        current_pos = -1
        for i, keyword in enumerate(self.target_order):
            if keyword.lower() in task_name.lower():
                current_pos = i
                break
        if current_pos == -1:
            return (set(), set())
        upstream_keywords   = {self.target_order[current_pos - 1]} if current_pos > 0 else set()
        downstream_keywords = {self.target_order[current_pos + 1]} if current_pos < len(self.target_order) - 1 else set()
        return (upstream_keywords, downstream_keywords)

    def calculate_topology_affinity(self, bottleneck_subtask_id, resource_map, current_locations, pending_locations=None):
        """
        拓撲感知資料引力親和性演算法（v7 邏輯完整保留）。
        Branch A：FORWARD 1-to-1 配對；Branch B：HASH M-to-N 比例分配。
        """
        FORWARD_STRATEGY_TASKS = {
            "Window_Join": "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer"
        }

        if bottleneck_subtask_id not in self._task_info:
            print(f"   ⚠️ [{bottleneck_subtask_id}] 不在 _task_info，無法計算親和力")
            return None, {}

        bottleneck_info = self._task_info[bottleneck_subtask_id]
        _, downstream_keywords = self.get_neighbors(bottleneck_subtask_id)
        if not downstream_keywords:
            return None, {}

        downstream_subtasks = [
            sid for sid, info in self._task_info.items()
            if any(kw in info["task_name"] for kw in downstream_keywords)
        ]
        if not downstream_subtasks:
            return None, {}

        bytes_out_sb = bottleneck_info.get("observed_output_rate", 0.0)
        bottleneck_task_name = bottleneck_info["task_name"]

        is_forward = False
        forward_downstream_kw = None
        for upstream_kw, downstream_kw in FORWARD_STRATEGY_TASKS.items():
            if (upstream_kw in bottleneck_task_name and
                    any(kw in downstream_kw for kw in downstream_keywords)):
                is_forward = True
                forward_downstream_kw = downstream_kw
                break

        edge_weights = {}

        # effective_locations：合併 pending_locations（本輪已分配的新位置）
        # 和 current_locations（實際當前位置），pending 優先。
        # 這確保親和力計算反映同一輪中其他 subtask 的最新分配決策。
        _pending = pending_locations or {}
        def _loc(sid):
            return _pending.get(sid, current_locations.get(sid, "unknown"))

        if is_forward:
            bottleneck_str_index = bottleneck_subtask_id.split('_')[-1]
            print(f"\n   📡 [{bottleneck_subtask_id}] Step 1 — Branch A (FORWARD 1-to-1) "
                  f"尋找下游 Index='{bottleneck_str_index}' 的配對 Subtask...")
            matched_forward_sid = None
            for d_sid in downstream_subtasks:
                d_info = self._task_info[d_sid]
                if (forward_downstream_kw in d_info["task_name"] and
                        d_sid.split('_')[-1] == bottleneck_str_index):
                    matched_forward_sid = d_sid
                    break
            if matched_forward_sid is None:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Branch A 找不到配對，退回 Branch B")
                is_forward = False
            else:
                edge_weights[matched_forward_sid] = bytes_out_sb
                d_tm = _loc(matched_forward_sid)
                print(f"      ✅ 配對成功: {matched_forward_sid} → TM: {d_tm} "
                      f"EdgeWeight={bytes_out_sb/1e6:.3f}MB/s (100%)")

        if not is_forward:
            # Branch B：用下游 obs_in 比例估算 edge weight（不再用 actual_input_rate）
            total_downstream_in = sum(
                self._task_info[d]["observed_input_rate"] for d in downstream_subtasks
            )
            if total_downstream_in <= 0:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Branch B — 下游 obs_in 總和為 0，退回")
                return None, {}
            for d_sid in downstream_subtasks:
                d_rate = self._task_info[d_sid]["observed_input_rate"]
                edge_weights[d_sid] = bytes_out_sb * (d_rate / total_downstream_in)
            print(f"\n   📡 [{bottleneck_subtask_id}] Step 1 — Branch B (HASH M-to-N) "
                  f"(下游obs_in總和={total_downstream_in/1e6:.2f}MB/s, "
                  f"S_b 輸出={bytes_out_sb/1e6:.2f}MB/s):")
            for d_sid, ew in edge_weights.items():
                d_tm = _loc(d_sid)
                print(f"      {d_sid:<40} → TM: {d_tm:<20} EdgeWeight={ew/1e6:.3f}MB/s")

        affinity_scores = {}
        for rid in resource_map:
            affinity_scores[rid] = sum(
                ew for d_sid, ew in edge_weights.items()
                if _loc(d_sid) == rid
            )

        print(f"   🎯 [{bottleneck_subtask_id}] Step 2 — 各 TM 親和力總分 (AffinityScore):")
        for rid, score in sorted(affinity_scores.items(), key=lambda x: -x[1]):
            print(f"      {rid:<25}: {score/1e6:.4f}MB/s")

        bytes_in_sb       = bottleneck_info.get("observed_input_rate", 0.0)
        b_total_traffic   = bytes_in_sb + bytes_out_sb
        b_current_tm_id   = _loc(bottleneck_subtask_id)

        print(f"   🔒 [{bottleneck_subtask_id}] Step 3 — 網路頻寬約束檢查 "
              f"(S_b 當前 TM={b_current_tm_id}, "
              f"in={bytes_in_sb/1e6:.2f}MB/s, out={bytes_out_sb/1e6:.2f}MB/s, "
              f"total={b_total_traffic/1e6:.2f}MB/s):")

        valid_tms = {}
        for rid, affinity in affinity_scores.items():
            if rid == b_current_tm_id:
                expected_network_added = 0.0
                case_label = "A (原地不動)"
            else:
                expected_network_added = b_total_traffic - (2.0 * affinity)
                case_label = "B (跨 TM 遷移)"
            current_net    = resource_map[rid].get("network_traffic", 0.0)
            bw_limit       = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
            projected_net  = current_net + expected_network_added
            double_saving  = 2.0 * affinity if rid != b_current_tm_id else 0.0
            if projected_net <= bw_limit:
                valid_tms[rid] = affinity
                print(f"      ✅ {rid:<25} [{case_label}]: "
                      f"增量={expected_network_added/1e6:+.3f}MB/s "
                      f"(total={b_total_traffic/1e6:.3f} - 2×score={double_saving/1e6:.3f}), "
                      f"預估={projected_net/1e6:.3f}/{bw_limit/1e6:.1f}MB/s → 通過")
            else:
                print(f"      ❌ {rid:<25} [{case_label}]: "
                      f"增量={expected_network_added/1e6:+.3f}MB/s, "
                      f"預估={projected_net/1e6:.3f}MB/s > 上限={bw_limit/1e6:.1f}MB/s → 剔除")

        bottleneck_cause = bottleneck_info.get("bottleneck_cause", "CPU_BOTTLENECK")

        def _find_max_remaining_bw_tm():
            best_rid, max_remaining = None, -float('inf')
            for rid, res in resource_map.items():
                remaining = self.tm_bandwidth_map.get(rid, self.default_bandwidth) \
                            - res.get("network_traffic", 0.0)
                if remaining > max_remaining:
                    max_remaining, best_rid = remaining, rid
            return best_rid, max_remaining

        if not valid_tms or all(s == 0.0 for s in valid_tms.values()):
            if bottleneck_cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                fb_tm, fb_remaining = _find_max_remaining_bw_tm()
                print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — "
                      f"[NETWORK 智能退回] 選剩餘頻寬最大的 TM: "
                      f"{fb_tm} (剩餘={fb_remaining/1e6:.2f}MB/s)")
                return fb_tm, affinity_scores
            else:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 無有效 TM，退回 CPU 負載均衡")
                return None, affinity_scores

        best_tm = max(valid_tms, key=lambda rid: valid_tms[rid])
        print(f"   ✅ [{bottleneck_subtask_id}] Step 4 — "
              f"拓撲親和力選定目標 TM: {best_tm} (AffinityScore={valid_tms[best_tm]/1e6:.4f}MB/s)")
        return best_tm, affinity_scores

    def generate_migration_plan(self, prioritized_list=None):
        """
        啟發式貪婪分配演算法（完整保留 v7 邏輯）。
        Fallback 三層降級策略：
          Layer 1：頻寬飽和率 < 0.85 的 TM 中選最充裕者
          Layer 2：所有 TM 超過 0.85，選飽和率最低者（最小傷害原則）
          Layer 3：所有 TM Slot 均滿，強制回歸原位
        """
        if prioritized_list is None:
            if not self._bottleneck_subtasks:
                print("⚠️ 未檢測到瓶頸，無需產生遷移計畫")
                return None
            prioritized_list = [(sid, 1.0) for sid, _, _ in self._bottleneck_subtasks]

        tm_info = self.get_taskmanager_info()
        current_locations = self.get_subtask_locations()
        prioritized_subtask_ids = {sid for sid, _ in prioritized_list}

        if not tm_info or not prioritized_list:
            print("⚠️ 無法獲取 TaskManager 資訊或優先級清單為空")
            return None

        MAX_SLOTS_PER_TM   = 5
        SOFT_BW_LIMIT_RATIO = 0.85

        # 初始化資源地圖（扣除瓶頸 subtask 的靜態資源）
        resource_map = {}
        for rid, info in tm_info.items():
            static_slots = static_busy = static_net = 0
            for sid, loc_rid in current_locations.items():
                if loc_rid == rid and sid not in prioritized_subtask_ids and sid in self._task_info:
                    static_slots += 1
                    static_busy  += self._task_info[sid]['T_busy'] * 1000
                    static_net   += self._task_info[sid]['observed_rate']
            resource_map[rid] = {
                'slots':           static_slots,
                'busy_time':       static_busy,
                'network_traffic': static_net,
                'cpu_limit':       info['cpu_limit'],
                'host':            info['host'],
            }

        print(f"\n📊 資源快照 (Pre-deduction 後已使用資源):")
        for rid, res in resource_map.items():
            print(f"   {rid}: Slots={res['slots']}/{MAX_SLOTS_PER_TM}, "
                  f"BusyTime={res['busy_time']:.0f}ms, "
                  f"NetTraffic={res['network_traffic']/(1024*1024):.2f}MB/s")

        migration_plan = {
            sid: rid for sid, rid in current_locations.items()
            if sid not in prioritized_subtask_ids
        }

        # pending_locations：追蹤同一輪中已被分配到新位置的 subtask。
        # calculate_topology_affinity 優先查此表，確保後分配的 subtask
        # 能正確感知先前已決定遷移的 subtask 的新位置，
        # 解決「親和力計算使用舊位置」的順序依賴問題。
        # 案例：WAC_0→tm_20c_3, WAC_3→tm_20c_2 已先分配，
        #        HOP_3 的親和力應感知 WAC 的新位置而非 tm_20c_5（舊位置）。
        pending_locations = {}

        print(f"\n🎯 啟發式貪婪分配")
        print(f"   待分配瓶頸 Subtask: {len(prioritized_subtask_ids)} 個")

        # Bundle 偵測（FORWARD 策略配對）
        _FORWARD_STRATEGY_TASKS = {
            "Window_Join": "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer"
        }
        work_items     = []
        already_bundled = set()

        print(f"\n📦 [Bundle 偵測] 掃描 FORWARD 配對中...")
        for subtask_id, priority_score in prioritized_list:
            if subtask_id in already_bundled:
                continue
            task_name_i = self._task_info.get(subtask_id, {}).get("task_name", "")
            matched_upstream_kw = matched_downstream_kw = None
            for upstream_kw, downstream_kw in _FORWARD_STRATEGY_TASKS.items():
                if upstream_kw in task_name_i:
                    matched_upstream_kw, matched_downstream_kw = upstream_kw, downstream_kw
                    break
            if matched_upstream_kw is None:
                work_items.append((subtask_id, priority_score))
                continue

            upstream_str_idx = subtask_id.split('_')[-1]
            ds_simple_kw = next(
                (kw for kw in matched_downstream_kw.split(':') if kw and not kw.startswith('_')), ""
            )
            paired_ds_id = paired_ds_score = None
            for other_id, other_score in prioritized_list:
                if other_id in already_bundled or other_id == subtask_id:
                    continue
                other_task = self._task_info.get(other_id, {}).get("task_name", "")
                if ds_simple_kw and ds_simple_kw in other_task and other_id.split('_')[-1] == upstream_str_idx:
                    paired_ds_id, paired_ds_score = other_id, other_score
                    break

            if paired_ds_id is None:
                work_items.append((subtask_id, priority_score))
                continue

            up_info = self._task_info.get(subtask_id, {})
            dn_info = self._task_info.get(paired_ds_id, {})
            bundle_traffic = (up_info.get('observed_input_rate', 0.0) +
                              dn_info.get('observed_output_rate', 0.0))
            work_items.append({
                'is_bundle': True,
                'upstream_id': subtask_id, 'downstream_id': paired_ds_id,
                'priority_score': max(priority_score, paired_ds_score),
                'required_slots': 2, 'bundle_traffic': bundle_traffic,
            })
            already_bundled.update([subtask_id, paired_ds_id])
            print(f"   📦 打包成功: [{subtask_id}] + [{paired_ds_id}] "
                  f"外部流量={bundle_traffic/1e6:.2f}MB/s")

        work_items.sort(
            key=lambda x: x['priority_score'] if isinstance(x, dict) else x[1],
            reverse=True
        )
        single_count = sum(1 for x in work_items if not isinstance(x, dict))
        bundle_count = sum(1 for x in work_items if isinstance(x, dict))
        print(f"   分配佇列: {single_count} 個 single + {bundle_count} 個 bundle\n")

        # 貪婪分配主迴圈
        for item in work_items:

            # 路徑 A：Bundle
            if isinstance(item, dict) and item.get('is_bundle'):
                upstream_id   = item['upstream_id']
                downstream_id = item['downstream_id']
                bundle_traffic = item['bundle_traffic']
                priority_score = item['priority_score']
                up_detail  = self._task_info.get(upstream_id, {})
                dn_detail  = self._task_info.get(downstream_id, {})
                bundle_busy_ms = (up_detail.get('T_busy', 0) + dn_detail.get('T_busy', 0)) * 1000
                orig_up_rid  = current_locations.get(upstream_id, 'unknown')
                orig_dn_rid  = current_locations.get(downstream_id, 'unknown')

                best_bundle_tm    = None
                best_bundle_score = -float('inf')
                for rid, res in resource_map.items():
                    if res['slots'] + 2 > MAX_SLOTS_PER_TM:
                        continue
                    tm_limit  = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                    net_score = 1 - (res['network_traffic'] + bundle_traffic) / tm_limit
                    if net_score <= 0.0:
                        continue
                    cpu_score = 1 - (res['busy_time'] + bundle_busy_ms) / (res['cpu_limit'] * 1000)
                    if cpu_score > best_bundle_score:
                        best_bundle_score, best_bundle_tm = cpu_score, rid

                if best_bundle_tm:
                    migration_plan[upstream_id]   = best_bundle_tm
                    migration_plan[downstream_id] = best_bundle_tm
                    resource_map[best_bundle_tm]['slots']           += 2
                    resource_map[best_bundle_tm]['busy_time']       += bundle_busy_ms
                    resource_map[best_bundle_tm]['network_traffic'] += bundle_traffic
                    print(f"✅ Bundle 分配成功 (優先級={priority_score:.3f}): "
                          f"[{upstream_id}] + [{downstream_id}] → {best_bundle_tm}")
                else:
                    print(f"⚠️ Bundle 無可用 TM，降級為各自回歸原位")
                    for sid, orig_rid in [(upstream_id, orig_up_rid), (downstream_id, orig_dn_rid)]:
                        migration_plan[sid] = orig_rid
                        if orig_rid in resource_map:
                            sd = self._task_info.get(sid, {})
                            resource_map[orig_rid]['slots']           += 1
                            resource_map[orig_rid]['busy_time']       += sd.get('T_busy', 0) * 1000
                            resource_map[orig_rid]['network_traffic'] += sd.get('observed_rate', 0) * 1.2
                        print(f"   🔄 {sid} → 回歸原位: {orig_rid}")
                continue

            # 路徑 B：Single Subtask
            subtask_id, priority_score = item
            if subtask_id not in self._task_info:
                print(f"⚠️ {subtask_id} 不在 task_info 中，跳過")
                continue

            task_detail   = self._task_info[subtask_id]
            observed_rate = task_detail['observed_rate']
            max_capacity  = task_detail['max_capacity']
            busy_time_ms  = task_detail['T_busy'] * 1000
            state_size    = task_detail.get('state_size', 0)
            cause         = task_detail.get('bottleneck_cause', 'CPU_BOTTLENECK')
            original_rid  = current_locations.get(subtask_id, 'unknown')
            cpu_limit     = resource_map.get(original_rid, {}).get('cpu_limit', 1.0)
            subtask_traffic = observed_rate * 1.2
            per_busy_rate   = busy_time_ms * cpu_limit

            # Network 瓶頸優先使用拓撲親和力演算法
            network_affinity_success = False
            best_tm = None
            best_score = -float('inf')

            if cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                print(f"\n🌐 [{subtask_id}] 偵測為 Network 瓶頸 ({cause})，啟動拓撲親和力演算法...")
                affinity_best_tm, _ = self.calculate_topology_affinity(
                    subtask_id, resource_map, current_locations, pending_locations
                )
                if affinity_best_tm and resource_map.get(affinity_best_tm, {}).get('slots', MAX_SLOTS_PER_TM) + 1 <= MAX_SLOTS_PER_TM:
                    best_tm = affinity_best_tm
                    network_affinity_success = True
                    print(f"   ✅ 拓撲親和力演算法成功，直接選定 TM: {best_tm}")
                else:
                    print(f"   ⚠️ 拓撲親和力退回，改用 CPU 負載均衡")

            if not network_affinity_success:
                for rid, res in resource_map.items():
                    if res['slots'] + 1 > MAX_SLOTS_PER_TM:
                        continue
                    busy_on_target    = per_busy_rate / res['cpu_limit']
                    projected_busy    = res['busy_time'] + busy_on_target
                    projected_traffic = res['network_traffic'] + subtask_traffic
                    tm_limit  = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                    net_score = 1 - (projected_traffic / tm_limit)
                    cpu_score = 1 - (projected_busy / (res['cpu_limit'] * 1000))
                    if net_score > 0.0 and res['cpu_limit'] >= cpu_limit and cpu_score > best_score:
                        best_score, best_tm = cpu_score, rid

            if best_tm:
                migration_plan[subtask_id] = best_tm
                pending_locations[subtask_id] = best_tm   # 更新 pending，供後續親和力計算使用
                resource_map[best_tm]['slots']           += 1
                resource_map[best_tm]['busy_time']       += busy_time_ms
                resource_map[best_tm]['network_traffic'] += subtask_traffic
                print(f"✅ 分配成功 (優先級={priority_score:.3f}, "
                      f"{cause.replace('_BOTTLENECK','')}): {subtask_id}")
                print(f"   從: {original_rid} -> 到: {best_tm}")
                print(f"   新狀態: Slots={resource_map[best_tm]['slots']}/{MAX_SLOTS_PER_TM}, "
                      f"BusyTime={resource_map[best_tm]['busy_time']:.0f} "
                      f"Traffic={resource_map[best_tm]['network_traffic']/(1024*1024):.2f}MB/s\n")
                if state_size > 0:
                    print(f"   💾 State: {format_bytes(state_size)}")
            else:
                # Fallback 三層降級
                print(f"\n⚠️ 無可用 TM: {subtask_id} (所有 TM 違反硬約束)")
                fallback_rid = None

                soft_candidates = [
                    (rid, res['network_traffic'] / self.tm_bandwidth_map.get(rid, self.default_bandwidth))
                    for rid, res in resource_map.items()
                    if res['slots'] + 1 <= MAX_SLOTS_PER_TM and
                       res['network_traffic'] / self.tm_bandwidth_map.get(rid, self.default_bandwidth) < SOFT_BW_LIMIT_RATIO
                ]
                if soft_candidates:
                    fallback_rid = min(soft_candidates, key=lambda x: x[1])[0]
                    print(f"   🔄 Fallback Layer 1: {fallback_rid}")
                else:
                    hard_candidates = [
                        (rid, res['network_traffic'] / self.tm_bandwidth_map.get(rid, self.default_bandwidth))
                        for rid, res in resource_map.items()
                        if res['slots'] + 1 <= MAX_SLOTS_PER_TM
                    ]
                    if hard_candidates:
                        fallback_rid = min(hard_candidates, key=lambda x: x[1])[0]
                        print(f"   ⚠️ Fallback Layer 2: {fallback_rid}")
                    else:
                        fallback_rid = original_rid
                        print(f"   ⚠️ Fallback Layer 3 (回歸原位): {fallback_rid}")

                migration_plan[subtask_id] = fallback_rid
                if fallback_rid in resource_map:
                    resource_map[fallback_rid]['slots']           += 1
                    resource_map[fallback_rid]['busy_time']       += busy_time_ms
                    resource_map[fallback_rid]['network_traffic'] += subtask_traffic
                    bw_limit = self.tm_bandwidth_map.get(fallback_rid, self.default_bandwidth)
                    new_sat  = resource_map[fallback_rid]['network_traffic'] / bw_limit if bw_limit > 0 else 0.0
                    print(f"   ⚠️ Fallback TM 更新: "
                          f"Slots={resource_map[fallback_rid]['slots']}/{MAX_SLOTS_PER_TM}, "
                          f"NetSat={new_sat:.2%}")

        # 最終驗證
        print(f"\n📊 最終資源分配驗證:")
        validation_errors = []
        for rid, res in resource_map.items():
            tm_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
            slots_ok = res['slots'] <= MAX_SLOTS_PER_TM
            net_ok   = res['network_traffic'] <= tm_limit
            status   = "✅" if (slots_ok and net_ok) else "❌"
            print(f"   {status} {rid}: Slots={res['slots']}/{MAX_SLOTS_PER_TM}, "
                  f"BusyTime={res['busy_time']:.0f}, "
                  f"Traffic={res['network_traffic']/(1024*1024):.2f}/{tm_limit/(1024*1024):.2f}MB/s")
            if not slots_ok:
                validation_errors.append(f"{rid}: Slot 超限")
            if not net_ok:
                validation_errors.append(f"{rid}: 頻寬超限")

        if validation_errors:
            print(f"\n❌ 資源分配驗證失敗: {validation_errors}")
        else:
            print(f"\n✅ 資源分配驗證通過：所有硬約束滿足")

        migrated_count = sum(
            1 for sid in prioritized_subtask_ids
            if migration_plan.get(sid) != current_locations.get(sid)
        )
        print(f"\n✅ 遷移計畫包含 {len(migration_plan)} 個 subtask")
        print(f"   需要遷移: {migrated_count} 個瓶頸 Subtask")
        print(f"   維持原位: {len(prioritized_subtask_ids) - migrated_count} 個瓶頸 Subtask")

        return migration_plan, migrated_count

    # ══════════════════════════════════════════════════════════════════════════
    # TaskManager 資訊（完整保留 v7 邏輯）
    # ══════════════════════════════════════════════════════════════════════════

    def get_taskmanager_info(self):
        try:
            cpu_capacity_map = {
                "tm_20c_1": 2.0, "tm_20c_2": 2.0, "tm_20c_3": 2.0,
                "tm_20c_4": 1.0, "tm_20c_5": 1.0
            }
            tm_info = {}

            print(f"🔍 步驟 1: 透過 Flink REST API 發現 TaskManager...")
            try:
                tm_list_response = requests.get(f"{self.flink_rest_url}/taskmanagers", timeout=5)
                if tm_list_response.status_code == 200:
                    tm_list_data = tm_list_response.json()
                    if 'taskmanagers' in tm_list_data:
                        print(f"   ✅ 發現 {len(tm_list_data['taskmanagers'])} 個 TaskManager")
                        for tm in tm_list_data['taskmanagers']:
                            tm_id = tm.get('id')
                            try:
                                detail_response = requests.get(
                                    f"{self.flink_rest_url}/taskmanagers/{tm_id}", timeout=3
                                )
                                if detail_response.status_code == 200:
                                    detail_data = detail_response.json()
                                    resource_id = (detail_data.get('resourceId') or tm_id).replace('-', '_')
                                    host = "unknown"
                                    if 'path' in detail_data:
                                        path = detail_data['path']
                                        if path.startswith('/'):
                                            host_part = path[1:].split(':')[0]
                                            host = host_part.replace('_', '.') if '_' in host_part and '.' not in host_part else host_part
                                    if host == "unknown" and 'hardware' in detail_data:
                                        host = detail_data.get('hardware', {}).get('hostname', resource_id)
                                    tm_info[resource_id] = {
                                        "host": host, "current_load": 0.0,
                                        "cpu_limit": cpu_capacity_map.get(resource_id, 1.0)
                                    }
                                    print(f"      ✓ {resource_id}: Host={host}, "
                                          f"CPU={tm_info[resource_id]['cpu_limit']}")
                            except Exception as e:
                                print(f"      ⚠️ 無法獲取 TM {tm_id} 詳細資訊: {e}")
            except Exception as e:
                print(f"   ⚠️ 無法連接 Flink REST API: {e}")

            print(f"\n📊 步驟 2: 透過 Prometheus 查詢 TaskManager 負載...")
            try:
                query = 'avg(flink_taskmanager_job_task_busyTimeMsPerSecond) by (resource_id, tm_id, host)'
                prom_response = requests.get(f"{self.base_url}/api/v1/query",
                                             params={'query': query}, timeout=5)
                prom_data = prom_response.json()
                if prom_data['status'] == 'success' and prom_data['data']['result']:
                    print(f"   ✅ 從 Prometheus 獲取到 {len(prom_data['data']['result'])} 個 TM 負載資料")
                    for r in prom_data['data']['result']:
                        resource_id   = (r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')).replace('-', '_')
                        current_load  = float(r['value'][1])
                        if resource_id in tm_info:
                            tm_info[resource_id]["current_load"] = current_load
                            print(f"      ✓ 更新 {resource_id} 負載: {current_load:.2f}ms")
                        else:
                            host = r['metric'].get('host', 'unknown')
                            tm_info[resource_id] = {
                                "host": host, "current_load": current_load,
                                "cpu_limit": cpu_capacity_map.get(resource_id, 1.0)
                            }
                            print(f"      ✓ (僅 Prom) {resource_id}: Load={current_load:.2f}ms")
            except Exception as e:
                print(f"   ⚠️ Prometheus 查詢失敗: {e}")

            print(f"\n✅ 總計發現 {len(tm_info)} 個 TaskManager:")
            for rid, info in tm_info.items():
                print(f"   • {rid}: Host={info['host']}, Load={info['current_load']:.2f}ms, "
                      f"CPU={info['cpu_limit']}")
            return tm_info

        except Exception as e:
            print(f"⚠️ 獲取 TaskManager 資訊失敗: {e}")
            return {}

    # ══════════════════════════════════════════════════════════════════════════
    # Job 控制（完整保留 v7 邏輯）
    # ══════════════════════════════════════════════════════════════════════════

    def get_running_jobs(self):
        try:
            response = requests.get(f"{self.flink_rest_url}/jobs")
            data = response.json()
            return [job['id'] for job in data.get('jobs', []) if job['status'] == 'RUNNING']
        except Exception as e:
            print(f"⚠️ 獲取 Job 列表失敗: {e}")
            return []

    def stop_job_with_savepoint(self, job_id):
        try:
            url     = f"{self.flink_rest_url}/jobs/{job_id}/stop"
            payload = {"targetDirectory": self.savepoint_dir, "drain": False}
            print(f"🛑 停止 Job {job_id} 並建立 Savepoint...")
            response   = requests.post(url, json=payload)
            trigger_id = response.json().get('request-id')
            start_time = time.time()
            while time.time() - start_time < 120:
                status_response = requests.get(
                    f"{self.flink_rest_url}/jobs/{job_id}/savepoints/{trigger_id}"
                )
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
        try:
            config = self.job_config
            flink_cmd = (
                f"export NEXMARK_CONF_DIR={config['nexmark_conf_dir']} && "
                f"/opt/flink/bin/flink run "
                f"-s {savepoint_path} -d "
                f"-c {config['entry_class']} "
                f"-p {config['parallelism']} "
                f"{config['jar_path']} "
                f"{' '.join(config['program_args'])}"
            )
            docker_cmd = ["docker", "exec", "-i", config["container"], "bash", "-c", flink_cmd]
            print(f"🚀 從 Savepoint 重新提交 Job...")
            print(f"   執行命令: {' '.join(docker_cmd)}")
            subprocess.Popen(docker_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print(f"🚀 已發送提交指令，正在確認 Job 狀態...")
            poll_start = time.time()
            while time.time() - poll_start < 60:
                running_jobs = self.get_running_jobs()
                if running_jobs:
                    print(f"✅ 偵測到 Job 已進入 RUNNING 狀態 (耗時: {time.time() - poll_start:.1f}s)")
                    return running_jobs[0]
                time.sleep(1)
            return None
        except Exception as e:
            print(f"❌ 提交失敗: {e}")
            return None

    def wait_for_job_termination(self, job_id, max_wait_sec=30):
        start_wait = time.perf_counter()
        print(f"⏳ 正在確認 Job {job_id} 是否已釋放資源...")
        while time.perf_counter() - start_wait < max_wait_sec:
            try:
                if job_id not in self.get_running_jobs():
                    print(f"✅ Job {job_id} 已確認停止 [耗時: {time.perf_counter() - start_wait:.3f}s]")
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        print(f"⚠️ 等待 Job {job_id} 停止超時")
        return False

    def write_migration_plan(self, migration_plan):
        try:
            os.makedirs(os.path.dirname(self.migration_plan_path), exist_ok=True)
            with open(self.migration_plan_path, 'w') as f:
                json.dump(migration_plan, f, indent=2)
            print(f"✅ 遷移計畫已寫入: {self.migration_plan_path}")
            return True
        except Exception as e:
            print(f"❌ 寫入遷移計畫失敗: {e}")
            return False

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
            if not running_jobs:
                return False
            job_id = running_jobs[0]

        migration_ts  = time.time()
        current_locs  = self.get_subtask_locations()
        file_exists   = os.path.isfile(self.log_file)
        with open(self.log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "subtask_id", "current_tm",
                                 "status", "cause", "also_implicit"])
            for sid, target_tm in migration_plan.items():
                if current_locs.get(sid) != target_tm:
                    writer.writerow([migration_ts, sid, current_locs.get(sid, "unknown"),
                                     "Migrating", "", False])

        start_migration = time.perf_counter()
        stop_start      = time.perf_counter()
        savepoint_path  = self.stop_job_with_savepoint(job_id)
        stop_end        = time.perf_counter()
        if not savepoint_path:
            return False

        wait_start = time.perf_counter()
        print("⏳ 動態檢查資源釋放情況...")
        self.wait_for_job_termination(job_id)
        wait_end = time.perf_counter()

        restart_start = time.perf_counter()
        new_job_id = self.submit_job_from_savepoint(savepoint_path) if auto_restart else None
        restart_end = time.perf_counter()

        total_downtime     = time.perf_counter() - start_migration
        savepoint_latency  = stop_end - stop_start
        wait_latency       = wait_end - wait_start
        restart_latency    = restart_end - restart_start

        perf_log = "/home/yenwei/research/structure_setup/output/propose_migration_performance.csv"
        perf_exists = os.path.isfile(perf_log)
        with open(perf_log, "a", newline="") as f:
            writer = csv.writer(f)
            if not perf_exists:
                writer.writerow(["event_timestamp", "total_downtime", "savepoint_time",
                                 "resource_wait_time", "restart_time", "job_id"])
            writer.writerow([migration_ts, total_downtime, savepoint_latency,
                             wait_latency, restart_latency, job_id])

        print("\n" + "=" * 40)
        print(f"📊 Propose 遷移分析 (中斷時間: {total_downtime:.3f}s)")
        print(f"🔹 Savepoint: {savepoint_latency:.3f}s | Wait: {wait_latency:.3f}s | Restart: {restart_latency:.3f}s")
        print("=" * 40)

        if new_job_id:
            self.last_migration_time = current_time
            return True
        return False

    # ══════════════════════════════════════════════════════════════════════════
    # 主工作流
    # ══════════════════════════════════════════════════════════════════════════

    def print_subtask_status(self):
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
                      f"{busy_map.get(task_name,{}).get(idx,0.0):>{col_w[1]}.1f}"
                      f"{bp_map.get(task_name,{}).get(idx,0.0):>{col_w[2]}.1f}"
                      f"{idle_map.get(task_name,{}).get(idx,0.0):>{col_w[3]}.1f}"
                      f"{rate_mb:>{col_w[4]}.3f}"
                      f"{in_pool_map.get(task_name,{}).get(idx,0.0):>{col_w[5]}.3f}"
                      f"{out_pool_map.get(task_name,{}).get(idx,0.0):>{col_w[6]}.3f}"
                      f"{subtask_locations.get(subtask_id,'unknown'):>{col_w[7]}}")
        print("=" * sum(col_w) + "\n")

    def auto_detect_and_migrate(self):
        """
        5-Step Migration Workflow：
        1+2: detect_bottleneck（兩層並行）
        3:   evaluate_migration_trigger（全域守門員）
        4:   get_prioritized_list（多維度優先級排序）
        5:   generate_migration_plan（貪婪分配）
        """
        print("\n" + "=" * 100)
        print("Propose 檢查叢集狀態...")

        self.print_subtask_status()

        print("=" * 100)
        print("STEP 1 & 2: 瓶頸偵測 ")
        print("=" * 100)
        reports = self.detect_bottleneck()

        if not reports:
            print("⚠️ 無法獲取監控數據")
            return False

        if not self._bottleneck_subtasks:
            print("✅ 未檢測到瓶頸，系統運行正常")
            return False

        print(f"\n🔥 檢測到 {len(self._bottleneck_subtasks)} 個瓶頸 Subtask:")
        for subtask_id, cause_str, score in self._bottleneck_subtasks:
            info  = self._task_info.get(subtask_id, {})
            cause = info.get("bottleneck_cause", "UNKNOWN")
            icon  = "🔥" if "CPU" in cause else ("🌐" if "NETWORK" in cause else "⚠️")
            also  = " (+隱性)" if info.get("also_implicit") else ""
            print(f"   {icon} {subtask_id}: 壓力分數={score:.3f}, "
                  f"T_busy={info.get('T_busy',0)*1000:.0f}ms, "
                  f"T_bp={info.get('T_bp',0)*1000:.0f}ms, cause={cause}{also}")

        print("\n" + "=" * 100)
        print("STEP 3: 全局觸發評估 (Gatekeeper)")
        print("=" * 100)
        should_trigger, reason = self.evaluate_migration_trigger(self._bottleneck_subtasks)

        if not should_trigger:
            print(f"⚠️ 全局觸發條件未滿足 ({reason})，放棄本次遷移")
            return False

        print("\n" + "=" * 100)
        print("STEP 4: 多維度優先級排序")
        print("=" * 100)
        bottleneck_ids = [sid for sid, _, _ in self._bottleneck_subtasks]
        prioritized_list = self.get_prioritized_list(bottleneck_ids)

        if not prioritized_list:
            print("⚠️ 優先級列表為空")
            return False

        print("\n" + "=" * 100)
        print("STEP 5: 貪婪分配遷移計畫")
        print("=" * 100)
        result = self.generate_migration_plan(prioritized_list)
        if not result:
            return False
        migration_plan, migrated_count = result

        if migrated_count == 0:
            print("\n✨ 決策結果: 分配算法認為維持現狀是最佳選擇 (migrated_count = 0)")
            return False

        # 紀錄決策過程
        detail_exists    = os.path.isfile(self.detail_log)
        event_time       = time.time()
        current_locations = self.get_subtask_locations()

        with open(self.detail_log, "a", newline="") as f:
            writer = csv.writer(f)
            if not detail_exists:
                writer.writerow(["timestamp", "decision_step", "subtask_id",
                                 "priority_rank", "from_tm", "to_tm", "decision_reason"])
            for subtask_id, _, _ in self._bottleneck_subtasks:
                info  = self._task_info.get(subtask_id, {})
                cause = info.get("bottleneck_cause", "UNKNOWN")
                label = "CPU Overload" if "CPU" in cause else "Network Overload" if "NETWORK" in cause else "Latent Bottleneck"
                writer.writerow([event_time, "Diagnosis", subtask_id, "",
                                 current_locations.get(subtask_id, "unknown"), "", f"Detected {label}"])
            for rank, (subtask_id, priority_score) in enumerate(prioritized_list, start=1):
                writer.writerow([event_time, "Prioritization", subtask_id, rank,
                                 current_locations.get(subtask_id, "unknown"), "",
                                 f"Priority={priority_score:.3f} (Rank {rank})"])
            for subtask_id, target_tm in migration_plan.items():
                original_tm = current_locations.get(subtask_id, "unknown")
                if target_tm != original_tm:
                    cause  = self._task_info.get(subtask_id, {}).get("bottleneck_cause", "UNKNOWN")
                    reason = ("CPU load balance" if "CPU" in cause
                              else "network topology affinity" if "NETWORK" in cause
                    else "latent bottleneck resolution")
                    writer.writerow([event_time, "Assignment", subtask_id, "",
                                     original_tm, target_tm, f"{target_tm} selected: {reason}"])

        return self.trigger_migration(migration_plan)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CAOM Flink Bottleneck Detector v8")
    parser.add_argument("--query", required=True, choices=["q4", "q5", "q7"],
                        help="Nexmark query to monitor (q4, q5, or q7)")
    parser.add_argument("--id", dest="output_id", default="t16",
                        help="Experiment output folder name (default: t16)")
    args = parser.parse_args()

    detector = FlinkPropose(query_type=args.query, output_id=args.output_id)

    while True:
        detector.auto_detect_and_migrate()
        time.sleep(30)