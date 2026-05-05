import requests
import statistics
# import numpy as np
import csv
import json
import time
import os
import subprocess
import argparse

"""
propose_v7.py — 基於 propose_v6.py 修正以下四個缺陷：

缺陷 1：TM 層級網路飽和未傳遞到 Subtask 判定
  → Step D.2 改用 IQR 動態計算飽和閾值；Step D.4 優先以 TM 飽和標記 NETWORK_TM_SATURATED

缺陷 2：Excluded 條件使用固定閾值，過於嚴苛
  → Step D.4 改用「相對同儕比較」：T_busy < peer_median × 0.80 才排除

缺陷 3：算子層級用總量比較，掩蓋 Straggler
  → Step D.3 增加 Path B：IQR Straggler 偵測（max_U_eff > Tukey fence）

缺陷 4：Fallback 機制繞過頻寬約束
  → generate_migration_plan Fallback 改為「最小頻寬飽和率優先」策略
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
        "target_order": ["Source", "Hop","Window_Auction", "Window_Max"],
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
    標準實作，避免依賴 statistics.quantiles 的 Python 版本限制。
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

    AVERAGE_RECORD_SIZE = 100
    SOURCE_MAX_TPS = 12_000_000

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

        cfg = JOB_CONFIG[query_type]
        self.query_type = query_type
        self.job_name = cfg["job_name"]
        self.target_order = cfg["target_order"]

        output_dir = f"/home/yenwei/research/structure_setup/output/{output_id}/"
        os.makedirs(output_dir, exist_ok=True)
        self.log_file = os.path.join(output_dir, f"metrics_{query_type}.csv")
        self.detail_log = os.path.join(output_dir, f"migration_details_{query_type}.csv")

        print(f"[FlinkPropose] Monitoring job : {self.job_name}")
        print(f"[FlinkPropose] Metrics CSV    : {self.log_file}")
        print(f"[FlinkPropose] Details CSV    : {self.detail_log}")

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
            "tm_20c_1":     int(37.5 * 1024 * 1024),
            "tm_20c_2":     int(18.75 * 1024 * 1024),
            "tm_20c_3":     int(18.75 * 1024 * 1024),
            "tm_20c_4":     int(37.5 * 1024 * 1024),
            "tm_20c_5":     int(37.5 * 1024 * 1024),
        }
        self.default_bandwidth = int(37.5 * 1024 * 1024)

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
                    if task_name not in task_map:
                        task_map[task_name] = {}
                    task_map[task_name][idx] = val

            return task_map

        except Exception as e:
            print(f"⚠️ 連線錯誤: {e}")
            return {}

    def get_job_vertex_mapping(self, job_id):
        try:
            url = f"{self.flink_rest_url}/jobs/{job_id}"
            response = requests.get(url, timeout=5)
            data = response.json()

            vertex_mapping = {}
            if 'vertices' in data:
                for vertex in data['vertices']:
                    vertex_id = vertex.get('id')
                    vertex_name = vertex.get('name', 'Unknown')
                    clean_name = vertex_name.split('(')[0].strip() if '(' in vertex_name else vertex_name
                    vertex_mapping[vertex_name] = vertex_id

            return vertex_mapping

        except Exception as e:
            print(f"⚠️ 獲取 Vertex 映射失敗: {e}")
            return {}

    def get_latest_checkpoint_id(self, job_id):
        try:
            url = f"{self.flink_rest_url}/jobs/{job_id}/checkpoints"
            response = requests.get(url, timeout=5)
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
                print("⚠️ 沒有正在運行的 Job，無法獲取狀態大小")
                return {}

            job_id = running_jobs[0]
            print(f"📊 正在查詢 Job {job_id} 的狀態大小...")

            checkpoint_id = self.get_latest_checkpoint_id(job_id)
            if not checkpoint_id:
                print("⚠️ 未找到完成的 Checkpoint")
                return {}

            vertex_mapping = self.get_job_vertex_mapping(job_id)
            if not vertex_mapping:
                print("⚠️ 無法獲取 Vertex 映射")
                return {}

            print(f"📋 找到 {len(vertex_mapping)} 個 Operators")

            state_size_map = {}

            for vertex_name, vertex_id in vertex_mapping.items():
                try:
                    url = f"{self.flink_rest_url}/jobs/{job_id}/checkpoints/details/{checkpoint_id}/subtasks/{vertex_id}"
                    response = requests.get(url, timeout=5)

                    if response.status_code != 200:
                        continue

                    data = response.json()

                    if 'subtasks' in data:
                        subtasks = data['subtasks']
                        print(f"      Subtasks: {len(subtasks)}")
                        for subtask in subtasks:
                            subtask_index = subtask.get('index', -1)
                            state_size = subtask.get('state_size', 0)
                            if state_size == 0:
                                state_size = subtask.get('checkpointed_size', 0)

                            if subtask_index != -1:
                                if vertex_name not in state_size_map:
                                    state_size_map[vertex_name] = {}
                                state_size_map[vertex_name][subtask_index] = state_size

                        if vertex_name in state_size_map and state_size_map[vertex_name]:
                            total_size = sum(state_size_map[vertex_name].values())
                            print(f"   ✅ {vertex_name}: {len(state_size_map[vertex_name])} subtasks, 總計 {format_bytes(total_size)}")

                except Exception as e:
                    continue

            if state_size_map:
                print(f"✅ 成功從 Checkpoint {checkpoint_id} 獲取狀態大小")
            else:
                print("⚠️ 未能獲取任何狀態大小信息")

            return state_size_map

        except Exception as e:
            print(f"⚠️ 獲取狀態大小失敗: {e}")
            return {}

    def detect_bottleneck(self):
        """
        v7 改進版瓶頸偵測。
        相較 v6 的四項核心改進：
          1. TM 層級網路飽和用 IQR Tukey fence 動態判定，並傳遞至 Subtask 分類
          2. Excluded 邏輯改用相對同儕比較（T_busy < peer_median × 80%）
          3. 新增 IQR Straggler 偵測路徑，應對 Data Skew 造成的單點過載
          4. （Fallback 修正在 generate_migration_plan 中）
        """
        busy_data_map = self.query_metric_by_task('flink_taskmanager_job_task_busyTimeMsPerSecond')
        bp_data_map = self.query_metric_by_task('flink_taskmanager_job_task_backPressuredTimeMsPerSecond')
        idle_data_map = self.query_metric_by_task('flink_taskmanager_job_task_idleTimeMsPerSecond')
        rate_in_map = self.query_metric_by_task('flink_taskmanager_job_task_numBytesInPerSecond')
        rate_out_map = self.query_metric_by_task('flink_taskmanager_job_task_numBytesOutPerSecond')
        outPoolUsage = self.query_metric_by_task('max_over_time(flink_taskmanager_job_task_buffers_outPoolUsage[15s])')
        inPoolUsage = self.query_metric_by_task('max_over_time(flink_taskmanager_job_task_buffers_inPoolUsage[15s])')

        state_size_map = self.get_subtask_state_sizes()

        if not busy_data_map:
            return []

        # Build task structure
        task_info = {}
        for task_name in busy_data_map.keys():
            subtasks_busy = busy_data_map.get(task_name, {})
            subtasks_bp = bp_data_map.get(task_name, {})
            subtasks_idle = idle_data_map.get(task_name, {})

            for idx in subtasks_busy.keys():
                T_busy = subtasks_busy.get(idx, 0) / 1000.0
                T_bp = subtasks_bp.get(idx, 0) / 1000.0
                T_idle = subtasks_idle.get(idx, 0) / 1000.0

                obs_in = rate_in_map.get(task_name, {}).get(idx, 0)
                obs_out = rate_out_map.get(task_name, {}).get(idx, 0)
                primary_observed_rate = obs_out if "source" in task_name.lower() else obs_in

                state_size = 0
                for vertex_name, vertex_states in state_size_map.items():
                    if match_vertex_to_task_name(vertex_name, task_name):
                        state_size = vertex_states.get(idx, 0)
                        break

                if state_size == 0:
                    state_size = state_size_map.get(task_name, {}).get(idx, 0)

                subtask_id = f"{task_name}_{idx}"
                out_pool = outPoolUsage.get(task_name, {}).get(idx, 0.0)
                in_pool  = inPoolUsage.get(task_name, {}).get(idx, 0.0)

                task_info[subtask_id] = {
                    "task_name": task_name,
                    "subtask_index": idx,
                    "T_busy": T_busy,
                    "T_bp": T_bp,
                    "T_idle": T_idle,
                    "observed_input_rate": obs_in,
                    "observed_output_rate": obs_out,
                    "observed_rate": primary_observed_rate,
                    "actual_input_rate": 0.0,
                    "max_capacity": 0.0,
                    "is_bottleneck": False,
                    "bottleneck_cause": None,
                    "state_size": state_size,
                    "out_pool_usage": out_pool,
                    "in_pool_usage": in_pool,
                }

        # Step A: Recover actual source rate
        source_tasks = {k: v for k, v in task_info.items() if "Source" in v["task_name"]}

        for subtask_id, info in source_tasks.items():
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            observed_rate = info["observed_rate"]

            if T_busy > 0:
                print(f"  Source: {subtask_id} T_busy = {T_busy} T_bp = {T_bp} observed_rate = {observed_rate}")
                T_busy = max(T_busy, 0.15)
                actual_source_rate = observed_rate * (1 + T_bp / T_busy)
                info["actual_input_rate"] = actual_source_rate
                print(f" 過高 Source: {subtask_id} actual_input_rate = {actual_source_rate}, T_busy = {T_busy}")
            else:
                info["actual_input_rate"] = observed_rate

        # Step B: BFS-based actual input rate propagation
        operator_groups = {}
        for subtask_id, info in task_info.items():
            task_name = info["task_name"]
            if task_name not in operator_groups:
                operator_groups[task_name] = []
            operator_groups[task_name].append(subtask_id)

        ordered_operators = []
        for keyword in self.target_order:
            for op_name in operator_groups.keys():
                if keyword.lower() in op_name.lower() and op_name not in ordered_operators:
                    ordered_operators.append(op_name)

        for op_name in operator_groups.keys():
            if op_name not in ordered_operators:
                ordered_operators.append(op_name)

        self._ordered_operators = ordered_operators

        for i in range(1, len(ordered_operators)):
            upstream_op = ordered_operators[i - 1]
            current_op = ordered_operators[i]

            upstream_subtasks = operator_groups[upstream_op]
            current_subtasks = operator_groups[current_op]

            upstream_avg_actual = sum(task_info[st]["actual_input_rate"] for st in upstream_subtasks) / len(upstream_subtasks) if upstream_subtasks else 0
            upstream_avg_observed = sum(task_info[st]["observed_rate"] for st in upstream_subtasks) / len(upstream_subtasks) if upstream_subtasks else 1

            if upstream_avg_observed > 0:
                for subtask_id in current_subtasks:
                    observed_rate = task_info[subtask_id]["observed_rate"]
                    task_info[subtask_id]["actual_input_rate"] = upstream_avg_actual * (observed_rate / upstream_avg_observed) if upstream_avg_observed > 0 else observed_rate

        # Step C: Calculate max capacity
        for subtask_id, info in task_info.items():
            T_busy = max(info["T_busy"], 0.15)
            T_bp = info["T_bp"]
            T_idle = info["T_idle"]
            observed_rate = info["observed_rate"]

            if T_busy > 0 and observed_rate > 0:
                max_capacity = (1 + (T_bp + T_idle) / T_busy) * observed_rate
            else:
                max_capacity = observed_rate

            info["max_capacity"] = max_capacity

        # Step D.1: Collect TM-level network traffic
        subtask_locations = self.get_subtask_locations()

        tm_network_traffic = {}
        for subtask_id, info in task_info.items():
            tm_id = subtask_locations.get(subtask_id, "unknown")
            if tm_id != "unknown":
                if tm_id not in tm_network_traffic:
                    tm_network_traffic[tm_id] = {"in": 0.0, "out": 0.0}
                tm_network_traffic[tm_id]["in"] += info["observed_input_rate"]
                tm_network_traffic[tm_id]["out"] += info["observed_output_rate"]

        # ══════════════════════════════════════════════════════════════════════
        # Step D.2: IQR 動態 TM 頻寬飽和閾值（修正缺陷 1）
        #
        # v6 問題：固定 0.95 閾值，無法反映叢集當前頻寬分佈。
        # v7 修正：用 IQR Tukey fence（Tukey 1977）動態計算飽和邊界。
        #
        # 閾值邊界設計：
        #   上界 0.95：物理近飽和前兆，即使 IQR fence 更高也封頂於此
        #   下界 0.85：TCP 在 85% 飽和時因 ACK 延遲出現非線性吞吐量衰退
        #              （Intel 網路效能白皮書建議保留 15% 緩衝）
        # ══════════════════════════════════════════════════════════════════════
        saturation_rates = {}
        for tm_id, traffic in tm_network_traffic.items():
            limit = self.tm_bandwidth_map.get(tm_id, self.default_bandwidth)
            saturation_rates[tm_id] = traffic["out"] / limit if limit > 0 else 0.0

        if len(saturation_rates) >= 2:
            rate_values = sorted(saturation_rates.values())
            q1 = _quantile_linear(rate_values, 0.25)
            q3 = _quantile_linear(rate_values, 0.75)
            iqr = q3 - q1
            if iqr > 0:
                # Tukey fence：上限 0.95（物理飽和），下限 0.85（TCP 非線性衰退前）
                saturation_threshold = max(min(q3 + 1.5 * iqr, 0.95), 0.85)
            else:
                # IQR 為零代表所有 TM 飽和率相近，無統計差異；保守閾值
                saturation_threshold = 0.90
        else:
            saturation_threshold = 0.90

        tm_out_saturated = {}
        print(f"\n📊 [真實 Byte 輸出監測] TaskManager 網路狀態 (飽和閾值={saturation_threshold:.2%}):")
        for tm_id, rate in saturation_rates.items():
            limit = self.tm_bandwidth_map.get(tm_id, self.default_bandwidth)
            traffic = tm_network_traffic[tm_id]
            tm_out_saturated[tm_id] = (rate >= saturation_threshold)
            print(f"   {tm_id}: {traffic['out']/1e6:.2f}MB/s "
                  f"{'🔴' if tm_out_saturated[tm_id] else '🟢'} "
                  f"(Limit: {limit/1e6:.1f}MB/s, Sat={rate:.2%})")

        # ══════════════════════════════════════════════════════════════════════
        # Step D.3: 算子層級瓶頸判定（雙路徑）（修正缺陷 3）
        #
        # Path A（既有）：total_actual > total_max → 整個算子全局過載
        # Path B（新增）：IQR Straggler 偵測 → 單一 Subtask U_eff 顯著高於同儕
        #
        # Straggler 使用場景：Data Skew 造成某個 Subtask 過載，
        # 但其他 Subtask 有餘裕，總量比較 total_actual < total_max 不會觸發，
        # 需靠 IQR 偵測單點異常。
        #
        # U_eff = T_busy / (1 - T_bp)：扣除下游反壓的假性等待後的真實忙碌程度。
        # 0.8 的理論依據：M/M/1 排隊理論，ρ=0.8 時平均隊長 ρ/(1-ρ)=4，
        # 代表顯著積壓效應開始出現。
        # ══════════════════════════════════════════════════════════════════════
        bottleneck_candidate_ids = set()

        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]
            total_actual = sum(task_info[st]["actual_input_rate"] for st in subtasks)
            total_max = sum(task_info[st]["max_capacity"] for st in subtasks)

            # Path A：全局總量判斷（既有邏輯）
            if total_actual > total_max and total_max > 0:
                print(f"Operator '{op_name}' is a bottleneck "
                      f"(total_actual={total_actual:.2f}, total_max={total_max:.2f})")
                for st in subtasks:
                    bottleneck_candidate_ids.add(st)
                continue  # 已為全局瓶頸，不做 Straggler 重複檢查

            # Path B：Straggler 偵測（新增）
            if len(subtasks) < 2:
                continue

            u_effs = []
            for st in subtasks:
                t_busy = task_info[st]["T_busy"]
                t_bp = task_info[st]["T_bp"]
                free_time = max(1.0 - t_bp, 0.001)  # 防止 T_bp 接近 1.0 時除以零
                u_eff = min(t_busy / free_time, 1.0)  # 上限 1.0，避免反壓造成數值失真
                u_effs.append(u_eff)

            sorted_u = sorted(u_effs)
            median_u = statistics.median(u_effs)
            max_u = max(u_effs)

            q1_u = _quantile_linear(sorted_u, 0.25)
            q3_u = _quantile_linear(sorted_u, 0.75)
            iqr_u = q3_u - q1_u
            # IQR 為零時退化為 median 的 1.5 倍（退化閾值，意義同 150% 同儕中位數）
            straggler_fence = q3_u + 1.5 * iqr_u if iqr_u > 0 else median_u * 1.5

            # Straggler 成立條件：
            #   1. U_eff 超出 Tukey fence（統計顯著異常）
            #   2. U_eff > 0.8（本身已處於高負載臨界點，M/M/1 隊長 = 4）
            if max_u > straggler_fence and max_u > 0.8:
                straggler_sids = [
                    subtasks[i] for i, u in enumerate(u_effs)
                    if u > straggler_fence and u > 0.8
                ]
                print(f"Operator '{op_name}' has straggler subtask(s) "
                      f"[max_U_eff={max_u:.3f} > IQR-fence={straggler_fence:.3f}]: "
                      f"{straggler_sids}")
                # 只把真正的 straggler 加入候選，而非整個算子所有 subtask
                for st in straggler_sids:
                    bottleneck_candidate_ids.add(st)

        # ══════════════════════════════════════════════════════════════════════
        # Step D.4: Subtask 層級精準分類（修正缺陷 1 + 缺陷 2）
        #
        # 判定優先順序：
        #
        # 優先級 1 — TM 層級網路飽和 → NETWORK_TM_SATURATED（缺陷 1 修正）
        #   TM 整體頻寬達飽和，即使個別 outPool 未滿，該 Subtask 仍被實體頻寬約束
        #   壓制；遷移它可直接緩解 TM 網路壓力。
        #
        # 優先級 2 — 相對同儕比較排除（缺陷 2 修正，取代固定閾值）
        #   排除條件：T_busy < peer_median × 80%
        #   80% 相對閾值意義：此 Subtask 比同儕輕鬆超過 20%，不符合「顯著落後同儕」
        #   的 outlier 標準，遷移它的邊際改善效益低。
        #   與 Tukey fence 精神一致：只干預統計上的顯著偏離。
        #
        # 優先級 3 — CPU vs NETWORK Subtask 層級分類
        #   outPool >= 0.8 + T_bp > 0.1 → NETWORK_BOTTLENECK
        #   0.8 依據：TCP 流控在 buffer 80% 時啟動限速（RFC 3517 慢啟動機制）
        #   否則 → CPU_BOTTLENECK
        # ══════════════════════════════════════════════════════════════════════
        bottleneck_subtasks = []

        # 預先計算每個算子的同儕 T_busy 中位數，供相對比較使用
        # key = task_name（即 op_name），value = 中位數 T_busy（秒）
        op_peer_median_busy = {}
        for op_name in ordered_operators:
            busy_vals = [task_info[st]["T_busy"] for st in operator_groups[op_name]]
            op_peer_median_busy[op_name] = statistics.median(busy_vals) if busy_vals else 0.0

        for subtask_id in bottleneck_candidate_ids:
            info = task_info[subtask_id]
            tm_id = subtask_locations.get(subtask_id, "unknown")
            task_name = info["task_name"]

            # task_name 即為 op_name，直接查同儕中位數
            peer_median_busy = op_peer_median_busy.get(task_name, 0.0)

            # ── 優先級 1：TM 層級網路飽和 ────────────────────────────────────
            # TM 整體頻寬已達 IQR 飽和閾值，此 Subtask 是 TM 網路壓力的一部分，
            # 遷移它可直接減輕 TM 整體網路負擔，不論個別 outPool 是否已滿
            if tm_out_saturated.get(tm_id, False):
                info["bottleneck_cause"] = "NETWORK_TM_SATURATED"
                info["is_bottleneck"] = True
                bottleneck_subtasks.append((subtask_id, info["actual_input_rate"], info["max_capacity"]))
                continue

            # ── 優先級 2：相對同儕比較排除（取代固定 in_pool >= 0.1 等絕對值）────
            # 若 T_busy 低於同儕中位數的 80%，代表此 Subtask 的繁忙程度未顯著異常，
            # 即使所屬算子被標記為全局瓶頸，遷移它的邊際效益低
            if peer_median_busy > 0 and info["T_busy"] < peer_median_busy * 0.80:
                info["is_bottleneck"] = False
                print(f"   Subtask '{subtask_id}' excluded: "
                      f"T_busy={info['T_busy']*1000:.0f}ms < "
                      f"80% × peer_median={peer_median_busy*1000:.0f}ms "
                      f"({info['T_busy']/peer_median_busy:.1%} of peers)")
                continue

            # ── 優先級 3：CPU vs NETWORK Subtask 層級分類 ────────────────────
            if info["out_pool_usage"] >= 0.8 and info["T_bp"] > 0.1:
                # 輸出 buffer 高水位 + 反壓存在 → Subtask 層級網路瓶頸
                info["bottleneck_cause"] = "NETWORK_BOTTLENECK"
            else:
                # busy 時間高而非 buffer 問題 → CPU 瓶頸
                info["bottleneck_cause"] = "CPU_BOTTLENECK"

            info["is_bottleneck"] = True
            bottleneck_subtasks.append((subtask_id, info["actual_input_rate"], info["max_capacity"]))

        # Generate report
        report_list = []
        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]

            bottleneck_count = sum(1 for st in subtasks if task_info[st]["is_bottleneck"])
            cpu_bottleneck_count = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") == "CPU_BOTTLENECK")
            network_bottleneck_count = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"))
            avg_actual_rate = sum(task_info[st]["actual_input_rate"] for st in subtasks) / len(subtasks)
            avg_max_capacity = sum(task_info[st]["max_capacity"] for st in subtasks) / len(subtasks)
            max_busy = max(task_info[st]["T_busy"] * 1000 for st in subtasks)
            max_bp = max(task_info[st]["T_bp"] * 1000 for st in subtasks)

            if bottleneck_count > 0:
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
                "cpu_bottleneck_count": cpu_bottleneck_count,
                "network_bottleneck_count": network_bottleneck_count,
                "avg_actual_rate": round(avg_actual_rate, 2),
                "avg_max_capacity": round(avg_max_capacity, 2),
                "max_busy": round(max_busy, 1),
                "max_bp": round(max_bp, 1),
                "subtasks": subtasks
            })

        self._bottleneck_subtasks = bottleneck_subtasks
        self._task_info = task_info

        # --- 紀錄所有 Subtask 的狀態 ---
        file_exists = os.path.isfile(self.log_file)
        curr_time = time.time()

        with open(self.log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "subtask_id", "current_tm", "status"])

            for sid, info in task_info.items():
                cause = info.get("bottleneck_cause", "NONE")
                if cause == "CPU_BOTTLENECK":
                    status = "CPU_Bottleneck"
                elif cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                    status = "NET_Bottleneck"
                else:
                    status = "Healthy"
                current_tm = subtask_locations.get(sid, "unknown")
                writer.writerow([curr_time, sid, current_tm, status])

        return report_list

    def evaluate_migration_trigger(self, bottleneck_subtasks):
        """
        Job 級別全局評估：時間領域成本效益分析。
        比較「遷移停機時間」與「不遷移累積延遲」，捨棄固定閥值。
        """
        if not bottleneck_subtasks:
            return False, "無任何瓶頸，系統健康"

        max_stress_task = max(bottleneck_subtasks, key=lambda x: x[2])
        subtask_id, cause, u_eff = max_stress_task

        info = self._task_info.get(subtask_id, {})
        state_size_bytes = info.get("state_size", 0.0)
        state_size_mb = state_size_bytes / (1024 * 1024)

        T_fixed = 4.0
        BW_internal = 150.0
        C_mig = T_fixed + (state_size_mb / BW_internal)

        W_eval = 30.0
        U_safe = 0.80
        C_stay = max(0, u_eff - U_safe) * W_eval

        print(f"\n  [TRIGGER 時間成本分析] 基準瓶頸: {subtask_id}")
        print(f"  - 停機代價 (C_mig) : {C_mig:.2f} 秒 (包含 {state_size_mb:.1f}MB 狀態傳輸)")
        print(f"  - 延遲代價 (C_stay): {C_stay:.2f} 秒 (基於 U_eff={u_eff:.2f}, 預測窗口={W_eval}s)")

        if C_stay > C_mig:
            print(f"  ✅ [TRIGGER 成立] 不遷移造成的延遲 ({C_stay:.2f}s) > 停機代價 ({C_mig:.2f}s)，必須開刀！")
            return True, "C_stay > C_mig"
        else:
            print(f"  ❌ [TRIGGER 拒絕] 停機代價 ({C_mig:.2f}s) >= 預期延遲 ({C_stay:.2f}s)，維持現狀總體效能更好。")
            return False, "C_mig >= C_stay"

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
        v7 新增：NETWORK_TM_SATURATED cause 納入網路壓力計算路徑。
        """
        if not filtered_ids:
            return []

        subtask_locations = self.get_subtask_locations()

        tm_network_traffic = {}
        for subtask_id, info in self._task_info.items():
            actual_input_rate = info["actual_input_rate"]
            tm_resource_id = subtask_locations.get(subtask_id, "unknown")

            if tm_resource_id != "unknown" and actual_input_rate > 0:
                if tm_resource_id not in tm_network_traffic:
                    tm_network_traffic[tm_resource_id] = 0.0
                tm_network_traffic[tm_resource_id] += actual_input_rate

        entries = []

        print(f"\n🎯 多維度優先級計算 (Priority = D_overload × (1 + R_impact)):")
        print(f"{'Subtask ID':<60} {'Cause':<22} {'D_overload':<12} {'R_impact':<10} {'Priority':<10}")
        print("=" * 99)

        for sid in filtered_ids:
            if sid not in self._task_info:
                continue

            info = self._task_info[sid]
            cause = info.get("bottleneck_cause", "CPU")
            tm_resource_id = subtask_locations.get(sid, "unknown")

            if cause in ("CPU_BOTTLENECK", "CPU"):
                T_busy = info["T_busy"]
                T_bp   = info["T_bp"]
                free_time = 1.0 - T_bp
                u_eff = T_busy / free_time if free_time > 0 else 1.0
                d_overload = max(T_busy, u_eff)
            elif cause in ("NETWORK_BOTTLENECK", "NETWORK", "NETWORK_TM_SATURATED"):
                # TM 層級飽和與 Subtask 層級網路瓶頸共用 TM 整體流量 / 頻寬上限
                actual_net = tm_network_traffic.get(tm_resource_id, 0)
                limit = self.tm_bandwidth_map.get(tm_resource_id, self.default_bandwidth)
                d_overload = actual_net / limit if limit > 0 else 1.0
            else:
                d_overload = 1.0

            r_impact = self.calculate_topology_impact(sid)
            priority_score = d_overload * (1 + r_impact)

            entries.append({
                'subtask_id':     sid,
                'cause':          cause,
                'd_overload':     d_overload,
                'r_impact':       r_impact,
                'priority_score': priority_score,
            })

            cause_display = cause.replace("_BOTTLENECK", "")
            print(f"{sid:<60} {cause_display:<22} {d_overload:>10.3f}  {r_impact:>8.2f}  {priority_score:>8.3f}")

        entries.sort(key=lambda x: x['priority_score'], reverse=True)
        prioritized_list = [(e['subtask_id'], e['priority_score']) for e in entries]

        print(f"\n📊 優先級排序完成: {len(prioritized_list)} 個 Subtask")
        return prioritized_list

    def get_taskmanager_info(self):
        try:
            cpu_capacity_map = {
                "tm_20c_1": 2.0,
                "tm_20c_2": 2.0,
                "tm_20c_3": 2.0,
                "tm_20c_4": 1.0,
                "tm_20c_5": 1.0
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
                                detail_response = requests.get(f"{self.flink_rest_url}/taskmanagers/{tm_id}", timeout=3)
                                if detail_response.status_code == 200:
                                    detail_data = detail_response.json()

                                    resource_id = detail_data.get('resourceId') or tm_id
                                    resource_id = resource_id.replace('-', '_')

                                    host = "unknown"
                                    if 'path' in detail_data:
                                        path = detail_data['path']
                                        if path.startswith('/'):
                                            host_part = path[1:].split(':')[0]
                                            if '_' in host_part and not '.' in host_part:
                                                host = host_part.replace('_', '.')
                                            else:
                                                host = host_part

                                    if host == "unknown" and 'hardware' in detail_data:
                                        host = detail_data.get('hardware', {}).get('hostname', resource_id)

                                    cpu_limit = cpu_capacity_map.get(resource_id, 1.0)
                                    tm_info[resource_id] = {
                                        "host": host,
                                        "current_load": 0.0,
                                        "cpu_limit": cpu_limit
                                    }
                                    print(f"      ✓ {resource_id}: Host={host}, CPU={cpu_limit}")

                            except Exception as e:
                                print(f"      ⚠️ 無法獲取 TM {tm_id} 詳細資訊: {e}")
                                continue

                else:
                    print(f"   ⚠️ Flink REST API 回應異常: {tm_list_response.status_code}")

            except Exception as e:
                print(f"   ⚠️ 無法連接 Flink REST API: {e}")
                print(f"   ⚠️ 將僅依賴 Prometheus 資料")

            print(f"\n📊 步驟 2: 透過 Prometheus 查詢 TaskManager 負載...")
            try:
                query = 'avg(flink_taskmanager_job_task_busyTimeMsPerSecond) by (resource_id, tm_id, host)'
                prom_response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query}, timeout=5)
                prom_data = prom_response.json()

                if prom_data['status'] == 'success' and prom_data['data']['result']:
                    print(f"   ✅ 從 Prometheus 獲取到 {len(prom_data['data']['result'])} 個 TM 負載資料")

                    for r in prom_data['data']['result']:
                        resource_id = r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')
                        resource_id = resource_id.replace('-', '_')
                        current_load = float(r['value'][1])

                        if resource_id in tm_info:
                            tm_info[resource_id]["current_load"] = current_load
                            print(f"      ✓ 更新 {resource_id} 負載: {current_load:.2f}ms")
                        else:
                            host = r['metric'].get('host', 'unknown')
                            if '_' in host and not '.' in host:
                                host = host.replace('_', '.')
                            cpu_limit = cpu_capacity_map.get(resource_id, 1.0)
                            tm_info[resource_id] = {
                                "host": host,
                                "current_load": current_load,
                                "cpu_limit": cpu_limit
                            }
                            print(f"      ✓ (僅從 Prom) {resource_id}: Load={current_load:.2f}ms, CPU={cpu_limit}")
                else:
                    print(f"   ⚠️ Prometheus 未回傳任何負載資料 (所有 TM 可能都是空閒的)")

            except Exception as e:
                print(f"   ⚠️ Prometheus 查詢失敗: {e}")

            print(f"\n✅ 總計發現 {len(tm_info)} 個 TaskManager:")
            for rid, info in tm_info.items():
                print(f"   • {rid}: Host={info['host']}, Load={info['current_load']:.2f}ms, CPU={info['cpu_limit']}")

            if not tm_info:
                print(f"❌ 未找到任何 TaskManager (REST API 與 Prometheus 皆無資料)")

            return tm_info

        except Exception as e:
            print(f"⚠️ 獲取 TaskManager 資訊失敗: {e}")
            return {}

    def get_subtask_locations(self):
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
                resource_id = r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')

                subtask_id = f"{task_name}_{subtask_index}"
                subtask_locations[subtask_id] = resource_id

            return subtask_locations

        except Exception as e:
            print(f"⚠️ 獲取 Subtask 位置失敗: {e}")
            return {}

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
        header = (
            f"{'Subtask ID':<{col_w[0]}}"
            f"{'Busy(ms/s)':>{col_w[1]}}"
            f"{'BP(ms/s)':>{col_w[2]}}"
            f"{'Idle(ms/s)':>{col_w[3]}}"
            f"{'In(MB/s)':>{col_w[4]}}"
            f"{'inPool':>{col_w[5]}}"
            f"{'outPool':>{col_w[6]}}"
            f"{'TaskManager':>{col_w[7]}}"
        )
        print("\n" + "=" * sum(col_w))
        print(header)
        print("=" * sum(col_w))

        ordered_names = []
        for keyword in self.target_order:
            for name in busy_map.keys():
                if keyword.lower() in name.lower() and name not in ordered_names:
                    ordered_names.append(name)
        for name in busy_map.keys():
            if name not in ordered_names:
                ordered_names.append(name)

        for task_name in ordered_names:
            for idx in sorted(busy_map[task_name].keys()):
                subtask_id = f"{task_name}_{idx}"
                busy       = busy_map.get(task_name, {}).get(idx, 0.0)
                bp         = bp_map.get(task_name, {}).get(idx, 0.0)
                idle       = idle_map.get(task_name, {}).get(idx, 0.0)
                rate_bytes = rate_map.get(task_name, {}).get(idx, 0.0)
                rate_mb    = rate_bytes / (1024 * 1024)
                in_pool    = in_pool_map.get(task_name, {}).get(idx, 0.0)
                out_pool   = out_pool_map.get(task_name, {}).get(idx, 0.0)
                tm_id      = subtask_locations.get(subtask_id, "unknown")

                print(
                    f"{subtask_id:<{col_w[0]}}"
                    f"{busy:>{col_w[1]}.1f}"
                    f"{bp:>{col_w[2]}.1f}"
                    f"{idle:>{col_w[3]}.1f}"
                    f"{rate_mb:>{col_w[4]}.3f}"
                    f"{in_pool:>{col_w[5]}.3f}"
                    f"{out_pool:>{col_w[6]}.3f}"
                    f"{tm_id:>{col_w[7]}}"
                )

        print("=" * sum(col_w) + "\n")

    def get_neighbors(self, subtask_id):
        task_name = "_".join(subtask_id.rsplit("_", 1)[0].split("_"))

        current_pos = -1
        for i, keyword in enumerate(self.target_order):
            if keyword.lower() in task_name.lower():
                current_pos = i
                break

        if current_pos == -1:
            return (set(), set())

        upstream_keywords = set()
        if current_pos > 0:
            upstream_keywords.add(self.target_order[current_pos - 1])

        downstream_keywords = set()
        if current_pos < len(self.target_order) - 1:
            downstream_keywords.add(self.target_order[current_pos + 1])

        return (upstream_keywords, downstream_keywords)

    def calculate_topology_affinity(self, bottleneck_subtask_id, resource_map, current_locations):
        """
        拓撲感知資料引力親和性演算法。
        v7 新增：NETWORK_TM_SATURATED cause 與 NETWORK_BOTTLENECK 共用此路徑。
        """
        FORWARD_STRATEGY_TASKS = {
            "Window_Join" : "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer"
        }

        if bottleneck_subtask_id not in self._task_info:
            print(f"   ⚠️ [{bottleneck_subtask_id}] 不在 _task_info，無法計算親和力")
            return None, {}

        bottleneck_info = self._task_info[bottleneck_subtask_id]

        _, downstream_keywords = self.get_neighbors(bottleneck_subtask_id)

        if not downstream_keywords:
            print(f"   ⚠️ [{bottleneck_subtask_id}] 無下游算子關鍵字，跳過親和力計算")
            return None, {}

        downstream_subtasks = [
            sid for sid, info in self._task_info.items()
            if any(kw in info["task_name"] for kw in downstream_keywords)
        ]

        if not downstream_subtasks:
            print(f"   ⚠️ [{bottleneck_subtask_id}] 找不到任何下游 Subtask，跳過親和力計算")
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

        if is_forward:
            bottleneck_str_index = bottleneck_subtask_id.split('_')[-1]

            print(f"\n   📡 [{bottleneck_subtask_id}] Step 1 — Branch A (FORWARD 1-to-1) "
                  f"尋找下游字串 Index='{bottleneck_str_index}' 的配對 Subtask...")

            matched_forward_sid = None
            for d_sid in downstream_subtasks:
                d_info = self._task_info[d_sid]
                d_str_index = d_sid.split('_')[-1]
                if (forward_downstream_kw in d_info["task_name"]
                        and d_str_index == bottleneck_str_index):
                    matched_forward_sid = d_sid
                    break

            if matched_forward_sid is None:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Branch A — "
                      f"找不到字串 Index='{bottleneck_str_index}' 的下游 Subtask，退回比例推算")
                is_forward = False
            else:
                edge_weights[matched_forward_sid] = bytes_out_sb
                d_tm = current_locations.get(matched_forward_sid, "unknown")
                print(f"      ✅ 配對成功: {matched_forward_sid:<40} "
                      f"→ TM: {d_tm:<20} EdgeWeight={bytes_out_sb/1e6:.3f}MB/s (100%)")

        if not is_forward:
            total_downstream_rate = sum(
                self._task_info[d]["actual_input_rate"] for d in downstream_subtasks
            )

            if total_downstream_rate <= 0:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Branch B — 下游 actual_input_rate 總和為 0，退回")
                return None, {}

            for d_sid in downstream_subtasks:
                d_rate = self._task_info[d_sid]["actual_input_rate"]
                edge_weights[d_sid] = bytes_out_sb * (d_rate / total_downstream_rate)

            print(f"\n   📡 [{bottleneck_subtask_id}] Step 1 — Branch B (HASH M-to-N) Edge 權重估算 "
                  f"(下游總速率={total_downstream_rate/1e6:.2f}MB/s, "
                  f"S_b 輸出={bytes_out_sb/1e6:.2f}MB/s):")
            for d_sid, ew in edge_weights.items():
                d_tm = current_locations.get(d_sid, "unknown")
                print(f"      {d_sid:<40} → TM: {d_tm:<20} EdgeWeight={ew/1e6:.3f}MB/s")

        affinity_scores = {}
        for rid in resource_map.keys():
            score = sum(
                ew for d_sid, ew in edge_weights.items()
                if current_locations.get(d_sid, "unknown") == rid
            )
            affinity_scores[rid] = score

        print(f"   🎯 [{bottleneck_subtask_id}] Step 2 — 各 TM 親和力總分 (AffinityScore):")
        for rid, score in sorted(affinity_scores.items(), key=lambda x: -x[1]):
            print(f"      {rid:<25}: {score/1e6:.4f}MB/s")

        bytes_in_sb = bottleneck_info.get("observed_input_rate", 0.0)
        b_total_traffic = bytes_in_sb + bytes_out_sb
        b_current_tm_id = current_locations.get(bottleneck_subtask_id, "unknown")

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

            current_net = resource_map[rid].get("network_traffic", 0.0)
            bw_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
            projected_net = current_net + expected_network_added
            double_saving = 2.0 * affinity if rid != b_current_tm_id else 0.0

            if projected_net <= bw_limit:
                valid_tms[rid] = affinity
                print(f"      ✅ {rid:<25} [{case_label}]: "
                      f"增量={expected_network_added/1e6:+.3f}MB/s "
                      f"(total={b_total_traffic/1e6:.3f} - 2×score={double_saving/1e6:.3f}), "
                      f"預估={projected_net/1e6:.3f}/{bw_limit/1e6:.1f}MB/s → 通過")
            else:
                print(f"      ❌ {rid:<25} [{case_label}]: "
                      f"增量={expected_network_added/1e6:+.3f}MB/s "
                      f"(total={b_total_traffic/1e6:.3f} - 2×score={double_saving/1e6:.3f}), "
                      f"預估={projected_net/1e6:.3f}MB/s > 上限={bw_limit/1e6:.1f}MB/s → 剔除")

        bottleneck_cause = bottleneck_info.get("bottleneck_cause", "CPU_BOTTLENECK")

        def _find_max_remaining_bw_tm():
            best_rid, max_remaining = None, -float('inf')
            for rid, res in resource_map.items():
                bw_limit   = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                remaining  = bw_limit - res.get("network_traffic", 0.0)
                if remaining > max_remaining:
                    max_remaining, best_rid = remaining, rid
            return best_rid, max_remaining

        if not valid_tms:
            if bottleneck_cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                fb_tm, fb_remaining = _find_max_remaining_bw_tm()
                print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 無有效 TM（均超過頻寬上限）")
                print(f"      [NETWORK 智能退回] 選剩餘頻寬最大的 TM: "
                      f"{fb_tm} (剩餘頻寬={fb_remaining/1e6:.2f}MB/s)")
                return fb_tm, affinity_scores
            else:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 無有效 TM（均超過頻寬上限），退回 CPU 負載均衡")
                return None, affinity_scores

        if all(score == 0.0 for score in valid_tms.values()):
            if bottleneck_cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                fb_tm, fb_remaining = _find_max_remaining_bw_tm()
                print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 所有有效 TM 親和力皆為 0")
                print(f"      [NETWORK 智能退回] 選剩餘頻寬最大的 TM: "
                      f"{fb_tm} (剩餘頻寬={fb_remaining/1e6:.2f}MB/s)")
                return fb_tm, affinity_scores
            else:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 所有有效 TM 親和力皆為 0，退回 CPU 負載均衡")
                return None, affinity_scores

        best_tm = max(valid_tms, key=lambda rid: valid_tms[rid])
        print(f"   ✅ [{bottleneck_subtask_id}] Step 4 — 拓撲親和力選定目標 TM: "
              f"{best_tm} (AffinityScore={valid_tms[best_tm]/1e6:.4f}MB/s)")

        return best_tm, affinity_scores

    def generate_migration_plan(self, prioritized_list=None):
        """
        啟發式貪婪分配演算法。
        v7 改進：Fallback 從「Slot 最少優先」改為「頻寬飽和率最低優先」（修正缺陷 4）。

        Fallback 三層降級策略：
          Layer 1：頻寬飽和率 < 0.85 的 TM 中選最充裕者（TCP 非線性衰退前的保守上限）
          Layer 2：所有 TM 超過 0.85，選飽和率最低者（最小傷害原則）
          Layer 3：所有 TM Slot 均滿，強制回歸原位

        0.85 軟性上限依據：TCP 接近滿載時因 ACK 延遲導致吞吐量非線性下降，
        Intel 網路效能白皮書建議保留 15% 緩衝以避免 HOL blocking。
        """
        if prioritized_list is None:
            if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
                print("⚠️ 未檢測到瓶頸，無需產生遷移計畫")
                return None
            prioritized_list = [(subtask_id, 1.0) for subtask_id, _, _ in self._bottleneck_subtasks]

        tm_info = self.get_taskmanager_info()
        current_locations = self.get_subtask_locations()
        prioritized_subtask_ids = {subtask_id for subtask_id, _ in prioritized_list}

        if not tm_info:
            print("⚠️ 無法獲取 TaskManager 資訊，無法產生遷移計畫")
            return None

        if not prioritized_list:
            print("⚠️ 未檢測到過載的 subtask，無需產生遷移計畫")
            return None

        MAX_SLOTS_PER_TM = 5

        # Fallback 軟性頻寬上限（TCP 非線性衰退前的保守閾值）
        SOFT_BW_LIMIT_RATIO = 0.85

        # ===== 1. 初始化資源地圖 =====
        resource_map = {}
        for rid, info in tm_info.items():
            static_busy_time = 0
            static_slots = 0
            static_net = 0

            for sid, loc_rid in current_locations.items():
                if loc_rid == rid and sid not in prioritized_subtask_ids:
                    static_slots += 1
                    if sid in self._task_info:
                        static_busy_time += (self._task_info[sid]['T_busy'] * 1000)
                        static_net += self._task_info[sid]['observed_rate']

            resource_map[rid] = {
                'slots':  static_slots,
                'busy_time': static_busy_time,
                'network_traffic': static_net,
                'cpu_limit': info['cpu_limit'],
                'host': info['host']
            }

        print(f"\n📊 資源快照 (Pre-deduction 後已使用資源):")
        for rid, res in resource_map.items():
            print(f"   {rid}: Slots={res['slots']}/{MAX_SLOTS_PER_TM}, "
                  f"BusyTime={res['busy_time']:.0f}ms, "
                  f"NetTraffic={res['network_traffic']/(1024*1024):.2f}MB/s")

        # ===== Initialize Migration Plan =====
        migration_plan = {}
        for subtask_id, current_resource_id in current_locations.items():
            if subtask_id not in prioritized_subtask_ids:
                migration_plan[subtask_id] = current_resource_id

        print(f"\n🎯 啟發式貪婪分配 (待定狀態 + 動態權重 + 多因子評分)")
        print(f"   待分配瓶頸 Subtask: {len(prioritized_subtask_ids)} 個")

        # ═══════════════════════════════════════════════════════════════════════
        # 幫派遷移 / 綑綁排程 (Gang Migration / Task Bundling)
        # ═══════════════════════════════════════════════════════════════════════
        _FORWARD_STRATEGY_TASKS = {
            "Window_Join": "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer"
        }

        work_items    = []
        already_bundled = set()

        print(f"\n📦 [Bundle 偵測] 掃描 FORWARD 配對中...")
        for subtask_id, priority_score in prioritized_list:
            if subtask_id in already_bundled:
                continue

            task_name_i = self._task_info.get(subtask_id, {}).get("task_name", "")

            matched_upstream_kw   = None
            matched_downstream_kw = None
            for upstream_kw, downstream_kw in _FORWARD_STRATEGY_TASKS.items():
                if upstream_kw in task_name_i:
                    matched_upstream_kw   = upstream_kw
                    matched_downstream_kw = downstream_kw
                    break

            if matched_upstream_kw is None:
                work_items.append((subtask_id, priority_score))
                continue

            upstream_str_idx = subtask_id.split('_')[-1]

            ds_simple_kw = next(
                (kw for kw in matched_downstream_kw.split(':') if kw and not kw.startswith('_')),
                ""
            )

            paired_downstream_id    = None
            paired_downstream_score = None
            for other_id, other_score in prioritized_list:
                if other_id in already_bundled or other_id == subtask_id:
                    continue
                other_task_name = self._task_info.get(other_id, {}).get("task_name", "")
                other_str_idx   = other_id.split('_')[-1]

                if ds_simple_kw and ds_simple_kw in other_task_name and other_str_idx == upstream_str_idx:
                    paired_downstream_id    = other_id
                    paired_downstream_score = other_score
                    break

            if paired_downstream_id is None:
                work_items.append((subtask_id, priority_score))
                continue

            up_info   = self._task_info.get(subtask_id, {})
            dn_info   = self._task_info.get(paired_downstream_id, {})
            bundle_external_traffic = (
                    up_info.get('observed_input_rate',  0.0) +
                    dn_info.get('observed_output_rate', 0.0)
            )

            bundle = {
                'is_bundle':      True,
                'upstream_id':    subtask_id,
                'downstream_id':  paired_downstream_id,
                'priority_score': max(priority_score, paired_downstream_score),
                'required_slots': 2,
                'bundle_traffic': bundle_external_traffic,
            }
            work_items.append(bundle)
            already_bundled.add(subtask_id)
            already_bundled.add(paired_downstream_id)
            print(f"   📦 打包成功: [{subtask_id}] + [{paired_downstream_id}]"
                  f"  Index='{upstream_str_idx}'"
                  f"  外部流量={bundle_external_traffic/1e6:.2f}MB/s"
                  f"  (up.in={up_info.get('observed_input_rate',0)/1e6:.2f} +"
                  f" dn.out={dn_info.get('observed_output_rate',0)/1e6:.2f})")

        work_items.sort(
            key=lambda x: x['priority_score'] if isinstance(x, dict) else x[1],
            reverse=True
        )
        single_count = sum(1 for x in work_items if not isinstance(x, dict))
        bundle_count = sum(1 for x in work_items if isinstance(x, dict))
        print(f"   分配佇列: {single_count} 個 single + {bundle_count} 個 bundle\n")

        # ===== Greedy Allocation Loop =====
        for item in work_items:

            # ── 路徑 A：Bundle 分配 ───────────────────────────────────────────
            if isinstance(item, dict) and item.get('is_bundle'):
                upstream_id    = item['upstream_id']
                downstream_id  = item['downstream_id']
                bundle_traffic = item['bundle_traffic']
                priority_score = item['priority_score']

                up_detail  = self._task_info.get(upstream_id, {})
                dn_detail  = self._task_info.get(downstream_id, {})
                up_busy_ms = up_detail.get('T_busy', 0) * 1000
                dn_busy_ms = dn_detail.get('T_busy', 0) * 1000
                bundle_busy_ms = up_busy_ms + dn_busy_ms

                orig_up_rid = current_locations.get(upstream_id,   'unknown')
                orig_dn_rid = current_locations.get(downstream_id, 'unknown')

                best_bundle_tm    = None
                best_bundle_score = -float('inf')

                for rid, res in resource_map.items():
                    if res['slots'] + 2 > MAX_SLOTS_PER_TM:
                        continue

                    projected_traffic = res['network_traffic'] + bundle_traffic
                    tm_limit  = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                    net_score = 1 - (projected_traffic / tm_limit)
                    if net_score <= 0.0:
                        continue

                    projected_busy = res['busy_time'] + bundle_busy_ms
                    cpu_score = 1 - (projected_busy / (res['cpu_limit'] * 1000))

                    if cpu_score > best_bundle_score:
                        best_bundle_score = cpu_score
                        best_bundle_tm    = rid

                if best_bundle_tm:
                    migration_plan[upstream_id]   = best_bundle_tm
                    migration_plan[downstream_id] = best_bundle_tm
                    resource_map[best_bundle_tm]['slots']           += 2
                    resource_map[best_bundle_tm]['busy_time']       += bundle_busy_ms
                    resource_map[best_bundle_tm]['network_traffic'] += bundle_traffic
                    print(f"✅ Bundle 分配成功 (優先級={priority_score:.3f}): "
                          f"[{upstream_id}] + [{downstream_id}] → {best_bundle_tm}")
                    print(f"   Slots={resource_map[best_bundle_tm]['slots']}/{MAX_SLOTS_PER_TM}, "
                          f"外部流量={bundle_traffic/1e6:.2f}MB/s\n")
                else:
                    print(f"⚠️ Bundle 無可用 TM（≥2 Slot），降級為各自回歸原位")
                    for sid, orig_rid in [(upstream_id, orig_up_rid), (downstream_id, orig_dn_rid)]:
                        migration_plan[sid] = orig_rid
                        if orig_rid in resource_map:
                            sd = self._task_info.get(sid, {})
                            resource_map[orig_rid]['slots']           += 1
                            resource_map[orig_rid]['busy_time']       += sd.get('T_busy', 0) * 1000
                            resource_map[orig_rid]['network_traffic'] += sd.get('observed_rate', 0) * 1.2
                        print(f"   🔄 {sid} → 回歸原位: {orig_rid}")
                continue

            # ── 路徑 B：Single Subtask 分配 ──────────────────────────────────
            subtask_id, priority_score = item
            if not hasattr(self, '_task_info') or subtask_id not in self._task_info:
                print(f"⚠️ {subtask_id} 不在 task_info 中，跳過")
                continue

            task_detail = self._task_info[subtask_id]
            observed_rate = task_detail['observed_rate']
            actual_input_rate = task_detail['actual_input_rate']
            max_capacity = task_detail['max_capacity']
            busy_time_sec = task_detail['T_busy']
            busy_time_ms = busy_time_sec * 1000
            state_size = task_detail.get('state_size', 0)
            cause = task_detail.get('bottleneck_cause', 'CPU_BOTTLENECK')

            original_rid = current_locations.get(subtask_id, 'unknown')
            cpu_limit = resource_map.get(original_rid, {}).get('cpu_limit', 1.0)

            subtask_traffic = observed_rate * 1.2
            per_busy_rate = busy_time_ms * cpu_limit

            upstream_keywords, downstream_keywords = self.get_neighbors(subtask_id)

            # Network 瓶頸（含 NETWORK_TM_SATURATED）優先使用拓撲親和力演算法
            network_affinity_success = False
            best_tm = None
            best_score = -float('inf')

            if cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                print(f"\n🌐 [{subtask_id}] 偵測為 Network 瓶頸 ({cause})，啟動拓撲親和力演算法...")
                affinity_best_tm, affinity_scores = self.calculate_topology_affinity(
                    subtask_id, resource_map, current_locations
                )
                if affinity_best_tm and resource_map.get(affinity_best_tm, {}).get('slots', MAX_SLOTS_PER_TM) + 1 <= MAX_SLOTS_PER_TM:
                    best_tm = affinity_best_tm
                    network_affinity_success = True
                    print(f"   ✅ 拓撲親和力演算法成功，直接選定 TM: {best_tm}，跳過貪婪評分迴圈")
                else:
                    print(f"   ⚠️ 拓撲親和力演算法退回，改用 CPU 負載均衡邏輯作為備援")

            if not network_affinity_success:
                for rid, res in resource_map.items():
                    busy_on_target = per_busy_rate / res['cpu_limit']
                    projected_slots = res['slots'] + 1
                    projected_busy = res['busy_time'] + busy_on_target
                    projected_traffic = res['network_traffic'] + subtask_traffic

                    if projected_slots > MAX_SLOTS_PER_TM:
                        continue

                    cpu_score = 1 - (projected_busy / (res['cpu_limit'] * 1000))
                    tm_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                    net_score = 1 - (projected_traffic / tm_limit)

                    if net_score > 0.0 and res['cpu_limit'] >= cpu_limit:
                        if cpu_score > best_score:
                            best_score = cpu_score
                            best_tm = rid

            # ===== Allocation Decision =====
            if best_tm:
                migration_plan[subtask_id] = best_tm
                resource_map[best_tm]['slots'] += 1
                resource_map[best_tm]['busy_time'] += busy_time_ms
                resource_map[best_tm]['network_traffic'] += subtask_traffic

                state_size_mb = state_size / (1024 * 1024) if state_size > 0 else 0
                overload_pct = ((observed_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0

                print(f"✅ 分配成功 (優先級={priority_score:.3f}, {cause.replace('_BOTTLENECK', '')}): {subtask_id}")
                print(f"   從: {original_rid} -> 到: {best_tm}")
                print(f"   新狀態: Slots={resource_map[best_tm]['slots']}/{MAX_SLOTS_PER_TM}, "
                      f"BusyTime={resource_map[best_tm]['busy_time']:.0f} "
                      f"Traffic={resource_map[best_tm]['network_traffic']/(1024*1024):.2f}MB/s \n")
                if state_size > 0:
                    print(f"   💾 State: {format_bytes(state_size)}")
            else:
                # ══════════════════════════════════════════════════════════════
                # Fallback：頻寬感知選擇（修正缺陷 4）
                #
                # v6 問題：以「Slot 數量最少」選 TM，忽略頻寬飽和率，
                # 可能把 Subtask 塞入已飽和的 TM 導致 40.58MB/s > 18.75MB/s。
                #
                # v7 修正：三層降級策略
                #   Layer 1：飽和率 < 0.85 且有空餘 Slot → 選最充裕者
                #   Layer 2：所有超過 0.85 → 選飽和率最低者（最小傷害）
                #   Layer 3：所有 Slot 均滿 → 強制回歸原位
                # ══════════════════════════════════════════════════════════════
                print(f"\n⚠️ 無可用 TM: {subtask_id} (所有 TM 違反硬約束)")

                fallback_rid = None

                # Layer 1：飽和率 < SOFT_BW_LIMIT_RATIO 且有空餘 Slot
                soft_candidates = []
                for rid, res in resource_map.items():
                    if res['slots'] + 1 > MAX_SLOTS_PER_TM:
                        continue
                    bw_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                    sat_rate = res['network_traffic'] / bw_limit if bw_limit > 0 else 1.0
                    if sat_rate < SOFT_BW_LIMIT_RATIO:
                        soft_candidates.append((rid, sat_rate))

                if soft_candidates:
                    best = min(soft_candidates, key=lambda x: x[1])
                    fallback_rid = best[0]
                    print(f"   🔄 Fallback (Layer 1 - 軟性上限內): "
                          f"選頻寬飽和率最低的 TM: {fallback_rid} (Sat={best[1]:.2%})")
                else:
                    # Layer 2：所有 TM 均超過軟性上限，選飽和率最低者
                    hard_candidates = []
                    for rid, res in resource_map.items():
                        if res['slots'] + 1 > MAX_SLOTS_PER_TM:
                            continue
                        bw_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                        sat_rate = res['network_traffic'] / bw_limit if bw_limit > 0 else 1.0
                        hard_candidates.append((rid, sat_rate))

                    if hard_candidates:
                        best = min(hard_candidates, key=lambda x: x[1])
                        fallback_rid = best[0]
                        print(f"   ⚠️ Fallback (Layer 2 - 超過軟性上限): "
                              f"選頻寬飽和率最低的 TM: {fallback_rid} (Sat={best[1]:.2%})")
                    else:
                        # Layer 3：所有 Slot 均滿，強制回歸原位
                        fallback_rid = original_rid
                        print(f"   ⚠️ Fallback (Layer 3 - 所有 TM Slot 均已滿): "
                              f"強制回歸原位: {fallback_rid}")

                migration_plan[subtask_id] = fallback_rid

                if fallback_rid in resource_map:
                    resource_map[fallback_rid]['slots'] += 1
                    resource_map[fallback_rid]['busy_time'] += busy_time_ms
                    resource_map[fallback_rid]['network_traffic'] += subtask_traffic

                    bw_limit_fb = self.tm_bandwidth_map.get(fallback_rid, self.default_bandwidth)
                    new_sat = resource_map[fallback_rid]['network_traffic'] / bw_limit_fb if bw_limit_fb > 0 else 0.0
                    print(f"   ⚠️ Fallback TM 資源更新: "
                          f"Slots={resource_map[fallback_rid]['slots']}/{MAX_SLOTS_PER_TM}, "
                          f"BusyTime={resource_map[fallback_rid]['busy_time']:.0f}ms, "
                          f"NetSat={new_sat:.2%}")

        # ===== Final Validation =====
        print(f"\n📊 最終資源分配驗證:")
        validation_errors = []
        for rid, res in resource_map.items():
            slots_ok = res['slots'] <= MAX_SLOTS_PER_TM
            tm_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
            net_ok = res['network_traffic'] <= tm_limit
            limit_mb = tm_limit / (1024 * 1024)

            status = "✅" if (slots_ok and net_ok) else "❌"

            print(f"   {status} {rid}: Slots={res['slots']}/{MAX_SLOTS_PER_TM}, "
                  f"BusyTime={res['busy_time']:.0f}, "
                  f"Traffic={res['network_traffic']/(1024*1024):.2f}/{limit_mb:.2f}MB/s")

            if not slots_ok:
                validation_errors.append(f"{rid}: Slot 超限 ({res['slots']} > {MAX_SLOTS_PER_TM})")
            if not net_ok:
                validation_errors.append(f"{rid}: 頻寬超限 ({res['network_traffic']/(1024*1024):.2f}MB/s > {limit_mb:.2f}MB/s)")

        if validation_errors:
            print(f"\n❌ 資源分配驗證失敗:")
            for error in validation_errors:
                print(f"   • {error}")
            print(f"\n⚠️ 警告：產生的遷移計畫可能違反硬約束，請檢查!")
        else:
            print(f"\n✅ 資源分配驗證通過：所有硬約束滿足")

        print(f"\n✅ 遷移計畫包含 {len(migration_plan)} 個 subtask")
        migrated_count = sum(1 for sid in prioritized_subtask_ids if migration_plan[sid] != current_locations.get(sid))
        print(f"   需要遷移: {migrated_count} 個瓶頸 Subtask")
        print(f"   維持原位: {len(prioritized_subtask_ids) - migrated_count} 個瓶頸 Subtask")

        return migration_plan, migrated_count

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

    def get_running_jobs(self):
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
        try:
            url = f"{self.flink_rest_url}/jobs/{job_id}/stop"
            payload = {
                "targetDirectory": self.savepoint_dir,
                "drain": False
            }

            print(f"🛑 停止 Job {job_id} 並建立 Savepoint...")
            response = requests.post(url, json=payload)
            data = response.json()

            trigger_id = data.get('request-id')

            max_wait = 120
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
        try:
            config = self.job_config
            program_args_str = " ".join(config["program_args"])

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

            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            print(f"🚀 已發送提交指令，正在確認 Job 狀態...")

            max_poll_time = 60
            poll_start = time.time()

            while time.time() - poll_start < max_poll_time:
                running_jobs = self.get_running_jobs()
                if running_jobs:
                    print(f"✅ 偵測到 Job 已進入 RUNNING 狀態 (耗時: {time.time() - poll_start:.1f}s)")
                    return running_jobs[0]
                time.sleep(1)

            stdout, stderr = process.communicate(timeout=5)
            if process.returncode != 0:
                print(f"❌ 命令執行失敗: {stderr}")
                return None

            return True

        except Exception as e:
            print(f"❌ 提交失敗: {e}")
            return None

    def wait_for_job_termination(self, job_id, max_wait_sec=30):
        start_wait = time.perf_counter()
        print(f"⏳ 正在確認 Job {job_id} 是否已釋放資源...")
        while time.perf_counter() - start_wait < max_wait_sec:
            try:
                running_jobs = self.get_running_jobs()
                if job_id not in running_jobs:
                    print(f"✅ Job {job_id} 已確認停止 [耗時: {time.perf_counter() - start_wait:.3f}s]")
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        print(f"⚠️ 等待 Job {job_id} 停止超時")
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
            if not running_jobs: return False
            job_id = running_jobs[0]

        migration_ts = time.time()
        current_locs = self.get_subtask_locations()
        file_exists = os.path.isfile(self.log_file)
        with open(self.log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "subtask_id", "current_tm", "status"])
            for sid, target_tm in migration_plan.items():
                if current_locs.get(sid) != target_tm:
                    writer.writerow([migration_ts, sid, current_locs.get(sid, "unknown"), "Migrating"])

        migration_event_timestamp = time.time()
        start_migration = time.perf_counter()

        stop_start = time.perf_counter()
        savepoint_path = self.stop_job_with_savepoint(job_id)
        stop_end = time.perf_counter()
        if not savepoint_path: return False

        wait_start = time.perf_counter()
        print("⏳ 動態檢查資源釋放情況...")
        self.wait_for_job_termination(job_id)
        wait_end = time.perf_counter()

        restart_start = time.perf_counter()
        new_job_id = None
        if auto_restart:
            new_job_id = self.submit_job_from_savepoint(savepoint_path)
        restart_end = time.perf_counter()

        end_migration = time.perf_counter()

        savepoint_latency = stop_end - stop_start
        wait_latency = wait_end - wait_start
        restart_latency = restart_end - restart_start
        total_downtime = end_migration - start_migration

        perf_log_file = "/home/yenwei/research/structure_setup/output/propose_migration_performance.csv"
        file_exists = os.path.isfile(perf_log_file)
        with open(perf_log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["event_timestamp", "total_downtime", "savepoint_time", "resource_wait_time", "restart_time", "job_id"])
            writer.writerow([migration_event_timestamp, total_downtime, savepoint_latency, wait_latency, restart_latency, job_id])

        print("\n" + "="*40)
        print(f"📊 Propose 遷移分析 (中斷時間: {total_downtime:.3f}s)")
        print(f"🔹 Savepoint: {savepoint_latency:.3f}s | Wait: {wait_latency:.3f}s | Restart: {restart_latency:.3f}s")
        print("="*40)

        if new_job_id:
            self.last_migration_time = current_time
            return True
        return False

    def auto_detect_and_migrate(self, busy_threshold=None, skew_threshold=None):
        """
        5-Step Migration Workflow:
        1 & 2: detect_bottleneck  — v7 改進版（四缺陷修正）
        3:     evaluate_migration_trigger — 全域守門員
        4:     get_prioritized_list — 多維度優先級排序
        5:     generate_migration_plan — v7 改進版 Fallback
        """
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
            info = self._task_info.get(subtask_id, {})
            T_busy = info.get("T_busy", 0)
            T_bp   = info.get("T_bp", 0)
            cause  = info.get("bottleneck_cause", "UNKNOWN")
            icon   = "🔥" if "CPU" in cause else ("🌐" if "NETWORK" in cause else "⚠️")
            print(f"   {icon} {subtask_id}: 壓力分數={score:.3f}, "
                  f"T_busy={T_busy*1000:.0f}ms, T_bp={T_bp*1000:.0f}ms, cause={cause}")

        # STEP 3
        print("\n" + "=" * 100)
        print("STEP 3: 全局觸發評估 (Gatekeeper)")
        print("=" * 100)
        should_trigger, reason = self.evaluate_migration_trigger(self._bottleneck_subtasks)

        if not should_trigger:
            print(f"⚠️ 全局觸發條件未滿足 ({reason})，放棄本次遷移")
            return False

        # STEP 4
        print("\n" + "=" * 100)
        print("STEP 4: 多維度優先級排序")
        print("=" * 100)
        bottleneck_ids = [sid for sid, _, _ in self._bottleneck_subtasks]
        prioritized_list = self.get_prioritized_list(bottleneck_ids)

        if not prioritized_list:
            print("⚠️ 優先級列表為空")
            return False

        # STEP 5
        print("\n" + "=" * 100)
        print("STEP 5: 貪婪分配遷移計畫 (6-slot limit, Normalized Load)")
        print("=" * 100)
        migration_plan, migrated_count = self.generate_migration_plan(prioritized_list)

        if not migration_plan:
            return False

        if migrated_count == 0:
            print("\n✨ 決策結果: 分配算法認為維持現狀是最佳選擇 (migrated_count = 0)")
            print("⚠️ 取消遷移觸發，避免無意義的中斷。")
            return False

        # 紀錄決策過程
        detail_exists = os.path.isfile(self.detail_log)
        event_time = time.time()
        current_locations = self.get_subtask_locations()

        with open(self.detail_log, "a", newline="") as f:
            writer = csv.writer(f)
            if not detail_exists:
                writer.writerow(["timestamp", "decision_step", "subtask_id", "priority_rank", "from_tm", "to_tm", "decision_reason"])

            for subtask_id, _, _ in self._bottleneck_subtasks:
                info = self._task_info.get(subtask_id, {})
                cause = info.get("bottleneck_cause", "UNKNOWN")
                cause_label = "CPU Overload" if "CPU" in cause else "Network Overload"
                writer.writerow([event_time, "Diagnosis", subtask_id, "", current_locations.get(subtask_id, "unknown"), "", f"Detected {cause_label}"])

            for rank, (subtask_id, priority_score) in enumerate(prioritized_list, start=1):
                info = self._task_info.get(subtask_id, {})
                cause = info.get("bottleneck_cause", "UNKNOWN")
                d_label = "D_overload" if "CPU" in cause else "D_overload(net)"
                writer.writerow([event_time, "Prioritization", subtask_id, rank, current_locations.get(subtask_id, "unknown"), "", f"Priority={priority_score:.3f} ({d_label}, Rank {rank})"])

            for subtask_id, target_tm in migration_plan.items():
                original_tm = current_locations.get(subtask_id, "unknown")
                if target_tm != original_tm:
                    info = self._task_info.get(subtask_id, {})
                    cause = info.get("bottleneck_cause", "UNKNOWN")
                    reason = "max absolute CPUscore" if "CPU" in cause else "network topology affinity"
                    writer.writerow([event_time, "Assignment", subtask_id, "", original_tm, target_tm, f"{target_tm} selected due to {reason}"])

        return self.trigger_migration(migration_plan)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CAOM Flink Bottleneck Detector v7")
    parser.add_argument("--query", required=True, choices=["q4", "q5", "q7"],
                        help="Nexmark query to monitor (q4, q5, or q7)")
    parser.add_argument("--id", dest="output_id", default="t16",
                        help="Experiment output folder name (default: t16)")
    args = parser.parse_args()

    detector = FlinkPropose(query_type=args.query, output_id=args.output_id)

    while True:
        detector.auto_detect_and_migrate()
        time.sleep(30)
