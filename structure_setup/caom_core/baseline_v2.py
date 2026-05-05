import requests
# import numpy as np
import argparse
import json
import time
import csv
import os
import subprocess
import re

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
                 job_config=None):
        self.base_url = prometheus_url
        self.flink_rest_url = flink_rest_url
        self.migration_plan_path = migration_plan_path
        self.savepoint_dir = savepoint_dir
        self.last_migration_time = 0
        self.migration_cooldown = 300  # 冷卻時間 5 分鐘，避免頻繁重啟
        self._bottleneck_subtasks = []  # CAOM detection results
        self._task_info = {}  # Task information from CAOM detection

        cfg = JOB_CONFIG[query_type]
        self.query_type = query_type
        self.target_order = cfg["target_order"]

        output_dir = f"/home/yenwei/research/structure_setup/output/{output_id}/"
        os.makedirs(output_dir, exist_ok=True)
        self.log_file    = os.path.join(output_dir, f"metrics_{query_type}_baseline.csv")
        self.detail_log  = os.path.join(output_dir, f"migration_details_{query_type}_baseline.csv")

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

                task_info[subtask_id] = {
                    "task_name": task_name,
                    "subtask_index": idx,
                    "T_busy": T_busy,
                    "T_bp": T_bp,
                    "T_idle": T_idle,
                    "observed_rate": observed_rate,
                    "actual_input_rate": 0.0,
                    "max_capacity": 0.0,
                    "is_bottleneck": False
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
            if total_actual_input_rate > total_max_capacity and total_max_capacity > 0 and avg_idle < 0.8:
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

        return report_list

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

        # ── Pre-deduct: 資源預扣除 vacate source TM slots for all bottleneck subtasks ──
        # This reflects that the entire bottleneck operator is stopped before any
        # subtask is reassigned, so the source nodes' slots are freed upfront.
        for candidate in migration_candidates:
            src = current_locations.get(candidate['subtask_id'], 'unknown')
            if src in slot_occupancy:
                slot_occupancy[src] -= 1
                # [Fix Bug 1 & 2] cpu_demand (ms/s) 與 tm_load_tracker (sum busyMs, ms/s) 同單位
                tm_load_tracker[src] -= candidate['cpu_demand']
                # [Fix Bug 4] net_demand 已是 bytes/s，直接加減，不需乘換算係數
                tm_net_tracker[src] -= candidate['net_demand']

        # ── Step 3: WASP filter + Amnis argmin(C_v / O_v) selection ──────────
        for candidate in migration_candidates:
            subtask_id = candidate['subtask_id']
            current_resource_id = current_locations.get(subtask_id, 'unknown')

            if current_resource_id not in tm_info:
                print(f"⚠️ {subtask_id} 的 resource_id {current_resource_id} 不在 TM 列表中，跳過")
                continue

            subtask_cpu_load = candidate['cpu_demand']       # ms/s
            subtask_net_bytes = candidate['net_demand']      # bytes/s（已是正確單位）

            best_node = None
            best_congestion_ratio = float('inf')

            for rid, info in tm_info.items():
                # ── WASP hard constraint 1: available computing slots ──────
                if slot_occupancy[rid] >= MAX_SLOTS_PER_TM:
                    continue

                # ── WASP hard constraint 2: bandwidth threshold ────────────
                # [Fix Bug 4] subtask_net_bytes 已是 bytes/s，與 net_cap 同單位，直接比較
                net_cap = network_capacity_map.get(rid, int(37.5 * 1024 * 1024))
                if (tm_net_tracker[rid] + subtask_net_bytes) > WASP_ALPHA * net_cap:
                    continue

                # ── Amnis objective: argmin(C_v / O_v) ───────────────────
                # C_v: TM 上現有 busy 總量 + 即將放入的 subtask busy 需求（ms/s）
                # O_v: TM 的 CPU 吞吐上限 = cpu_limit × MAX_SLOTS_PER_TM × 1000 ms/s
                # [Fix Bug 1 & 2] O_v 使用 slots × 1000 使容量有物理意義，
                # 讓雙核 TM（O_v=10000）比單核 TM（O_v=5000）有更大的承接能力。
                C_v = tm_load_tracker[rid] + subtask_cpu_load
                O_v = info['cpu_limit'] * MAX_SLOTS_PER_TM * 1000.0
                congestion_ratio = C_v / O_v if O_v > 0 else float('inf')
                print(f"   {subtask_id} -> {rid}: C_v={C_v:.3f}, O_v={O_v:.3f}, congestion_ratio={congestion_ratio:.3f} ")

                if congestion_ratio < best_congestion_ratio:
                    best_congestion_ratio = congestion_ratio
                    best_node = rid

            if best_node:
                migration_plan[subtask_id] = best_node

                # Update trackers so later subtasks in the sorted list see accurate state
                slot_occupancy[best_node] += 1
                tm_load_tracker[best_node] += subtask_cpu_load
                tm_net_tracker[best_node] += subtask_net_bytes

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
            O_v = cpu_limit * MAX_SLOTS_PER_TM * 1000.0
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
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            print(f"🚀 已發送提交指令，正在確認 Job 狀態...")

            # 2. 輪詢 Flink REST API 確認是否有新 Job 變成 RUNNING
            # 主動去詢問 Flink REST API
            max_poll_time = 60
            poll_start = time.time()

            while time.time() - poll_start < max_poll_time:
                running_jobs = self.get_running_jobs()
                if running_jobs:
                    # 這裡可以進一步比對是否為剛提交的 ID，但通常有 Job 在跑就是成功了
                    print(f"✅ 偵測到 Job 已進入 RUNNING 狀態 (耗時: {time.time() - poll_start:.1f}s)")
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

        if new_job_id:
            self.last_migration_time = current_time
            return True
        return False

    def auto_detect_and_migrate(self, busy_threshold=None, skew_threshold=None):
        """
        Automatically detect bottlenecks using CAOM algorithm and trigger migration
        Uses backpressure recovery to identify true bottlenecks in a single pass
        """
        # Run CAOM bottleneck detection
        reports = self.detect_bottleneck()

        if not reports:
            print("⚠️ 無法獲取監控數據")
            return False

        # Check if any bottlenecks were detected
        if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
            print("✅ 未檢測到瓶頸，系統運行正常")
            return False

        print("\n" + "=" * 100)
        print("CAOM 檢查叢集狀態...")
        self.print_subtask_status()

        print(f"\n🔥 CAOM 檢測到 {len(self._bottleneck_subtasks)} 個瓶頸 Subtask:")
        for subtask_id, actual_rate, max_capacity in self._bottleneck_subtasks:
            overload_pct = ((actual_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0
            print(f"   - {subtask_id}: 實際輸入 {actual_rate:.2f} rec/s > 最大容量 {max_capacity:.2f} rec/s (過載 {overload_pct:.1f}%)")

        # Generate migration plan (automatically uses CAOM detection results)
        migration_plan = self.generate_migration_plan()

        if not migration_plan:
            return False

        # Write migration plan to JSON
        if not self.write_migration_plan(migration_plan):
            return False

        # ── 記錄決策過程至 migration_details CSV ───────────────────────────────
        detail_exists     = os.path.isfile(self.detail_log)
        event_time        = time.time()
        current_locations = self.get_subtask_locations()

        # [Fix Bug 4] Rebuild Neptune interference scores using bytes/s for net_demand
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
                "subtask_id":          subtask_id,
                "actual_rate":         actual_rate,
                "cpu_demand":          info.get("T_busy", 0) * 1000,
                "net_demand":          net_demand_bytes,   # bytes/s
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
            # Diagnosis
            for subtask_id, actual_rate, max_cap in self._bottleneck_subtasks:
                overload_pct = ((actual_rate - max_cap) / max_cap * 100) if max_cap > 0 else 0
                writer.writerow([event_time, "Diagnosis", subtask_id, "",
                                 current_locations.get(subtask_id, "unknown"), "",
                                 f"CAOM_BOTTLENECK: actual={actual_rate:.1f} > cap={max_cap:.1f} ({overload_pct:.1f}% overload)"])
            # Prioritization (Neptune interference score ranking)
            for rank, c in enumerate(migration_candidates, start=1):
                writer.writerow([event_time, "Prioritization", c["subtask_id"], rank,
                                 current_locations.get(c["subtask_id"], "unknown"), "",
                                 f"Neptune_score={c['interference_score']:.4f} (Rank {rank}), net_demand_bytes={c['net_demand']:.1f}"])
            # Assignment
            for subtask_id, target_tm in migration_plan.items():
                original_tm = current_locations.get(subtask_id, "unknown")
                if target_tm != original_tm:
                    writer.writerow([event_time, "Assignment", subtask_id, "",
                                     original_tm, target_tm,
                                     f"{target_tm} selected: WASP+Amnis argmin(C_v/O_v)"])

        return self.trigger_migration(migration_plan)