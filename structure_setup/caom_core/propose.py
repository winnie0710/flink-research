import requests
# import numpy as np
import csv
import json
import time
import os
import subprocess

def format_bytes(bytes_value):
    """將字節轉換為人類可讀的格式"""
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
    """
    匹配 Flink REST API 的 vertex_name 與 Prometheus 的 task_name
    例如:
    - Vertex: "Window(TumblingEventTimeWindows(5000), EventTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) -> Map"
    - Task: "Window_Max____Map"
    """
    # 清理 vertex_name，移除括號內容
    clean_vertex = vertex_name.split('(')[0].strip() if '(' in vertex_name else vertex_name

    # 處理包含箭頭的情況 (例如: "Window -> Map")
    if '->' in vertex_name:
        # 提取主要部分並轉換為 task_name 格式
        parts = [p.strip().split('(')[0] for p in vertex_name.split('->')]
        # 移除括號後的部分
        clean_parts = [p.split('(')[0].strip() for p in parts]
        vertex_pattern = '_'.join(clean_parts)

        # 檢查是否匹配（模糊匹配）
        if all(part.lower() in task_name.lower() for part in clean_parts):
            return True

    # 簡單匹配：檢查 vertex 的主要關鍵字是否在 task_name 中
    vertex_keywords = clean_vertex.replace('->', '_').replace(' ', '_').lower()
    if vertex_keywords in task_name.lower():
        return True

    # 反向檢查
    task_keywords = task_name.replace('_', ' ').lower()
    if clean_vertex.lower() in task_keywords:
        return True

    return False

class FlinkPropose:
    # Network bandwidth limit: 50Mbit/s = 6.25MB/s
    BANDWIDTH_LIMIT_BYTES_PER_SEC = 125000000  # 100Mbit/s in bytes
    AVERAGE_RECORD_SIZE = 100  # bytes per record (default assumption 確實一筆資料大約 100 byte)

    def __init__(self, prometheus_url="http://localhost:9090",
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

        # Job configuration for auto-restart from savepoint
        self.job_config = job_config or {
            "container": "jobmanager",
            "entry_class": "com.github.nexmark.flink.BenchmarkIsoQ7",
            "parallelism": 4,
            "jar_path": "/opt/flink/usrlib/nexmark.jar",
            "nexmark_conf_dir": "/opt/nexmark",
            "program_args": [
                "--queries", "q7-isolated",
                "--location", "/opt/nexmark",
                "--suite-name", "100m",
                "--category", "oa",
                "--kafka-server", "kafka:9092"
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

    def get_job_vertex_mapping(self, job_id):
        """
        獲取 Job 的 Operator (Vertex) ID 映射
        返回: { "operator_name": "vertex_id", ... }
        """
        try:
            url = f"{self.flink_rest_url}/jobs/{job_id}"
            response = requests.get(url, timeout=5)
            data = response.json()

            vertex_mapping = {}

            # 從 job graph 中提取 vertex 信息
            if 'vertices' in data:
                for vertex in data['vertices']:
                    vertex_id = vertex.get('id')
                    vertex_name = vertex.get('name', 'Unknown')

                    # 清理 vertex 名稱（移除額外的信息）
                    # 例如: "Window(TumblingEventTimeWindows(5000), ...)" -> "Window"
                    clean_name = vertex_name.split('(')[0].strip() if '(' in vertex_name else vertex_name
                    vertex_mapping[vertex_name] = vertex_id

            return vertex_mapping

        except Exception as e:
            print(f"⚠️ 獲取 Vertex 映射失敗: {e}")
            return {}

    def get_latest_checkpoint_id(self, job_id):
        """
        獲取最近一次成功的 Checkpoint ID
        """
        try:
            url = f"{self.flink_rest_url}/jobs/{job_id}/checkpoints"
            response = requests.get(url, timeout=5)
            data = response.json()

            # 從 latest.completed 獲取最近完成的 checkpoint ID
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
        """
        使用 Flink REST API 獲取每個 subtask 的 checkpoint 狀態大小
        返回: { "task_name": { subtask_index: state_size_bytes, ... }, ... }
        """
        try:
            # Step 1: 獲取正在運行的 Job ID
            running_jobs = self.get_running_jobs()
            if not running_jobs:
                print("⚠️ 沒有正在運行的 Job，無法獲取狀態大小")
                return {}

            job_id = running_jobs[0]
            print(f"📊 正在查詢 Job {job_id} 的狀態大小...")

            # Step 2: 獲取最新的 Checkpoint ID
            checkpoint_id = self.get_latest_checkpoint_id(job_id)
            if not checkpoint_id:
                print("⚠️ 未找到完成的 Checkpoint")
                return {}

            # Step 3: 獲取 Job 的 Vertex 映射
            vertex_mapping = self.get_job_vertex_mapping(job_id)
            if not vertex_mapping:
                print("⚠️ 無法獲取 Vertex 映射")
                return {}

            print(f"📋 找到 {len(vertex_mapping)} 個 Operators")

            # Step 4: 對每個 Vertex 獲取其 Subtask 的狀態大小
            state_size_map = {}

            for vertex_name, vertex_id in vertex_mapping.items():
                try:
                    url = f"{self.flink_rest_url}/jobs/{job_id}/checkpoints/details/{checkpoint_id}/subtasks/{vertex_id}"
                    response = requests.get(url, timeout=5)

                    if response.status_code != 200:
                        continue

                    data = response.json()

                    # 從回應中提取每個 subtask 的狀態大小
                    if 'subtasks' in data:
                        subtasks = data['subtasks']
                        print(f"      Subtasks: {len(subtasks)}")
                        for subtask in subtasks:
                            # 使用 'index' 欄位，不是 'subtask'
                            subtask_index = subtask.get('index', -1)

                            # 狀態大小在 subtask 的頂層，不是在 checkpoint 內
                            state_size = subtask.get('state_size', 0)

                            # 如果沒有 state_size，嘗試 checkpointed_size
                            if state_size == 0:
                                state_size = subtask.get('checkpointed_size', 0)

                            if subtask_index != -1:
                                if vertex_name not in state_size_map:
                                    state_size_map[vertex_name] = {}
                                state_size_map[vertex_name][subtask_index] = state_size

                        # 顯示成功獲取的 operator 信息
                        if vertex_name in state_size_map and state_size_map[vertex_name]:
                            total_size = sum(state_size_map[vertex_name].values())
                            print(f"   ✅ {vertex_name}: {len(state_size_map[vertex_name])} subtasks, 總計 {format_bytes(total_size)}")

                except Exception as e:
                    # 某些 vertex 可能沒有狀態，跳過
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
        My propose Bottleneck Detection: Identify all potential bottleneck operators in a single pass
        Uses backpressure recovery to calculate actual input rates and max processing capacity
        """
        # Query all required metrics
        busy_data_map = self.query_metric_by_task('flink_taskmanager_job_task_busyTimeMsPerSecond')
        bp_data_map = self.query_metric_by_task('flink_taskmanager_job_task_backPressuredTimeMsPerSecond')
        idle_data_map = self.query_metric_by_task('flink_taskmanager_job_task_idleTimeMsPerSecond')
        rate_data_map = self.query_metric_by_task('flink_taskmanager_job_task_numRecordsInPerSecond')
        # 除了 In，也要抓取 Out 指標 (給source用)
        rate_in_map = self.query_metric_by_task('flink_taskmanager_job_task_numRecordsInPerSecond')
        rate_out_map = self.query_metric_by_task('flink_taskmanager_job_task_numRecordsOutPerSecond')
        # 獲取狀態大小（用於遷移代價評估）
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

                # 獲取狀態大小（bytes）
                # 需要在 state_size_map 中查找匹配的 vertex_name
                state_size = 0
                for vertex_name, vertex_states in state_size_map.items():
                    if match_vertex_to_task_name(vertex_name, task_name):
                        state_size = vertex_states.get(idx, 0)
                        break

                # 如果沒有匹配，嘗試直接匹配
                if state_size == 0:
                    state_size = state_size_map.get(task_name, {}).get(idx, 0)

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
                    "is_bottleneck": False,
                    "bottleneck_cause": None,  # "CPU_BOTTLENECK" or "NETWORK_BOTTLENECK"
                    "state_size": state_size  # bytes
                }

        # Step A: Recover actual source rate 由 source 開始 其他用 BFS 搭配 out/in 推算
        # TODO source 用背壓還原後過大 真的直接這樣算嗎？
        # Identify source operators (those with "Source" in name)
        source_tasks = {k: v for k, v in task_info.items() if "Source" in v["task_name"]}

        for subtask_id, info in source_tasks.items():
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            observed_rate = info["observed_rate"]

            if T_busy > 0:
                # λ̂_Source = λ_Source × (1 + T_bp / T_busy)  只考慮被「反壓」擋住的資料。
                actual_source_rate = observed_rate * (1 + T_bp / T_busy)
                info["actual_input_rate"] = actual_source_rate
                print(f" 過高 Source: {subtask_id} actual_input_rate = {actual_source_rate}")
            else:
                info["actual_input_rate"] = observed_rate

        # Step B: Recover actual input rate for all operators using BFS
        # Build adjacency list based on typical Flink pipeline order
        target_order = ["Source", "Window_Max", "Window_Join", "Sink"]

        # Group tasks by operator type
        operator_groups = {}
        for subtask_id, info in task_info.items():
            task_name = info["task_name"]
            if task_name not in operator_groups:
                operator_groups[task_name] = []
            operator_groups[task_name].append(subtask_id)

        # Order operators by pipeline position
        ordered_operators = []
        for keyword in target_order:
            for op_name in operator_groups.keys():
                if keyword in op_name and op_name not in ordered_operators:
                    ordered_operators.append(op_name)

        # Add any remaining operators
        for op_name in operator_groups.keys():
            if op_name not in ordered_operators:
                ordered_operators.append(op_name)

        # Propagate actual input rates downstream
        # TODO : 建立「多對多」的拓撲視圖 ，針對多輸入算子（如 Window_Join 同時接收來自兩個算子流的情況），目前的公式確實會失效，因為它假設了「一對一」的上下游關係。

        for i in range(1, len(ordered_operators)):
            upstream_op = ordered_operators[i - 1]
            current_op = ordered_operators[i]

            upstream_subtasks = operator_groups[upstream_op]
            current_subtasks = operator_groups[current_op]

            # Calculate average rates for upstream
            upstream_avg_actual = sum(task_info[st]["actual_input_rate"] for st in upstream_subtasks) / len(upstream_subtasks) if upstream_subtasks else 0
            upstream_avg_observed = sum(task_info[st]["observed_rate"] for st in upstream_subtasks) / len(upstream_subtasks) if upstream_subtasks else 1

            if upstream_avg_observed > 0:
                for subtask_id in current_subtasks:
                    observed_rate = task_info[subtask_id]["observed_rate"]
                    # λ̂_i = λ̂_upstream × (observed_rate_i / observed_rate_upstream)
                    ratio = observed_rate / upstream_avg_observed if upstream_avg_observed > 0 else 1
                    task_info[subtask_id]["actual_input_rate"] = upstream_avg_actual * (observed_rate / upstream_avg_observed) if upstream_avg_observed > 0 else observed_rate
                    print(f"   {upstream_op} -> {current_op}: {subtask_id}, ratio= {ratio} - actual_input_rate = {task_info[subtask_id]['actual_input_rate']}")

        # Step C: Calculate max capacity for all subtasks
        for subtask_id, info in task_info.items():
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            T_idle = info["T_idle"]
            observed_rate = info["observed_rate"]

            # Calculate max processing capacity: λ^a = (1 + (T_bp + T_idle) / T_busy) × λ   考慮被「反壓」與「空閒」浪費掉的所有潛力。
            if T_busy > 0 and observed_rate > 0:
                max_capacity = (1 + (T_bp + T_idle) / T_busy) * observed_rate
            else:
                max_capacity = observed_rate

            info["max_capacity"] = max_capacity

        # Step D: Hierarchical Diagnosis - Two-Branch Logic
        # Step D.1: Get subtask locations and TM-level network traffic
        subtask_locations = self.get_subtask_locations()

        # Calculate total network traffic per TaskManager
        tm_network_traffic = {}  # {tm_resource_id: total_bytes_per_sec}
        # 遍歷所有 subtask ，找出其所在的 tm_resource_id 後，將流量累加到該 TM 的總值中。
        for subtask_id, info in task_info.items():
            actual_input_rate = info["actual_input_rate"]
            tm_resource_id = subtask_locations.get(subtask_id, "unknown")

            if tm_resource_id != "unknown" and actual_input_rate > 0:
                # Calculate network traffic: rate × avg_record_size
                traffic_bytes = actual_input_rate * self.AVERAGE_RECORD_SIZE


                if tm_resource_id not in tm_network_traffic:
                    tm_network_traffic[tm_resource_id] = 0.0
                tm_network_traffic[tm_resource_id] += traffic_bytes
                print(f"Traffic update for TM {tm_resource_id}:{subtask_id} with {traffic_bytes} bytes")

        # Determine which TMs have reached bandwidth limit
        tm_bandwidth_saturated = {}
        print(f"\n📊 TaskManager 網路流量檢測:")
        for tm_id, total_traffic in tm_network_traffic.items():
            is_saturated = (total_traffic >= self.BANDWIDTH_LIMIT_BYTES_PER_SEC)
            tm_bandwidth_saturated[tm_id] = is_saturated
            traffic_mb = total_traffic / (1024 * 1024)
            limit_mb = self.BANDWIDTH_LIMIT_BYTES_PER_SEC / (1024 * 1024)
            saturation_pct = (total_traffic / self.BANDWIDTH_LIMIT_BYTES_PER_SEC) * 100
            status_icon = "🔴" if is_saturated else "🟢"
            print(f"   {status_icon} {tm_id}: {traffic_mb:.2f} MB/s / {limit_mb:.2f} MB/s ({saturation_pct:.1f}%)")

        # Step D.2: Subtask-Specific Classification
        bottleneck_subtasks = []

        for subtask_id, info in task_info.items():
            actual_input_rate = info["actual_input_rate"]
            max_capacity = info["max_capacity"]
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            tm_resource_id = subtask_locations.get(subtask_id, "unknown")

            # Check if actual_input_rate > max_capacity  後續觸發遷移的時候再設定閥值
            # 確認瓶頸是 cpu or network
            if actual_input_rate > max_capacity and max_capacity > 0:
                info["is_bottleneck"] = True

                # Branch A: CPU Bottleneck - Factory is full
                if T_busy > 0.8:  # 800ms/s
                    info["bottleneck_cause"] = "CPU_BOTTLENECK"
                    bottleneck_subtasks.append((subtask_id, actual_input_rate, max_capacity))

                # Branch B: Network Bottleneck - Factory is idle but entrance is narrow
                elif T_busy <= 0.8 and T_bp > 0 and tm_bandwidth_saturated.get(tm_resource_id, False):
                    info["bottleneck_cause"] = "NETWORK_BOTTLENECK"
                    bottleneck_subtasks.append((subtask_id, actual_input_rate, max_capacity))

                # Edge case: Neither CPU nor Network clearly identified
                else:
                    # Default to CPU if we can't determine
                    info["bottleneck_cause"] = "CPU_BOTTLENECK"
                    bottleneck_subtasks.append((subtask_id, actual_input_rate, max_capacity))

        # Generate report
        report_list = []
        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]

            # Aggregate statistics for the operator
            bottleneck_count = sum(1 for st in subtasks if task_info[st]["is_bottleneck"])
            cpu_bottleneck_count = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") == "CPU_BOTTLENECK")
            network_bottleneck_count = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") == "NETWORK_BOTTLENECK")
            avg_actual_rate = sum(task_info[st]["actual_input_rate"] for st in subtasks) / len(subtasks)
            avg_max_capacity = sum(task_info[st]["max_capacity"] for st in subtasks) / len(subtasks)
            max_busy = max(task_info[st]["T_busy"] * 1000 for st in subtasks)  # Convert back to ms
            max_bp = max(task_info[st]["T_bp"] * 1000 for st in subtasks)

            # Status determination
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

        # Store bottleneck info for migration planning
        self._bottleneck_subtasks = bottleneck_subtasks
        self._task_info = task_info

        return report_list

    def evaluate_migration_trigger(self, bottleneck_ids):
        """
        Two-Tier Migration Trigger Logic:
        Phase 1: Subtask-level filtering (individual cost-benefit)
        Phase 2: Job-level global assessment (total cost vs shared overhead)

        Args:
            bottleneck_ids: List of (subtask_id, actual_rate, max_capacity) tuples

        Returns:
            Tuple (should_trigger: bool, final_candidates: list of subtask_ids)
        """
        if not bottleneck_ids:
            return (False, [])

        # ========== Phase 1: Subtask-Level Evaluation ==========
        print(f"\n🔍 階段 1: Subtask 級別評估 (個別成本效益)")
        print(f"{'Subtask ID':<30} {'State(MB)':<12} {'D_mig':<10} {'D_bot':<10} {'Decision':<15}")
        print("=" * 90)

        worthy_candidates = []
        candidate_details = []  # Store for Phase 2

        for subtask_id, actual_rate, max_capacity in bottleneck_ids:
            # Get task info
            if subtask_id not in self._task_info:
                continue

            task_info = self._task_info[subtask_id]
            state_size = task_info.get('state_size', 0)
            state_size_mb = state_size / (1024 * 1024)

            # Calculate Individual Migration Cost: D_mig = 4 + state_size/(10 MB/s)
            # Note: state_size in bytes, so divide by (10 * 1024 * 1024) for 10 MB/s
            D_mig = 4 + (state_size / (10 * 1024 * 1024))

            # Calculate Individual Bottleneck Cost: D_bot = overload_ratio × 30
            if max_capacity > 0:
                overload_ratio = (actual_rate - max_capacity) / max_capacity
                D_bot = overload_ratio * 30
            else:
                overload_ratio = 0
                D_bot = 0

            # Phase 1 Decision: Keep only if D_bot > D_mig
            individual_worthy = D_bot > D_mig
            decision = "✅ KEEP" if individual_worthy else "❌ SKIP"

            print(f"{subtask_id:<30} {state_size_mb:>10.2f}  {D_mig:>8.2f}  {D_bot:>8.2f}  {decision:<15}")

            if individual_worthy:
                worthy_candidates.append(subtask_id)
                candidate_details.append({
                    'subtask_id': subtask_id,
                    'state_size': state_size,
                    'state_size_mb': state_size_mb,
                    'D_mig': D_mig,
                    'D_bot': D_bot,
                    'overload_ratio': overload_ratio
                })

        print(f"\n📋 階段 1 結果: {len(worthy_candidates)}/{len(bottleneck_ids)} 個 Subtask 通過個別評估")

        if not worthy_candidates:
            print("⚠️ 沒有 Subtask 通過個別成本效益評估")
            return (False, [])

        # ========== Phase 2: Job-Level Global Assessment ==========
        print(f"\n🌐 階段 2: Job 級別全局評估 (總體成本效益)")
        print("=" * 90)

        # Calculate Total Bottleneck Cost: Sum of all D_bot
        total_bottleneck_cost = sum(c['D_bot'] for c in candidate_details)

        # Calculate Total Migration Overhead:
        # Shared fixed cost (4s) + max recovery time (max state_size / 10 MB/s)
        max_state_size = max(c['state_size'] for c in candidate_details)
        max_recovery_time = max_state_size / (10 * 1024 * 1024)  # in seconds
        total_migration_overhead = 4 + max_recovery_time

        # Apply 20% buffer to prevent thrashing
        threshold_overhead = total_migration_overhead * 1.2

        # Global Trigger Decision
        should_trigger = total_bottleneck_cost > threshold_overhead

        print(f"總瓶頸成本 (Total D_bot):           {total_bottleneck_cost:.2f}s")
        print(f"總遷移開銷 (Shared Overhead):       {total_migration_overhead:.2f}s")
        print(f"  ├─ 固定成本 (Fixed):               4.00s")
        print(f"  └─ 最大恢復時間 (Max Recovery):    {max_recovery_time:.2f}s (State: {max_state_size / (1024*1024):.2f} MB)")
        print(f"觸發閾值 (1.2x Overhead):           {threshold_overhead:.2f}s")
        print(f"成本比率 (Cost Ratio):              {total_bottleneck_cost / total_migration_overhead:.2f}x")
        print()

        if should_trigger:
            print(f"✅ 全局觸發決策: 遷移 (Total D_bot {total_bottleneck_cost:.2f}s > Threshold {threshold_overhead:.2f}s)")
            print(f"📋 最終候選清單: {len(worthy_candidates)} 個 Subtask")
            return (True, worthy_candidates)
        else:
            print(f"❌ 全局觸發決策: 不遷移 (Total D_bot {total_bottleneck_cost:.2f}s ≤ Threshold {threshold_overhead:.2f}s)")
            print(f"⚠️ 成本效益不足，不建議執行遷移 (防止頻繁抖動)")
            return (False, [])

    def get_prioritized_list(self, filtered_ids):
        """
        Multi-dimensional prioritization: Calculate priority score using D_overload and R_impact
        Priority_Score = D_overload × (1 + R_impact)

        Args:
            filtered_ids: List of subtask_ids that passed two-tier evaluation

        Returns:
            List of (subtask_id, priority_score) tuples sorted by score descending
        """
        if not filtered_ids:
            return []

        # Get subtask locations for TM-level traffic calculation
        subtask_locations = self.get_subtask_locations()

        # Calculate TM-level total traffic
        tm_network_traffic = {}
        for subtask_id, info in self._task_info.items():
            actual_input_rate = info["actual_input_rate"]
            tm_resource_id = subtask_locations.get(subtask_id, "unknown")

            if tm_resource_id != "unknown" and actual_input_rate > 0:
                traffic_bytes = actual_input_rate * self.AVERAGE_RECORD_SIZE
                if tm_resource_id not in tm_network_traffic:
                    tm_network_traffic[tm_resource_id] = 0.0
                tm_network_traffic[tm_resource_id] += traffic_bytes

        # R_impact (Impact Range) mapping based on operator position in pipeline
        impact_range_map = {
            "Source": 0.0,       # Upstream operators have lower impact
            "Window_Max": 0.2,
            "Window_Join": 0.5,
            "Sink": 1.0          # Downstream operators have higher impact
        }

        prioritized_list = []

        print(f"\n🎯 多維度優先級計算 (Priority = D_overload × (1 + R_impact)):")
        print(f"{'Subtask ID':<30} {'Cause':<18} {'D_overload':<12} {'R_impact':<10} {'Priority':<10}")
        print("=" * 95)

        for subtask_id in filtered_ids:
            if subtask_id not in self._task_info:
                continue

            task_info = self._task_info[subtask_id]
            cause = task_info.get("bottleneck_cause", "UNKNOWN")
            actual_rate = task_info["actual_input_rate"]
            max_capacity = task_info["max_capacity"]
            task_name = task_info["task_name"]
            tm_resource_id = subtask_locations.get(subtask_id, "unknown")

            # Calculate D_overload (Overload Degree) based on bottleneck cause
            if cause == "CPU_BOTTLENECK":
                # For CPU bottlenecks: actual_rate / max_capacity
                if max_capacity > 0:
                    D_overload = actual_rate / max_capacity
                else:
                    D_overload = 1.0
            elif cause == "NETWORK_BOTTLENECK":
                # For Network bottlenecks: TM_total_traffic / bandwidth_limit
                tm_total_traffic = tm_network_traffic.get(tm_resource_id, 0)
                D_overload = tm_total_traffic / self.BANDWIDTH_LIMIT_BYTES_PER_SEC
            else:
                D_overload = 1.0

            # Determine R_impact (Impact Range) based on operator position in pipeline
            R_impact = 0.0
            for keyword, impact_value in impact_range_map.items():
                if keyword in task_name:
                    R_impact = impact_value
                    break

            # Calculate Priority Score: D_overload × (1 + R_impact)
            priority_score = D_overload * (1 + R_impact)

            prioritized_list.append((subtask_id, priority_score))

            # Display
            cause_display = cause.replace("_BOTTLENECK", "")
            print(f"{subtask_id:<30} {cause_display:<18} {D_overload:>10.3f}  {R_impact:>8.2f}  {priority_score:>8.3f}")

        # Sort by priority score descending
        prioritized_list.sort(key=lambda x: x[1], reverse=True)

        print(f"\n📊 優先級排序完成: {len(prioritized_list)} 個 Subtask")

        return prioritized_list

    def get_taskmanager_info(self):
        """
        查詢所有 TaskManager 的資訊和當前負載，包含 CPU 容量限制
        採用「Flink REST API」與「Prometheus」雙路並行方案：
        1. 動態發現：透過 Flink REST API 獲取所有已註冊的 TaskManager
        2. 負載獲取：透過 Prometheus 查詢實際負載 (busyTime)
        3. 靜態補全：使用 cpu_capacity_map 匹配 CPU 限制
        返回: { resource_id: {"host": "192.168.1.100", "current_load": 0.5, "cpu_limit": 1.5}, ... }
        """
        try:
            # CPU capacity mapping based on docker-compose.yml
            # Note: resourceId 格式可能是 tm-20c-1 或 tm_20c_1，需要兼容兩種格式
            cpu_capacity_map = {
                "tm_20c_1": 2.0,
                "tm_20c_2_net": 2.0,
                "tm_10c_3_cpu": 1.0,
                "tm_20c_4": 1.0
            }

            tm_info = {}

            # ===== Step 1: 動態發現 - 透過 Flink REST API 獲取所有 TaskManager =====
            print(f"🔍 步驟 1: 透過 Flink REST API 發現 TaskManager...")
            try:
                tm_list_response = requests.get(f"{self.flink_rest_url}/taskmanagers", timeout=5)
                if tm_list_response.status_code == 200:
                    tm_list_data = tm_list_response.json()

                    if 'taskmanagers' in tm_list_data:
                        print(f"   ✅ 發現 {len(tm_list_data['taskmanagers'])} 個 TaskManager")

                        for tm in tm_list_data['taskmanagers']:
                            tm_id = tm.get('id')

                            # 獲取詳細資訊
                            try:
                                detail_response = requests.get(f"{self.flink_rest_url}/taskmanagers/{tm_id}", timeout=3)
                                if detail_response.status_code == 200:
                                    detail_data = detail_response.json()

                                    # 提取 resourceId (優先) 或使用 tm_id
                                    resource_id = detail_data.get('resourceId') or tm_id

                                    # 正規化 resource_id: 將連字號轉換為底線 (tm-20c-1 -> tm_20c_1)
                                    resource_id = resource_id.replace('-', '_')

                                    # 提取 Host 資訊
                                    # 優先從 path 中解析 (格式: /192.168.1.100:xxxxx)
                                    # 或從 hardware 中獲取
                                    host = "unknown"
                                    if 'path' in detail_data:
                                        path = detail_data['path']
                                        # 解析格式 "/192.168.1.100:xxxxx" -> "192.168.1.100"
                                        if path.startswith('/'):
                                            host_part = path[1:].split(':')[0]
                                            # 處理 IPv4 格式中的下劃線 (192_168_1_100 -> 192.168.1.100)
                                            if '_' in host_part and not '.' in host_part:
                                                host = host_part.replace('_', '.')
                                            else:
                                                host = host_part

                                    # 如果從 path 無法取得，嘗試從 hardware.cpuCores 相關資訊推斷
                                    if host == "unknown" and 'hardware' in detail_data:
                                        # 某些部署可能會在 hardware 中包含 hostname
                                        host = detail_data.get('hardware', {}).get('hostname', resource_id)

                                    # 初始化 TM 資訊 (負載先設為 0.0，稍後由 Prometheus 更新)
                                    cpu_limit = cpu_capacity_map.get(resource_id, 1.0)  # 預設 1.0 CPU

                                    tm_info[resource_id] = {
                                        "host": host,
                                        "current_load": 0.0,  # 初始化為 0，稍後更新
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

            # ===== Step 2: 負載獲取 - 透過 Prometheus 查詢實際負載 =====
            print(f"\n📊 步驟 2: 透過 Prometheus 查詢 TaskManager 負載...")
            try:
                query = 'avg(flink_taskmanager_job_task_busyTimeMsPerSecond) by (resource_id, tm_id, host)'
                prom_response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query}, timeout=5)
                prom_data = prom_response.json()

                if prom_data['status'] == 'success' and prom_data['data']['result']:
                    print(f"   ✅ 從 Prometheus 獲取到 {len(prom_data['data']['result'])} 個 TM 負載資料")

                    for r in prom_data['data']['result']:
                        # 提取 resource_id (優先使用 resource_id，否則使用 tm_id)
                        resource_id = r['metric'].get('resource_id') or r['metric'].get('tm_id', 'unknown')

                        # 正規化 resource_id
                        resource_id = resource_id.replace('-', '_')

                        current_load = float(r['value'][1])

                        # 如果 REST API 已經發現該 TM，則更新負載
                        if resource_id in tm_info:
                            tm_info[resource_id]["current_load"] = current_load
                            print(f"      ✓ 更新 {resource_id} 負載: {current_load:.2f}ms")
                        else:
                            # 如果 REST API 沒發現，但 Prometheus 有資料（向後兼容）
                            host = r['metric'].get('host', 'unknown')

                            # 修正 IP 格式：如果包含下劃線，替換為點號
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

            # ===== Step 3: 靜態補全 - 確保 cpu_capacity_map 中的所有 TM 都被考慮 =====
            # (此步驟在 Step 1 中已處理，如果 REST API 回傳的 TM 不在 map 中，則使用預設值)

            # 最終統計
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

    def get_neighbors(self, subtask_id):
        """
        Identify upstream and downstream neighbors of a subtask based on pipeline topology

        Args:
            subtask_id: The subtask ID (e.g., "Window_Max_0")

        Returns:
            Tuple (upstream_task_names: set, downstream_task_names: set)
        """
        # Pipeline order: Source -> Window_Max -> Window_Join -> Sink
        target_order = ["Source", "Window_Max", "Window_Join", "Sink"]

        # Extract task name from subtask_id (remove subtask index)
        task_name = "_".join(subtask_id.rsplit("_", 1)[0].split("_"))

        # Find current position in pipeline
        current_pos = -1
        for i, keyword in enumerate(target_order):
            if keyword in task_name:
                current_pos = i
                break

        if current_pos == -1:
            return (set(), set())

        # Identify upstream (previous operators in pipeline)
        upstream_keywords = set()
        if current_pos > 0:
            upstream_keywords.add(target_order[current_pos - 1])

        # Identify downstream (next operators in pipeline)
        downstream_keywords = set()
        if current_pos < len(target_order) - 1:
            downstream_keywords.add(target_order[current_pos + 1])

        return (upstream_keywords, downstream_keywords)

    def generate_migration_plan(self, prioritized_list=None):
        """
        Heuristic Greedy Allocation Algorithm with:
        - Pending State: Remove bottleneck subtasks from migration_plan before allocation
        - Pre-deduction: Release resources from original TMs upfront
        - Dynamic weighting based on bottleneck cause (CPU vs Network)
        - Multi-factor scoring: CPU load, network bandwidth, topology affinity
        - Hard constraints: slot limit (6), CPU headroom (800ms), bandwidth limit
        - Immediate feedback: Update resource_map and migration_plan after each allocation
        - Fallback guarantee: Force return to original TM if no valid allocation found

        Args:
            prioritized_list: List of (subtask_id, priority_score) tuples sorted by score descending
        Returns:
            { "subtask_id": "target_resource_id", ... }
        """
        # Use bottlenecks from CAOM detection if no explicit list provided
        if prioritized_list is None:
            if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
                print("⚠️ 未檢測到瓶頸，無需產生遷移計畫")
                return None
            # Extract subtask IDs from CAOM detection results (backward compatibility)
            prioritized_list = [(subtask_id, 1.0) for subtask_id, _, _ in self._bottleneck_subtasks]

        tm_info = self.get_taskmanager_info()
        current_locations = self.get_subtask_locations()

        if not tm_info:
            print("⚠️ 無法獲取 TaskManager 資訊，無法產生遷移計畫")
            return None

        if not prioritized_list:
            print("⚠️ 未檢測到過載的 subtask，無需產生遷移計畫")
            return None

        # ===== Hard Constraints =====
        MAX_SLOTS_PER_TM = 6
        CPU_HEADROOM_LIMIT = 800  # ms (changed from 850 to 800 as per requirement)

        # ===== 1. Initialize Resource Map =====
        resource_map = {}
        for rid, info in tm_info.items():
            resource_map[rid] = {
                'slots': 0,
                'busy_time': info['current_load'],  # in ms
                'network_traffic': 0.0,  # in bytes/sec
                'cpu_limit': info['cpu_limit'],
                'host': info['host']
            }

        # ===== 2. Build Initial State: Count all subtasks (including those NOT in prioritized_list) =====
        for subtask_id, resource_id in current_locations.items():
            if resource_id in resource_map:
                resource_map[resource_id]['slots'] += 1

                # Calculate network traffic for this subtask
                if subtask_id in self._task_info:
                    actual_rate = self._task_info[subtask_id]['actual_input_rate']
                    traffic = actual_rate * self.AVERAGE_RECORD_SIZE
                    resource_map[resource_id]['network_traffic'] += traffic

        # ===== 3. Pre-deduction: Release resources from bottleneck subtasks' original TMs =====
        prioritized_subtask_ids = {subtask_id for subtask_id, _ in prioritized_list}

        print(f"\n🔄 預扣除階段：從原 TM 釋放瓶頸 Subtask 資源")
        for subtask_id in prioritized_subtask_ids:
            if subtask_id not in self._task_info:
                continue

            original_rid = current_locations.get(subtask_id, 'unknown')
            if original_rid not in resource_map:
                continue

            # Release resources from original TM
            resource_map[original_rid]['slots'] -= 1

            task_detail = self._task_info[subtask_id]
            busy_time_ms = task_detail['T_busy'] * 1000
            resource_map[original_rid]['busy_time'] -= busy_time_ms

            actual_rate = task_detail['actual_input_rate']
            traffic = actual_rate * self.AVERAGE_RECORD_SIZE
            resource_map[original_rid]['network_traffic'] -= traffic

            print(f"   ✓ 釋放 {subtask_id} 從 {original_rid}: "
                  f"-1 Slot, -{busy_time_ms:.0f}ms, -{traffic/(1024*1024):.2f}MB/s")

        print(f"\n📊 資源快照 (Pre-deduction 後可用資源):")
        for rid, res in resource_map.items():
            print(f"   {rid}: Slots={res['slots']}/{MAX_SLOTS_PER_TM}, "
                  f"BusyTime={res['busy_time']:.0f}ms, "
                  f"NetTraffic={res['network_traffic']/(1024*1024):.2f}MB/s")

        # ===== 4. Initialize Migration Plan with Pending State =====
        # Start with current locations for non-bottleneck subtasks
        migration_plan = {}
        for subtask_id, current_resource_id in current_locations.items():
            if subtask_id not in prioritized_subtask_ids:
                # Keep non-bottleneck subtasks in their current locations
                migration_plan[subtask_id] = current_resource_id
            # else: Leave bottleneck subtasks in pending state (not in migration_plan yet)

        # TODO: 找相對符合要球的TM 而非追求極致的改善
        print(f"\n🎯 啟發式貪婪分配 (待定狀態 + 動態權重 + 多因子評分)")
        print(f"   待分配瓶頸 Subtask: {len(prioritized_subtask_ids)} 個")

        # ===== 5. Greedy Allocation Loop =====
        for subtask_id, priority_score in prioritized_list:
            # Get task info if available
            if not hasattr(self, '_task_info') or subtask_id not in self._task_info:
                print(f"⚠️ {subtask_id} 不在 task_info 中，跳過")
                continue

            task_detail = self._task_info[subtask_id]
            actual_rate = task_detail['actual_input_rate']
            max_capacity = task_detail['max_capacity']
            busy_time_sec = task_detail['T_busy']
            busy_time_ms = busy_time_sec * 1000
            state_size = task_detail.get('state_size', 0)
            cause = task_detail.get('bottleneck_cause', 'CPU_BOTTLENECK')

            original_rid = current_locations.get(subtask_id, 'unknown')

            # Calculate subtask's resource requirements
            subtask_traffic = actual_rate * self.AVERAGE_RECORD_SIZE

            # ===== 6. Dynamic Weighting by Bottleneck Cause =====
            if cause == "CPU_BOTTLENECK":
                W_cpu, W_net, W_topo = 0.6, 0.2, 0.2
            elif cause == "NETWORK_BOTTLENECK":
                W_cpu, W_net, W_topo = 0.1, 0.5, 0.4
            else:
                W_cpu, W_net, W_topo = 0.4, 0.3, 0.3

            # Get neighbors for topology scoring
            upstream_keywords, downstream_keywords = self.get_neighbors(subtask_id)

            # ===== 7. Evaluate all TMs and find best match =====
            best_tm = None
            best_score = -float('inf')
            scores_log = []

            for rid, res in resource_map.items():
                # ===== 8. Hard Constraints Guard =====
                # Projected resources after adding this subtask
                projected_slots = res['slots'] + 1
                projected_busy = res['busy_time'] + busy_time_ms
                projected_traffic = res['network_traffic'] + subtask_traffic

                # Check hard constraints
                if projected_slots > MAX_SLOTS_PER_TM:
                    continue  # Violates slot limit
                if projected_busy > CPU_HEADROOM_LIMIT:
                    continue  # Violates CPU headroom
                if projected_traffic > self.BANDWIDTH_LIMIT_BYTES_PER_SEC:
                    continue  # Violates bandwidth limit

                # ===== 9. Multi-Factor Scoring =====
                # CPU Score: (1 - projected_busy / 1000)
                cpu_score = max(0, 1 - (projected_busy / 1000.0))

                # Net Score: (1 - projected_traffic / bandwidth_limit)
                net_score = max(0, 1 - (projected_traffic / self.BANDWIDTH_LIMIT_BYTES_PER_SEC))

                # Topo Score: Communication Affinity (based on current migration_plan)
                topo_score = 0.0
                # Check if this TM hosts upstream or downstream neighbors
                for other_subtask_id, other_rid in migration_plan.items():
                    if other_rid == rid and other_subtask_id != subtask_id:
                        # Check if other_subtask is a neighbor
                        if other_subtask_id in self._task_info:
                            other_task_name = self._task_info[other_subtask_id]['task_name']
                            # Check if upstream neighbor
                            for upstream_kw in upstream_keywords:
                                if upstream_kw in other_task_name:
                                    topo_score += 1.0
                            # Check if downstream neighbor
                            for downstream_kw in downstream_keywords:
                                if downstream_kw in other_task_name:
                                    topo_score += 1.0

                # Calculate final score
                final_score = (W_cpu * cpu_score) + (W_net * net_score) + (W_topo * topo_score)

                scores_log.append((rid, final_score, cpu_score, net_score, topo_score))

                if final_score > best_score:
                    best_score = final_score
                    best_tm = rid

            # ===== 10. Allocation Decision & Immediate Feedback Update =====
            if best_tm:
                # Success: Assign to best TM
                migration_plan[subtask_id] = best_tm

                # Immediately update Resource Map
                resource_map[best_tm]['slots'] += 1
                resource_map[best_tm]['busy_time'] += busy_time_ms
                resource_map[best_tm]['network_traffic'] += subtask_traffic

                # Display decision
                state_size_mb = state_size / (1024 * 1024) if state_size > 0 else 0
                overload_pct = ((actual_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0

                print(f"\n✅ 分配成功 (優先級={priority_score:.3f}, {cause.replace('_BOTTLENECK', '')}): {subtask_id}")
                print(f"   從: {original_rid} -> 到: {best_tm}")
                print(f"   權重: W_cpu={W_cpu}, W_net={W_net}, W_topo={W_topo}")
                print(f"   最佳得分: {best_score:.3f} (CPU:{scores_log[[s[0] for s in scores_log].index(best_tm)][2]:.2f}, "
                      f"Net:{scores_log[[s[0] for s in scores_log].index(best_tm)][3]:.2f}, "
                      f"Topo:{scores_log[[s[0] for s in scores_log].index(best_tm)][4]:.2f})")
                print(f"   新狀態: Slots={resource_map[best_tm]['slots']}/{MAX_SLOTS_PER_TM}, "
                      f"BusyTime={resource_map[best_tm]['busy_time']:.0f}/{CPU_HEADROOM_LIMIT}ms, "
                      f"Traffic={resource_map[best_tm]['network_traffic']/(1024*1024):.2f}MB/s")
                if state_size > 0:
                    print(f"   💾 State: {format_bytes(state_size)}")
            else:
                # ===== 11. Fallback Guarantee: Force return to original TM =====
                print(f"\n⚠️ 無可用 TM: {subtask_id} (所有 TM 違反硬約束)")
                print(f"   🔄 強制回歸原位: {original_rid}")

                # Force allocation back to original TM
                migration_plan[subtask_id] = original_rid

                # Update resource_map to reflect this forced allocation
                # (Prevent subsequent subtasks from over-allocating)
                if original_rid in resource_map:
                    resource_map[original_rid]['slots'] += 1
                    resource_map[original_rid]['busy_time'] += busy_time_ms
                    resource_map[original_rid]['network_traffic'] += subtask_traffic

                    print(f"   ⚠️ 原 TM 資源更新 (防止後續過度分配): "
                          f"Slots={resource_map[original_rid]['slots']}/{MAX_SLOTS_PER_TM}, "
                          f"BusyTime={resource_map[original_rid]['busy_time']:.0f}ms")

        # ===== 12. Final Validation & Summary =====
        print(f"\n📊 最終資源分配驗證:")
        validation_errors = []
        for rid, res in resource_map.items():
            slots_ok = res['slots'] <= MAX_SLOTS_PER_TM
            cpu_ok = res['busy_time'] <= CPU_HEADROOM_LIMIT
            net_ok = res['network_traffic'] <= self.BANDWIDTH_LIMIT_BYTES_PER_SEC

            status = "✅" if (slots_ok and cpu_ok and net_ok) else "❌"

            print(f"   {status} {rid}: Slots={res['slots']}/{MAX_SLOTS_PER_TM}, "
                  f"BusyTime={res['busy_time']:.0f}/{CPU_HEADROOM_LIMIT}ms, "
                  f"Traffic={res['network_traffic']/(1024*1024):.2f}/6.25MB/s")

            if not slots_ok:
                validation_errors.append(f"{rid}: Slot 超限 ({res['slots']} > {MAX_SLOTS_PER_TM})")
            if not cpu_ok:
                validation_errors.append(f"{rid}: CPU 超限 ({res['busy_time']:.0f}ms > {CPU_HEADROOM_LIMIT}ms)")
            if not net_ok:
                validation_errors.append(f"{rid}: 頻寬超限 ({res['network_traffic']/(1024*1024):.2f}MB/s > 6.25MB/s)")

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
    def wait_for_job_termination(self, job_id, max_wait_sec=30):
        """
        [優化點] 輪詢 REST API 確保舊 Job 已完全停止並釋放 Slot。
        這取代了硬編碼的 time.sleep(5)，能顯著降低總中斷時間。
        """
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
            time.sleep(0.5) # 每 0.5 秒檢查一次
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

        # === 紀錄詳細中斷時間至 CSV (用於實驗分析) ===
        log_file = "/home/yenwei/research/structure_setup/output/propose_migration_performance.csv"
        file_exists = os.path.isfile(log_file)
        with open(log_file, "a", newline="") as f:
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
        Two-Tier Migration Workflow:
        1. detect_bottleneck (CAOM with hierarchical diagnosis)
        2. evaluate_migration_trigger (Two-tier: subtask-level + job-level)
        3. get_prioritized_list (D_overload × (1 + R_impact))
        4. generate_migration_plan (Greedy allocation with 6-slot limit)
        """
        # Step 1: Run CAOM bottleneck detection with hierarchical diagnosis
        print("=" * 100)
        print("STEP 1: CAOM 瓶頸檢測 (階層式診斷: CPU vs Network)")
        print("=" * 100)
        reports = self.detect_bottleneck()

        if not reports:
            print("⚠️ 無法獲取監控數據")
            return False

        # Check if any bottlenecks were detected
        if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
            print("✅ 未檢測到瓶頸，系統運行正常")
            return False

        print(f"\n🔥 CAOM 檢測到 {len(self._bottleneck_subtasks)} 個瓶頸 Subtask:")
        for subtask_id, actual_rate, max_capacity in self._bottleneck_subtasks:
            overload_pct = ((actual_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0

            # Get bottleneck cause from task_info
            cause = self._task_info[subtask_id].get("bottleneck_cause", "UNKNOWN")
            T_busy = self._task_info[subtask_id]["T_busy"]
            T_bp = self._task_info[subtask_id]["T_bp"]

            # Display different icons and reasons based on cause
            if cause == "CPU_BOTTLENECK":
                icon = "🔥"
                reason = f"CPU過載 (T_busy={T_busy*1000:.0f}ms > 800ms)"
            elif cause == "NETWORK_BOTTLENECK":
                icon = "🌐"
                reason = f"網路頻寬受限 (T_busy={T_busy*1000:.0f}ms, T_bp={T_bp*1000:.0f}ms, TM頻寬已飽和)"
            else:
                icon = "⚠️"
                reason = "原因未知"

            print(f"   {icon} {subtask_id}: 實際輸入 {actual_rate:.2f} rec/s > 最大容量 {max_capacity:.2f} rec/s (過載 {overload_pct:.1f}%)")
            print(f"      原因: {reason}")

        # Step 2: Two-tier migration trigger evaluation
        print("\n" + "=" * 100)
        print("STEP 2: 兩階段遷移觸發評估")
        print("=" * 100)
        should_trigger, final_candidates = self.evaluate_migration_trigger(self._bottleneck_subtasks)

        if not should_trigger:
            print("\n⚠️ 全局觸發條件未滿足，不執行遷移")
            return False

        # Step 3: Multi-dimensional prioritization
        print("\n" + "=" * 100)
        print("STEP 3: 多維度優先級排序")
        print("=" * 100)
        prioritized_list = self.get_prioritized_list(final_candidates)

        if not prioritized_list:
            print("⚠️ 優先級列表為空")
            return False

        # Step 4: Generate migration plan using greedy allocation
        print("\n" + "=" * 100)
        print("STEP 4: 貪婪分配遷移計畫 (6-slot limit, Normalized Load)")
        print("=" * 100)
        migration_plan = self.generate_migration_plan(prioritized_list)

        if not migration_plan:
            return False

        # Write migration plan to JSON
        if not self.write_migration_plan(migration_plan):
            return False

        # Optionally trigger migration
        return self.trigger_migration(migration_plan)

        print("\n✅ 遷移計畫已生成並寫入檔案")
        return True