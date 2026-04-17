import requests
# import numpy as np
import csv
import json
import time
import os
import subprocess

"""
加入 topology affinity 函式 forward 的綁在一起
分成顯性瓶頸和隱性瓶頸 
顯性用 Z-score 異常檢測法
隱性用 待修正
後面和 v3 相同
"""

log_file = "/home/yenwei/research/structure_setup/output/t13_6/subtask_metrics_history.csv"
detail_log = "/home/yenwei/research/structure_setup/output/t13_6/migration_details.csv"

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
    import re

    def normalize(s):
        """統一將連字號、空格都轉成底線，並折疊多個底線，轉小寫"""
        s = s.replace('-', '_').replace(' ', '_')
        s = re.sub(r'_+', '_', s)
        return s.lower().strip('_')

    norm_task = normalize(task_name)

    # 清理 vertex_name，移除括號內容
    clean_vertex = vertex_name.split('(')[0].strip() if '(' in vertex_name else vertex_name

    # 處理包含箭頭的情況 (例如: "Window-Max -> Map")
    if '->' in vertex_name:
        parts = [p.strip().split('(')[0].strip() for p in vertex_name.split('->')]
        norm_parts = [normalize(p) for p in parts if p.strip()]

        # 所有片段都能在 task_name 中找到即視為匹配
        if all(p and p in norm_task for p in norm_parts):
            return True

    # 簡單匹配：normalize 後比較
    norm_vertex = normalize(clean_vertex)
    if norm_vertex and norm_vertex in norm_task:
        return True

    # 反向檢查
    norm_task_readable = normalize(task_name.replace('_', ' '))
    if norm_vertex and norm_vertex in norm_task_readable:
        return True

    return False

class FlinkPropose:

    AVERAGE_RECORD_SIZE = 100  # bytes per record (default assumption 確實一筆資料大約 100 byte)
    SOURCE_MAX_TPS = 12_000_000  # bytes/s，全局 Source 總速率上限，防止 busyTime 極小時速率失真

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
        self._bottleneck_subtasks = []  # bottleneck detection results
        self._task_info = {}  # Task information from detection

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
        self.tm_bandwidth_map = {      # (單位 Bytes/sec)
            "tm_20c_1": 31250000,      # 250mbit
            "tm_20c_2_net": 18750000,  # 100mbit
            "tm_10c_3_cpu": 31250000,  # 250mbit
            "tm_20c_4": 18750000       # 100mbit
        }
        self.default_bandwidth = 31250000 # 預設值

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
        # 除了 In，也要抓取 Out 指標 (sink 用 in , 其他用out  )
        rate_in_map = self.query_metric_by_task('flink_taskmanager_job_task_numBytesInPerSecond')
        rate_out_map = self.query_metric_by_task('flink_taskmanager_job_task_numBytesOutPerSecond')
        outPoolUsage = self.query_metric_by_task('flink_taskmanager_job_task_buffers_outPoolUsage')
        inPoolUsage = self.query_metric_by_task('flink_taskmanager_job_task_buffers_inPoolUsage')
        # 輸入輸出資料

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


            for idx in subtasks_busy.keys():
                T_busy = subtasks_busy.get(idx, 0) / 1000.0  # Convert to seconds
                T_bp = subtasks_bp.get(idx, 0) / 1000.0
                T_idle = subtasks_idle.get(idx, 0) / 1000.0
                # 邏輯：如果是 Source 算子，優先使用 numRecordsOut；否則使用 numRecordsIn

                # 取得真實觀測到的雙向流量
                obs_in = rate_in_map.get(task_name, {}).get(idx, 0)
                obs_out = rate_out_map.get(task_name, {}).get(idx, 0)

                # 決定用於「處理量計算」的主指標 (Source 節點看 Out，其餘看 In)
                primary_observed_rate = obs_out if "source" in task_name.lower() else obs_in

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

                out_pool = outPoolUsage.get(task_name, {}).get(idx, 0.0)
                in_pool  = inPoolUsage.get(task_name, {}).get(idx, 0.0)

                task_info[subtask_id] = {
                    "task_name": task_name,
                    "subtask_index": idx,
                    "T_busy": T_busy,
                    "T_bp": T_bp,
                    "T_idle": T_idle,
                    "observed_input_rate": obs_in,   # 真實輸入 Byte/s
                    "observed_output_rate": obs_out, # 真實輸出 Byte/s
                    "observed_rate": primary_observed_rate,
                    "actual_input_rate": 0.0,
                    "max_capacity": 0.0,
                    "is_bottleneck": False,
                    "bottleneck_cause": None,
                    "state_size": state_size,
                    "out_pool_usage": out_pool,
                    "in_pool_usage": in_pool,
                }

        # Step A: Recover actual source rate 由 source 開始 其他用 BFS 搭配 out/in 推算
        # Identify source operators (those with "Source" in name)
        source_tasks = {k: v for k, v in task_info.items() if "Source" in v["task_name"]}

        for subtask_id, info in source_tasks.items():
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            observed_rate = info["observed_rate"]

            if T_busy > 0:
                # λ̂_Source = λ_Source × (1 + T_bp / T_busy)
                print(f"  Source: {subtask_id} T_busy = {T_busy} T_bp = {T_bp} observed_rate = {observed_rate}")
                T_busy = max(T_busy, 0.15)  # 保底 150ms，防止分母過小導致速率爆炸
                actual_source_rate = observed_rate * (1 + T_bp / T_busy)
                info["actual_input_rate"] = actual_source_rate
                print(f"  還原速率 Source: {subtask_id} actual_input_rate = {actual_source_rate:.0f}, T_busy = {T_busy}")
            else:
                info["actual_input_rate"] = observed_rate
        """
        # Step A.1: Proportional Capping — 防止 busyTime 極小時 Source 速率失真
        total_source_rate = sum(info["actual_input_rate"] for info in source_tasks.values())
        if total_source_rate > self.SOURCE_MAX_TPS and total_source_rate > 0:
            scale = self.SOURCE_MAX_TPS / total_source_rate
            print(f"\n  [Proportional Capping] 還原總速率 {total_source_rate/1e6:.2f} MB/s > 上限 {self.SOURCE_MAX_TPS/1e6:.2f} MB/s，縮放比例 = {scale:.4f}")
            for subtask_id, info in source_tasks.items():
                before = info["actual_input_rate"]
                info["actual_input_rate"] *= scale
                print(f"    {subtask_id}: {before:.0f} -> {info['actual_input_rate']:.0f} bytes/s")
        """
        # Step B: Recover actual input rate for all operators using BFS
        # Build adjacency list based on typical Flink pipeline order
        target_order = ["Source", "Window_Max", "Window_Join", "Sink"]
        #q5
        #target_order = ["Source", "Window_Auction", "Window_Max", "Sink"]

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
        # TODO : BFS 也要設防爆門檻 ratio 嗎？
        # TODO : 「應該不用」建立「多對多」的拓撲視圖 ，針對多輸入算子（如 Window_Join 同時接收來自兩個算子流的情況），目前的公式確實會失效，因為它假設了「一對一」的上下游關係。

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
                    # 計算每個subtask的 actual input rate, 一樣是用operator的輸入輸出比例執行BFS 但依照本operator的各個subtask本身比例 分配計算後的輸入速率
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

        # ── STEP 0: 全域指標正則化 (Z-score Normalization) ─────────────────
        Z_THRESHOLD = 1.0    # 1-sigma: 前 ~16% 最忙碌視為統計顯著
        SIGMA_MIN   = 0.001  # σ 過小代表負載均勻，停用 Z-score 判斷以防抖動

        all_T_busy  = [info["T_busy"]        for info in task_info.values()]
        all_pool_out = [info["out_pool_usage"] for info in task_info.values()]

        mu_busy  = sum(all_T_busy)   / len(all_T_busy)   if all_T_busy   else 0.0
        mu_pool  = sum(all_pool_out) / len(all_pool_out) if all_pool_out else 0.0

        def _std(values, mean_val):
            if len(values) < 2:
                return 0.0
            return (sum((x - mean_val) ** 2 for x in values) / len(values)) ** 0.5

        sigma_busy = _std(all_T_busy,   mu_busy)
        sigma_pool = _std(all_pool_out, mu_pool)

        print(f"\n📊 [STEP 0] Z-score 正則化: "
              f"μ_busy={mu_busy*1000:.1f}ms σ_busy={sigma_busy*1000:.1f}ms | "
              f"μ_pool={mu_pool:.3f} σ_pool={sigma_pool:.3f}")
        if sigma_busy < SIGMA_MIN:
            print("   ⚠️ σ_busy 過小 (負載均勻)，CPU Z-score 判斷停用")
        if sigma_pool < SIGMA_MIN:
            print("   ⚠️ σ_pool 過小，Network Z-score 判斷停用")

        # TM-level network saturation (needed for explicit network detection)
        subtask_locations = self.get_subtask_locations()

        tm_network_traffic = {}
        for subtask_id, info in task_info.items():
            tm_id = subtask_locations.get(subtask_id, "unknown")
            if tm_id != "unknown":
                if tm_id not in tm_network_traffic:
                    tm_network_traffic[tm_id] = {"in": 0.0, "out": 0.0}
                tm_network_traffic[tm_id]["in"]  += info["observed_input_rate"]
                tm_network_traffic[tm_id]["out"] += info["observed_output_rate"]

        tm_out_saturated = {}
        print(f"\n📊 [真實 Byte 輸出監測] TaskManager 網路狀態:")
        for tm_id, traffic in tm_network_traffic.items():
            limit = self.tm_bandwidth_map.get(tm_id, self.default_bandwidth)
            tm_out_saturated[tm_id] = (traffic["out"] >= limit * 0.95)
            print(f"   {tm_id}: {traffic['out']/1e6:.2f}MB/s "
                  f"{'🔴' if tm_out_saturated[tm_id] else '🟢'} (Limit: {limit/1e6:.1f}MB/s)")

        # ── STEP 1: 顯性瓶頸偵測 (Explicit Root Cause Identification) ────────
        # CPU 顯性: Z_busy > threshold 且 T_busy > T_bp  (源頭，非受害者)
        # Net 顯性: Z_pool > threshold 且 TM 頻寬飽和
        Priority_1_Migration_List = []  # [(subtask_id, actual_rate, max_capacity)]

        print(f"\n🔍 [STEP 1] 顯性瓶頸偵測 (Z-score + 因果判定):")
        for subtask_id, info in task_info.items():
            T_busy  = info["T_busy"]
            T_bp    = info["T_bp"]
            out_pool = info["out_pool_usage"]
            tm_id   = subtask_locations.get(subtask_id, "unknown")

            Z_busy = (T_busy  - mu_busy)  / sigma_busy if sigma_busy >= SIGMA_MIN else 0.0
            Z_pool = (out_pool - mu_pool) / sigma_pool if sigma_pool >= SIGMA_MIN else 0.0
            info["Z_busy"] = Z_busy
            info["Z_pool"] = Z_pool

            # 運算顯性瓶頸: 統計顯著 (Z > 1.0) + 因果判定 (T_busy > T_bp)
            if Z_busy > Z_THRESHOLD and T_busy > T_bp:
                info["is_bottleneck"]    = True
                info["bottleneck_cause"] = "CPU_BOTTLENECK"
                Priority_1_Migration_List.append(
                    (subtask_id, info["actual_input_rate"], info["max_capacity"]))
                print(f"  [CPU P1] {subtask_id}: Z_busy={Z_busy:.2f}, "
                      f"T_busy={T_busy*1000:.0f}ms > T_bp={T_bp*1000:.0f}ms")
                continue

            # 網路顯性瓶頸: Z_pool 統計顯著 and TM 頻寬飽和
            if Z_pool > Z_THRESHOLD and tm_out_saturated.get(tm_id, False):
                info["is_bottleneck"]    = True
                info["bottleneck_cause"] = "NETWORK_BOTTLENECK"
                Priority_1_Migration_List.append(
                    (subtask_id, info["actual_input_rate"], info["max_capacity"]))
                print(f"  [NET P1] {subtask_id}: Z_pool={Z_pool:.2f}, "
                      f"TM {tm_id} 頻寬飽和")
                continue

                # 網路顯性瓶頸: Z_pool 統計顯著
            if Z_pool > Z_THRESHOLD :
                info["is_bottleneck"]    = True
                info["bottleneck_cause"] = "NETWORK_BOTTLENECK"
                Priority_1_Migration_List.append(
                    (subtask_id, info["actual_input_rate"], info["max_capacity"]))
                print(f"  [NET P1] {subtask_id}: Z_pool={Z_pool:.2f}, "
                      f"TM {tm_id} outpool滿了")
                continue

        # ── STEP 2: 隱性瓶頸偵測 (Latent Bottleneck Prediction) ─────────────
        # 篩選受害者 (T_bp > T_busy)，再以產能還原公式判斷是否會成為顯性瓶頸:
        #   λ_req = actual_input_rate  (BFS 還原後的上游實際需求)
        #   λ_max = max_capacity       = observed_rate × (1 + (T_bp+T_idle)/T_busy)
        #   若 λ_req > λ_max → 修好下游後此算子立刻成為新瓶頸
        Priority_2_Migration_List = []  # [(subtask_id, lambda_req, lambda_max)]

        print(f"\n🔮 [STEP 2] 隱性瓶頸偵測 (產能還原預測):")
        for subtask_id, info in task_info.items():
            if info["is_bottleneck"]:
                continue  # 已列入 P1，跳過

            T_busy = info["T_busy"]
            T_bp   = info["T_bp"]

            if T_bp > T_busy and T_busy > 0:
                lambda_req = info["actual_input_rate"]
                lambda_max = info["max_capacity"]

                if lambda_req > lambda_max and lambda_max > 0:
                    info["bottleneck_cause"] = "CPU_BOTTLENECK"
                    Priority_2_Migration_List.append((subtask_id, lambda_req, lambda_max))
                    print(f"  [LAT P2] {subtask_id}: T_bp={T_bp*1000:.0f}ms > T_busy={T_busy*1000:.0f}ms, "
                          f"λ_req={lambda_req:.0f} > λ_max={lambda_max:.0f}")
                else:
                    info["bottleneck_cause"] = "BACKPRESSURE_VICTIM"

        # ── 彙整 Migration Lists ─────────────────────────────────────────────
        bottleneck_subtasks = Priority_1_Migration_List  # 向下相容

        p1_id_set = {sid for sid, _, _ in Priority_1_Migration_List}
        p2_id_set = {sid for sid, _, _ in Priority_2_Migration_List}

        # ── Generate report (format unchanged, add P1/P2 per operator) ───────
        report_list = []
        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]

            bottleneck_count         = sum(1 for st in subtasks if task_info[st]["is_bottleneck"])
            cpu_bottleneck_count     = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") == "CPU_BOTTLENECK")
            network_bottleneck_count = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") == "NETWORK_BOTTLENECK")
            victim_count             = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") == "BACKPRESSURE_VICTIM")
            latent_bottleneck_count  = sum(1 for st in subtasks if task_info[st].get("bottleneck_cause") == "LATENT_BOTTLENECK")

            avg_actual_rate  = sum(task_info[st]["actual_input_rate"] for st in subtasks) / len(subtasks)
            avg_max_capacity = sum(task_info[st]["max_capacity"]       for st in subtasks) / len(subtasks)
            max_busy = max(task_info[st]["T_busy"] * 1000 for st in subtasks)
            max_bp   = max(task_info[st]["T_bp"]   * 1000 for st in subtasks)

            if bottleneck_count > 0:
                status = "🔴 BOTTLENECK"
            elif latent_bottleneck_count > 0:
                status = "🟠 LATENT_BOTTLENECK"
            elif max_busy > 700:
                status = "🟠 HIGH_LOAD"
            elif max_bp > 500:
                status = "🟡 BACKPRESSURED"
            else:
                status = "🟢 NORMAL"

            op_p1 = [st for st in subtasks if st in p1_id_set]
            op_p2 = [st for st in subtasks if st in p2_id_set]

            report_list.append({
                "task_name":               op_name,
                "status":                  status,
                "bottleneck_count":        bottleneck_count,
                "cpu_bottleneck_count":    cpu_bottleneck_count,
                "network_bottleneck_count": network_bottleneck_count,
                "victim_count":            victim_count,
                "latent_bottleneck_count": latent_bottleneck_count,
                "avg_actual_rate":         round(avg_actual_rate,  2),
                "avg_max_capacity":        round(avg_max_capacity, 2),
                "max_busy":                round(max_busy, 1),
                "max_bp":                  round(max_bp,   1),
                "subtasks":                subtasks,
                "Priority_1_Migration_List": op_p1,
                "Priority_2_Migration_List": op_p2,
            })

        # Store for downstream functions
        self._bottleneck_subtasks   = bottleneck_subtasks
        self._task_info             = task_info
        self._priority_1_list       = Priority_1_Migration_List
        self._priority_2_list       = Priority_2_Migration_List

        # --- 紀錄所有 Subtask 的歷史指標 ---
        file_exists = os.path.isfile(log_file)
        curr_time = time.time()

        with open(log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp", "subtask_id", "tm_id", "observed_rate",
                    "actual_input_rate", "busy_rate", "bp_rate", "idle_rate",
                    "is_bottleneck", "cause"
                ])

            for sid, info in task_info.items():
                writer.writerow([
                    curr_time,
                    sid,
                    subtask_locations.get(sid, "unknown"),
                    round(info["observed_rate"],       2),
                    round(info["actual_input_rate"],   2),
                    round(info["T_busy"],  3),
                    round(info["T_bp"],    3),
                    round(info["T_idle"],  3),
                    info["is_bottleneck"],
                    info.get("bottleneck_cause", "NONE")
                ])

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

        # Get subtask locations once for TM bandwidth lookup
        subtask_locations = self.get_subtask_locations()

        # ========== Phase 1: Subtask-Level Evaluation ==========
        print(f"\n🔍 階段 1: Subtask 級別評估 (個別成本效益)")
        print(f"{'Subtask ID':<60} {'State(MB)':<12} {'BW(MB/s)':<10} {'D_mig':<10} {'D_bot':<10} {'Decision':<15}")
        print("=" * 100)

        worthy_candidates = []
        candidate_details = []  # Store for Phase 2

        for subtask_id, actual_rate, max_capacity in bottleneck_ids:
            # Get task info
            if subtask_id not in self._task_info:
                continue

            task_info = self._task_info[subtask_id]
            state_size = task_info.get('state_size', 0)
            state_size_mb = state_size / (1024 * 1024)

            # Use the source TM's upload bandwidth for migration cost
            tm_id = subtask_locations.get(subtask_id, "unknown")
            tm_bandwidth = self.tm_bandwidth_map.get(tm_id, self.default_bandwidth)  # bytes/s
            tm_bandwidth_mb = tm_bandwidth / (1024 * 1024)

            # Calculate Individual Migration Cost: D_mig = state_size / TM_bandwidth
            D_mig = (state_size / tm_bandwidth) if tm_bandwidth > 0 else 0

            # Calculate Individual Bottleneck Cost: D_bot = overload_ratio × 30
            if max_capacity > 0:
                overload_ratio = (actual_rate - max_capacity) / max_capacity
                D_bot = overload_ratio * 30
            else:
                overload_ratio = 0
                D_bot = 0

            # Phase 1 Decision: Keep only if D_bot > D_mig
            individual_worthy = D_bot > D_mig
            # ！！！ 先去掉subtask級別評估
            #individual_worthy = D_bot > 0
            decision = "✅ KEEP" if individual_worthy else "❌ SKIP"

            print(f"{subtask_id:<60} {state_size_mb:>10.2f}  {tm_bandwidth_mb:>8.2f}  {D_mig:>8.2f}  {D_bot:>8.2f}  {decision:<15}")

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

        # TODO : 瓶頸造成的延遲 是全部加總嗎？ 還是取最大值？
        # Calculate Total Bottleneck Cost: Sum of all D_bot
        total_bottleneck_cost = sum(c['D_bot'] for c in candidate_details)

        # Calculate Total Migration Overhead:
        # Shared fixed cost (4s) + max recovery time (max state_size / TM_bandwidth)
        # Use the worst-case D_mig (slowest bandwidth among candidates) for the shared overhead
        max_recovery_time = max(c['D_mig'] for c in candidate_details)
        max_state_size = max(c['state_size'] for c in candidate_details)
        total_migration_overhead = 4 + max_recovery_time

        # Apply 20% buffer to prevent thrashing
        threshold_overhead = total_migration_overhead * 1.2

        # Global Trigger Decision
        should_trigger = total_bottleneck_cost > threshold_overhead

        print(f"總瓶頸成本 (Total D_bot):           {total_bottleneck_cost:.2f}s")
        print(f"總遷移開銷 (Shared Overhead):       {total_migration_overhead:.2f}s")
        print(f"  ├─ 固定成本 (Fixed):               4.00s")
        print(f"  └─ 最大恢復時間 (Max Recovery):    {max_recovery_time:.2f}s (MaxState: {max_state_size / (1024*1024):.2f} MB, TM BW-limited)")
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
                traffic_bytes = actual_input_rate
                if tm_resource_id not in tm_network_traffic:
                    tm_network_traffic[tm_resource_id] = 0.0
                tm_network_traffic[tm_resource_id] += traffic_bytes

        # R_impact (Impact Range) mapping based on operator position in pipeline

        impact_range_map = {
            "Source": 0.0,       # Upstream operators have lower impact
            "Window_Max": 0.25,
            "Window_Join": 0.5,
            "Sink": 0.75          # Downstream operators have higher impact
        }
        """
        impact_range_map = {
            "Source": 0.0,       # Upstream operators have lower impact
            "Window_Auction": 0.25,
            "Window_Max": 0.5,
            "Sink": 0.75          # Downstream operators have higher impact
        }
        """
        prioritized_list = []

        print(f"\n🎯 多維度優先級計算 (Priority = D_overload × (1 + R_impact)):")
        print(f"{'Subtask ID':<60} {'Cause':<18} {'D_overload':<12} {'R_impact':<10} {'Priority':<10}")
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
                limit = self.tm_bandwidth_map.get(tm_resource_id, self.default_bandwidth)
                D_overload = tm_total_traffic / limit
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
                "tm_20c_4": 2.0
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
                # TODO:  busy rate 都蠻低的 500以下 why?
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

    def print_subtask_status(self):
        """
        查詢並列印每個 subtask 當前的監控指標：
        busy rate, bp rate, idle rate, observed input rate (bytes/s), 所在 TaskManager
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

        col_w = [65, 10, 10, 10, 12, 10, 11, 20]
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

        # 依 task_name 排序輸出，同一 task 內依 index 排序
        for task_name in sorted(busy_map.keys()):
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
        """
        Identify upstream and downstream neighbors of a subtask based on pipeline topology

        Args:
            subtask_id: The subtask ID (e.g., "Window_Max_0")

        Returns:
            Tuple (upstream_task_names: set, downstream_task_names: set)
        """
        # Pipeline order: Source -> Window_Max -> Window_Join -> Sink
        target_order = ["Source", "Window_Max", "Window_Join", "Sink"]
        #target_order = ["Source", "Window_Auction", "Window_Max", "Sink"]

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

    def calculate_topology_affinity(self, bottleneck_subtask_id, resource_map, current_locations):
        """
        拓撲感知資料引力親和性演算法 (Topology-Aware Data Gravity Affinity)

        針對 Network 瓶頸的遷移目標選擇：
        將瓶頸 Subtask 遷到「能把最多 Cross-TM 傳輸轉為 Local Exchange」的目標 TM，
        以最大化記憶體內傳輸、最小化實體網路頻寬消耗。

        支援兩種 Flink 傳輸策略：
          - FORWARD (1-to-1)：上游 Subtask i 的 100% 輸出只送給下游 Subtask i，
                              直接賦予下游 Subtask i 所在 TM 滿分引力權重，無需比例推算。
          - HASH / REBALANCE (M-to-N)：上游輸出按流量比例分散至所有下游 Subtask，
                              以 actual_input_rate 佔比推算每條 Edge 的引力權重。

        Args:
            bottleneck_subtask_id : 待遷移的瓶頸 Subtask ID（如 "Window_Join_0"）
            resource_map          : 目前資源快照
                                    { rid: {'slots', 'busy_time', 'network_traffic', 'cpu_limit'} }
            current_locations     : 目前每個 Subtask 所在的 TM { subtask_id: resource_id }

        Returns:
            (best_tm_id, affinity_scores)
            best_tm_id      : 通過網路約束且親和力最高的目標 TM；
                              若無法找到有效 TM 則回傳 None，由呼叫端觸發 CPU 退回機制。
            affinity_scores : { rid: affinity_score_bytes_per_sec }，供除錯輸出用。
        """
        # ══════════════════════════════════════════════════════════════════════
        # FORWARD 策略白名單 (FORWARD_STRATEGY_TASKS)
        #
        # 定義哪些「上游 Task 關鍵字 → 下游 Task 關鍵字」之間採用 FORWARD (1-to-1) 策略。
        # FORWARD 策略的判定條件：兩端平行度相同，且 Flink 採用 FORWARD 分區策略。
        # 格式：{ "上游 task_name 的子字串": "對應下游 task_name 的子字串" }
        #
        # 範例（請依照實際部署的 Flink Task 名稱填入）：
        #   "Window_Join" → "Sink" 代表：
        #       task_name 含 "Window_Join" 的算子，其輸出以 FORWARD 策略傳給含 "Sink" 的算子。
        # ══════════════════════════════════════════════════════════════════════
        FORWARD_STRATEGY_TASKS = {
            # "Window_Join": "Sink",   # ← 範例：請填入您環境的真實 Task 名稱子字串
            "Window_Join" : "Sink:_KafkaSink:_Writer____Sink:_KafkaSink:_Committer"
        }

        # ── 基本防衛：確認瓶頸資訊存在 ──────────────────────────────────────
        if bottleneck_subtask_id not in self._task_info:
            print(f"   ⚠️ [{bottleneck_subtask_id}] 不在 _task_info，無法計算親和力")
            return None, {}

        bottleneck_info = self._task_info[bottleneck_subtask_id]

        # ══════════════════════════════════════════════════════════════════════
        # Step 1: 估算下游節點的傳輸比例 (Proportional Estimation)
        #         含 FORWARD vs HASH 分流邏輯
        # ══════════════════════════════════════════════════════════════════════

        # 透過 get_neighbors 取得此 Subtask 的下游算子關鍵字（如 {"Window_Join"}）
        _, downstream_keywords = self.get_neighbors(bottleneck_subtask_id)

        if not downstream_keywords:
            # 例如 Sink 算子沒有下游，無法建立親和力分數，退回
            print(f"   ⚠️ [{bottleneck_subtask_id}] 無下游算子關鍵字，跳過親和力計算")
            return None, {}

        # 在 _task_info 中找出所有屬於下游算子的 Subtask
        downstream_subtasks = [
            sid for sid, info in self._task_info.items()
            if any(kw in info["task_name"] for kw in downstream_keywords)
        ]

        if not downstream_subtasks:
            print(f"   ⚠️ [{bottleneck_subtask_id}] 找不到任何下游 Subtask，跳過親和力計算")
            return None, {}

        # 取得 S_b 的總輸出流量（Bytes/s），代表它對所有下游的總傳輸量
        # S_b.numBytesOutPerSecond ≈ observed_output_rate
        bytes_out_sb = bottleneck_info.get("observed_output_rate", 0.0)

        # ── 判斷此瓶頸 Task 是否在 FORWARD 策略白名單中 ──────────────────────
        # 取出瓶頸算子的 task_name（subtask_id 格式為 "TaskName_index"，最後一段是數字）
        bottleneck_task_name = bottleneck_info["task_name"]

        # 取出瓶頸的 subtask_index（用於 FORWARD 策略中的 1-to-1 配對）
        # subtask_index 儲存在 _task_info 的 "subtask_index" 欄位
        bottleneck_subtask_index = bottleneck_info.get("subtask_index", -1)

        # 比對白名單：檢查 bottleneck_task_name 是否含有任一 FORWARD 上游關鍵字，
        # 且其對應的下游關鍵字與 downstream_keywords 有交集
        is_forward = False
        forward_downstream_kw = None
        for upstream_kw, downstream_kw in FORWARD_STRATEGY_TASKS.items():
            if upstream_kw in bottleneck_task_name and downstream_kw in downstream_keywords:
                # 確認：此 Task 的確是以 FORWARD 策略對應該下游算子
                is_forward = True
                forward_downstream_kw = downstream_kw
                break

        edge_weights = {}

        if is_forward:
            # ════════════════════════════════════════════════════════════════
            # Branch A: FORWARD 策略 (1-to-1 傳輸)
            #
            # 上游 Subtask i 的全部輸出只會傳給下游 Subtask i（同 index）。
            # 因此不需要按比例分攤，直接將 100% 的 bytes_out_sb 賦予
            # 與 bottleneck_subtask_index 相同的那個下游 Subtask。
            # 若找不到對應 index 的下游，視為退化情況退回。
            # ════════════════════════════════════════════════════════════════
            print(f"\n   📡 [{bottleneck_subtask_id}] Step 1 — Branch A (FORWARD 1-to-1) "
                  f"尋找下游 index={bottleneck_subtask_index} 的配對 Subtask...")

            # 找出「下游算子中，subtask_index 與瓶頸相同」的唯一一個 Subtask
            # FORWARD 策略保證上游 i → 下游 i，因此只有一個配對目標
            matched_forward_sid = None
            for d_sid in downstream_subtasks:
                d_info = self._task_info[d_sid]
                # 僅鎖定屬於 FORWARD 指定下游算子且 index 相符的 Subtask
                if (forward_downstream_kw in d_info["task_name"]
                        and d_info.get("subtask_index", -2) == bottleneck_subtask_index):
                    matched_forward_sid = d_sid
                    break

            if matched_forward_sid is None:
                # 找不到配對的下游 Subtask（可能 index 不連續或名稱不符），退回
                print(f"   ⚠️ [{bottleneck_subtask_id}] Branch A — "
                      f"找不到 index={bottleneck_subtask_index} 的下游 Subtask，退回比例推算")
                # 退化處理：fallthrough 到 Branch B（不直接 return，讓後續比例邏輯接管）
                is_forward = False
            else:
                # FORWARD 策略：直接將 100% 輸出流量賦予配對下游 Subtask
                # EdgeWeight(S_b → d_i) = bytes_out_sb × 100%
                # 因為沒有任何資料被打散，所有流量都流向這一個下游
                edge_weights[matched_forward_sid] = bytes_out_sb

                d_tm = current_locations.get(matched_forward_sid, "unknown")
                print(f"      ✅ 配對成功: {matched_forward_sid:<40} "
                      f"→ TM: {d_tm:<20} EdgeWeight={bytes_out_sb/1e6:.3f}MB/s (100%)")

        if not is_forward:
            # ════════════════════════════════════════════════════════════════
            # Branch B: HASH / REBALANCE 策略 (M-to-N 傳輸，預設情況)
            #
            # 因 Flink 無法直接提供 M-to-N 的點對點傳輸量，
            # 以「各下游 Subtask 的 actual_input_rate 佔比」推算分配比例，
            # 再乘上 S_b 的總輸出流量，得到每條 Edge 的估計頻寬消耗。
            # ════════════════════════════════════════════════════════════════

            # 計算下游所有 Subtask 的 actual_input_rate 加總（作為比例分母）
            # total_downstream_rate = Σ d_i.actual_input_rate
            total_downstream_rate = sum(
                self._task_info[d]["actual_input_rate"] for d in downstream_subtasks
            )

            if total_downstream_rate <= 0:
                # 分母為 0（下游全部靜止），無法推算比例，退回
                print(f"   ⚠️ [{bottleneck_subtask_id}] Branch B — 下游 actual_input_rate 總和為 0，退回")
                return None, {}

            # 計算每條 Edge 的估計傳輸量（Bytes/s）：
            # EdgeWeight(S_b → d_i) = bytes_out_sb × (d_i.actual_input_rate / total_downstream_rate)
            # 意義：d_i 消耗的流量佔下游總量的比例，即 S_b 實際分配給 d_i 的輸出量
            for d_sid in downstream_subtasks:
                d_rate = self._task_info[d_sid]["actual_input_rate"]
                # 按下游各 Subtask 的流量比例，估算 S_b 傳給該 Subtask 的資料量
                edge_weights[d_sid] = bytes_out_sb * (d_rate / total_downstream_rate)

            print(f"\n   📡 [{bottleneck_subtask_id}] Step 1 — Branch B (HASH M-to-N) Edge 權重估算 "
                  f"(下游總速率={total_downstream_rate/1e6:.2f}MB/s, "
                  f"S_b 輸出={bytes_out_sb/1e6:.2f}MB/s):")
            for d_sid, ew in edge_weights.items():
                d_tm = current_locations.get(d_sid, "unknown")
                print(f"      {d_sid:<40} → TM: {d_tm:<20} EdgeWeight={ew/1e6:.3f}MB/s")

        # ══════════════════════════════════════════════════════════════════════
        # Step 2: 計算各候選 TM 的親和力總分 (Aggregate Affinity Score)
        #
        # AffinityScore(TM_j) = Σ EdgeWeight(S_b → d_i)，for all d_i 位於 TM_j
        #
        # 分數越高 = S_b 搬過去後，可省下越多實體網路傳輸（轉為 Local Exchange）
        # ══════════════════════════════════════════════════════════════════════
        affinity_scores = {}
        for rid in resource_map.keys():
            # 累加所有「目前住在此 TM 上的下游 Subtask」的 EdgeWeight
            score = sum(
                ew for d_sid, ew in edge_weights.items()
                if current_locations.get(d_sid, "unknown") == rid
            )
            affinity_scores[rid] = score

        print(f"   🎯 [{bottleneck_subtask_id}] Step 2 — 各 TM 親和力總分 (AffinityScore):")
        for rid, score in sorted(affinity_scores.items(), key=lambda x: -x[1]):
            print(f"      {rid:<25}: {score/1e6:.4f}MB/s")

        # ══════════════════════════════════════════════════════════════════════
        # Step 3: 動態網路約束檢查 (Dynamic Network Constraint Check)
        #
        # 修正後的物理模型，分兩種情況計算真實的網路增量：
        #
        # 情況 A (目標 TM == 瓶頸目前所在 TM)：
        #   → expected_network_added = 0
        #   原因：Subtask 留在原地，所有網路流量已反映在目前的監控讀數中，
        #         resource_map 的 network_traffic 並未重複計算，增量為零。
        #
        # 情況 B (目標 TM != 瓶頸目前所在 TM)：
        #   → expected_network_added = b_total_traffic - (2 × score)
        #   推導：
        #     · b_total_traffic = bytes_in_sb + bytes_out_sb（S_b 產生的全部網卡流量）
        #     · score (AffinityScore) 代表「搬過來後能就地消化的下游流量」，
        #       此部分原本需要：
        #         (i)  S_b 透過外部網卡「送出」一次 Egress → 省下 score
        #         (ii) 下游 Subtask 透過外部網卡「接收」一次 Ingress → 省下 score
        #       兩端合計省下 2×score 的實體網卡流量，故從增量中扣除兩倍。
        #     · expected_network_added 可以為負值，代表遷移後目標 TM 的網卡負載
        #       實際上是「淨減少」的，屬於物理合理情況，不需 clamp 到 0。
        #
        # 硬性檢查：current_net + expected_network_added ≤ bw_limit
        # 超過上限的 TM 直接從候選名單剔除
        # ══════════════════════════════════════════════════════════════════════

        # 取得 S_b 的輸入流量（Bytes/s）
        bytes_in_sb = bottleneck_info.get("observed_input_rate", 0.0)

        # S_b 在目前 TM 上產生的全部網卡流量（Ingress + Egress 加總）
        b_total_traffic = bytes_in_sb + bytes_out_sb

        # 瓶頸 Subtask 目前所在的 TM（用於判斷情況 A / B）
        b_current_tm_id = current_locations.get(bottleneck_subtask_id, "unknown")

        print(f"   🔒 [{bottleneck_subtask_id}] Step 3 — 網路頻寬約束檢查 "
              f"(S_b 當前 TM={b_current_tm_id}, "
              f"in={bytes_in_sb/1e6:.2f}MB/s, out={bytes_out_sb/1e6:.2f}MB/s, "
              f"total={b_total_traffic/1e6:.2f}MB/s):")

        valid_tms = {}
        for rid, affinity in affinity_scores.items():

            if rid == b_current_tm_id:
                # ── 情況 A：目標就是瓶頸原本所在的 TM ────────────────────────
                # 流量已計入 resource_map，不會產生任何新增的網卡負載，增量為零
                expected_network_added = 0.0
                case_label = "A (原地不動)"
            else:
                # ── 情況 B：目標是其他 TM ─────────────────────────────────────
                # 雙倍節省效應：
                #   · S_b 送出的 score 部分轉為 Local Exchange → 省下 score Egress
                #   · 下游接收的 score 部分亦轉為 Local Exchange → 省下 score Ingress
                # 因此從總流量中扣除 2 × score，得到真實的新增網卡負載
                # 注意：結果可能為負，代表遷移後此 TM 網卡負載淨減少，屬正常現象
                expected_network_added = b_total_traffic - (2.0 * affinity)
                case_label = "B (跨 TM 遷移)"

            # 目前此 TM 已累積的網路流量（靜態 Subtask + 已決定的遷移）
            current_net = resource_map[rid].get("network_traffic", 0.0)
            # 此 TM 的頻寬硬性上限（Bytes/s）
            bw_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)

            # 預估遷移後的總網路流量（允許 expected_network_added 為負值）
            projected_net = current_net + expected_network_added

            # 雙倍扣除的明細，方便 debug
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

        # ══════════════════════════════════════════════════════════════════════
        # Step 4: 貪婪選擇與退回機制 (Greedy Selection & Fallback)
        #
        # 在通過 Step 3 的候選 TM 中，選 AffinityScore 最高的作為目標。
        # 若：(a) 無有效候選、(b) 所有親和力為 0、(c) 無下游資訊
        # → 回傳 None，由呼叫端退回 CPU 負載均衡邏輯。
        # ══════════════════════════════════════════════════════════════════════
        if not valid_tms:
            # 所有 TM 均無法通過頻寬約束，退回 CPU 負載均衡
            print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 無有效 TM（均超過頻寬上限），退回 CPU 負載均衡")
            return None, affinity_scores

        if all(score == 0.0 for score in valid_tms.values()):
            # 所有親和力均為 0（下游不在任何候選 TM 上），親和力無法區分，退回 CPU 負載均衡
            print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 所有有效 TM 親和力皆為 0，退回 CPU 負載均衡")
            return None, affinity_scores

        # 在通過約束的 TM 中，選擇親和力分數最高者（最大化 Local Exchange 節省量）
        best_tm = max(valid_tms, key=lambda rid: valid_tms[rid])
        print(f"   ✅ [{bottleneck_subtask_id}] Step 4 — 拓撲親和力選定目標 TM: "
              f"{best_tm} (AffinityScore={valid_tms[best_tm]/1e6:.4f}MB/s)")

        return best_tm, affinity_scores

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
        #  detection if no explicit list provided
        if prioritized_list is None:
            if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
                print("⚠️ 未檢測到瓶頸，無需產生遷移計畫")
                return None
            # Extract subtask IDs from CAOM detection results (backward compatibility)
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
        # TODO 除了slot數量限制，不要用硬約束，用軟約束 ＋ 評分機制
        # ===== Hard Constraints =====
        MAX_SLOTS_PER_TM = 6
        # CPU_HEADROOM_LIMIT = 800  # ms (changed from 850 to 800 as per requirement)

        # ===== 1. 初始化資源地圖 (乾淨基線) 可用 busy rate 是 6000 =====
        resource_map = {}
        for rid, info in tm_info.items():
            # 我們只計算那些「不遷移」的 subtask 的總負載
            static_busy_time = 0
            static_slots = 0
            static_net = 0

            for sid, loc_rid in current_locations.items():
                # 如果這個 subtask 在這台 TM 上，且它「不在」待遷移名單中
                if loc_rid == rid and sid not in prioritized_subtask_ids:
                    static_slots += 1
                    if sid in self._task_info:
                        static_busy_time += (self._task_info[sid]['T_busy'] * 1000)
                        static_net += self._task_info[sid]['observed_rate']

            resource_map[rid] = {
                'slots':  static_slots,
                'busy_time': static_busy_time, # 這絕對不會是負數
                'network_traffic': static_net,
                'cpu_limit': info['cpu_limit'],
                'host': info['host']
            }

        print(f"\n📊 資源快照 (Pre-deduction 後已使用資源):")
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

            # subtask 資訊
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

            # Calculate subtask's resource requirements, actual input rate是由observed rate等比例放大的
            # 網路頻寬用硬指標，cpu負載用軟指標
            # subtask_traffic = actual_input_rate
            subtask_traffic = observed_rate * 1.2
            # 標準化 busy rate * current_tm_cpu_limit
            per_busy_rate = busy_time_ms * cpu_limit

            # ===== 6. Dynamic Weighting by Bottleneck Cause =====
            # Get neighbors for topology scoring
            upstream_keywords, downstream_keywords = self.get_neighbors(subtask_id)

            # ===== 6.5 若為 Network 瓶頸，優先使用拓撲親和力演算法 =====
            # 呼叫 calculate_topology_affinity，透過資料引力分數找到最佳目標 TM，
            # 以最大化 Local Exchange、最小化實體網路頻寬消耗。
            # 若演算法找不到有效目標（退回條件：無下游、頻寬全滿、親和力全為 0），
            # 則 network_affinity_success = False，後續退回使用 CPU 負載均衡邏輯。
            network_affinity_success = False
            best_tm = None
            best_score = -float('inf')

            if cause == "NETWORK_BOTTLENECK":
                print(f"\n🌐 [{subtask_id}] 偵測為 Network 瓶頸，啟動拓撲親和力演算法...")
                affinity_best_tm, affinity_scores = self.calculate_topology_affinity(
                    subtask_id, resource_map, current_locations
                )
                # 拓撲演算法回傳有效 TM，且 Slot 數量未超限（硬約束驗證）
                if affinity_best_tm and resource_map.get(affinity_best_tm, {}).get('slots', MAX_SLOTS_PER_TM) + 1 <= MAX_SLOTS_PER_TM:
                    best_tm = affinity_best_tm
                    network_affinity_success = True
                    print(f"   ✅ 拓撲親和力演算法成功，直接選定 TM: {best_tm}，跳過貪婪評分迴圈")
                else:
                    # 退回：拓撲演算法未能找到有效 TM，改用 CPU 負載均衡邏輯
                    print(f"   ⚠️ 拓撲親和力演算法退回，改用 CPU 負載均衡邏輯作為備援")

            # ===== 7. Evaluate all TMs and find best match =====
            # 執行條件：
            #   (a) CPU 瓶頸 → 一律執行此迴圈，使用 CPU 分數選擇最佳 TM
            #   (b) Network 瓶頸且拓撲演算法已成功 → 跳過（best_tm 已由拓撲演算法設定）
            #   (c) Network 瓶頸且拓撲演算法退回 → 執行此迴圈，以 CPU 分數作為備援依據
            if not network_affinity_success:
                for rid, res in resource_map.items():
                    # 依照各個 TM 計算 Projected resources after adding this subtask
                    # 轉向目標機器的預估負載
                    busy_on_target = per_busy_rate / res['cpu_limit']
                    projected_slots = res['slots'] + 1
                    projected_busy = res['busy_time'] + busy_on_target
                    projected_traffic = res['network_traffic'] + subtask_traffic

                    # 硬約束：Slot 數量不可超限
                    if projected_slots > MAX_SLOTS_PER_TM:
                        continue  # Violates slot limit

                    # ===== 9. Multi-Factor Scoring =====
                    # CPU Score: 剩餘可用 CPU 比例 (1 - 已投影負載 / 該 TM 的 CPU 上限×1000ms)
                    # 分數越高代表該 TM 越空閒，越適合承接新的運算負載
                    cpu_score = 1 - (projected_busy / (res['cpu_limit'] * 1000))

                    # Net Score: 剩餘可用頻寬比例 (1 - 已投影流量 / 頻寬上限)
                    # 用於確認候選 TM 有足夠頻寬餘裕，不可為負（會被硬約束擋住）
                    tm_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                    net_score = 1 - (projected_traffic / tm_limit)

                    # 至少候選 TM 的 CPU 上限要 ≥ 瓶頸 Subtask 原本所在 TM 的 CPU 上限
                    # 並且頻寬餘量必須為正（不超過頻寬上限）
                    if net_score > 0.0 and res['cpu_limit'] >= cpu_limit:
                        # CPU 瓶頸 或 Network 瓶頸退回情況：
                        # 統一以「CPU 剩餘容量分數」作為貪婪選擇依據，選擇最空閒的 TM
                        if cpu_score > best_score:
                            best_score = cpu_score
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
                overload_pct = ((observed_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0

                print(f"✅ 分配成功 (優先級={priority_score:.3f}, {cause.replace('_BOTTLENECK', '')}): {subtask_id}")
                print(f"   從: {original_rid} -> 到: {best_tm}")
                print(f"   新狀態: Slots={resource_map[best_tm]['slots']}/{MAX_SLOTS_PER_TM}, "
                      f"BusyTime={resource_map[best_tm]['busy_time']:.0f} "
                      f"Traffic={resource_map[best_tm]['network_traffic']/(1024*1024):.2f}MB/s \n")
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

        # ===== 12. Final Validation & Summary 驗證每個TM 遷移後狀態 =====
        print(f"\n📊 最終資源分配驗證:")
        validation_errors = []
        for rid, res in resource_map.items():
            slots_ok = res['slots'] <= MAX_SLOTS_PER_TM
            # cpu_ok = res['busy_time'] <= CPU_HEADROOM_LIMIT
            tm_limit = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
            net_ok = res['network_traffic'] <= tm_limit
            limit_mb = tm_limit / (1024 * 1024)

            status = "✅" if (slots_ok  and net_ok) else "❌"

            print(f"   {status} {rid}: Slots={res['slots']}/{MAX_SLOTS_PER_TM}, "
                  f"BusyTime={res['busy_time']:.0f}, "
                  f"Traffic={res['network_traffic']/(1024*1024):.2f}/{limit_mb:.2f}MB/s")

            if not slots_ok:
                validation_errors.append(f"{rid}: Slot 超限 ({res['slots']} > {MAX_SLOTS_PER_TM})")
            """
            if not cpu_ok:
                validation_errors.append(f"{rid}: CPU 超限 ({res['busy_time']:.0f}ms > {CPU_HEADROOM_LIMIT}ms)")
            """
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

        # --- 新增：紀錄遷移路徑細節 ---
        current_locations = self.get_subtask_locations()
        detail_exists = os.path.isfile(detail_log)
        event_time = time.time()

        with open(detail_log, "a", newline="") as f:
            writer = csv.writer(f)
            if not detail_exists:
                writer.writerow(["event_timestamp", "subtask_id", "from_tm", "to_tm", "cause", "state_size_mb"])

            for sid, target_tm in migration_plan.items():
                original_tm = current_locations.get(sid, "unknown")
                if target_tm != original_tm:
                    # 只有真正有搬動的才紀錄
                    info = self._task_info.get(sid, {})
                    cause = info.get("bottleneck_cause", "UNKNOWN")
                    state_mb = info.get("state_size", 0) / (1024*1024)

                    writer.writerow([event_time, sid, original_tm, target_tm, cause, round(state_mb, 2)])

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
        print("STEP 1: 細粒度瓶頸檢測 (階層式診斷: CPU vs Network)")
        print("=" * 100)
        reports = self.detect_bottleneck()

        if not reports:
            print("⚠️ 無法獲取監控數據")
            return False

        # Check if any bottlenecks were detected
        if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
            print("✅ 未檢測到瓶頸，系統運行正常")
            return False

        print(f"\n🔥 檢測到 {len(self._bottleneck_subtasks)} 個瓶頸 Subtask:")
        for subtask_id, actual_rate, max_capacity in self._bottleneck_subtasks:
            overload_pct = ((actual_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0

            # Get bottleneck cause from task_info
            cause = self._task_info[subtask_id].get("bottleneck_cause", "UNKNOWN")
            T_busy = self._task_info[subtask_id]["T_busy"]
            T_bp = self._task_info[subtask_id]["T_bp"]

            # Display different icons and reasons based on cause
            if cause == "CPU_BOTTLENECK":
                icon = "🔥"
                reason = f"CPU過載 (T_busy={T_busy*1000:.0f}ms , T_bp={T_bp*1000:.0f}ms )"

            elif cause == "NETWORK_BOTTLENECK":
                icon = "🌐"
                reason = f"網路頻寬受限 (T_busy={T_busy*1000:.0f}ms, T_bp={T_bp*1000:.0f}ms, TM頻寬已飽和)"
            else:
                icon = "⚠️"
                reason = "原因未知"

            cmp_sym = ">" if actual_rate > max_capacity else "<"
            print(f"   {icon} {subtask_id}: 實際輸入 {actual_rate:.2f} rec/s {cmp_sym} 最大容量 {max_capacity:.2f} rec/s (過載 {overload_pct:.1f}%)")
            print(f"      原因: {reason}")
        '''
        # Step 2: Two-tier migration trigger evaluation
        print("\n" + "=" * 100)
        print("STEP 2: 兩階段遷移觸發評估")
        print("=" * 100)
        
        should_trigger, final_candidates = self.evaluate_migration_trigger(self._bottleneck_subtasks)

        if not should_trigger:
            print("\n⚠️ 全局觸發條件未滿足，不執行遷移")
            return False
        '''
        # Step 3: Multi-dimensional prioritization
        print("\n" + "=" * 100)
        #print("STEP 3: 多維度優先級排序")
        print("STEP 3: 繞過觸發評估，直接進行多維度優先級排序")
        print("=" * 100)
        bottleneck_ids = [sid for sid, _, _ in self._bottleneck_subtasks]
        prioritized_list = self.get_prioritized_list(bottleneck_ids)
        # prioritized_list = self.get_prioritized_list(final_candidates)

        if not prioritized_list:
            print("⚠️ 優先級列表為空")
            return False

        # Step 4: Generate migration plan using greedy allocation
        print("\n" + "=" * 100)
        print("STEP 4: 貪婪分配遷移計畫 (6-slot limit, Normalized Load)")
        print("=" * 100)
        migration_plan, migrated_count = self.generate_migration_plan(prioritized_list)

        if not migration_plan:
            return False

        if migrated_count == 0:
            print("\n✨ 決策結果: 雖然有瓶頸，但分配算法認為維持現狀是最佳選擇 (migrated_count = 0)")
            print("⚠️ 取消遷移觸發，避免無意義的中斷。")
            return False

        return self.trigger_migration(migration_plan)

        print("\n✅ 遷移計畫已生成並寫入檔案")
        return True