import requests
import statistics
import csv
import json
import time
import os
import subprocess
import argparse
import math

"""
propose_v13.py — 基於 propose_v12.py，修正 affinity 頻寬預估與 resource_map 累加不一致（FIX-10）

v12 修正項目（相對於 v11）：

  [FIX-9] trigger_migration 加入遷移後重啟前的 TM Slot 就緒等待
     問題根因：Job 停止後 Flink 回報 CANCELED/FINISHED 的時間點，
     早於各 TM 實際將 Slot 狀態從 ALLOCATED 改回 FREE 的時間點。
     弱節點（單核 TM，如 tm_20c_5）因長期高負載導致 JVM GC 積壓，
     Slot 釋放速度更慢，在 flink run -s <savepoint> 送出時，
     JobManager 等不到其 freeSlot 回報，直接跳過該 TM 進行分配，
     造成應落在此 TM 的 Subtask 沒有可用 Slot，Job 啟動失敗。

     症狀：
       - 遷移後 Subtask 列表比預期少（tm_20c_5 整個消失）
       - HOP_2、Window_Count_2 等 Subtask 不見，Job 處於部分啟動狀態
       - 之後持續出現「獲取 Checkpoint ID 失敗: NoneType...」
       - Latency 歸零，後續完全無資料

     修法：在 wait_for_job_termination 之後、submit_job_from_savepoint 之前，
     依序呼叫兩個新方法：
       1. _wait_for_slots_freed(expected_slots, timeout=30)
          等待全叢集 freeSlots 總數達到停止 Job 所釋放的 slot 數量，
          確認 Slot 帳目已回歸到「無 Job 運行」的基線值。
          判斷依據：/taskmanagers REST API 回報的 freeSlots 加總。
       2. _wait_for_all_tms_have_free_slot(timeout=20)
          確認每一個 TM 都有至少 1 個 freeSlot，
          防止弱節點因回應慢被 JobManager 在 slot offer 視窗內排除。
     兩步驟都通過後，才真正執行 submit_job_from_savepoint。

v11 修正項目（完整保留）：
  [FIX-1] resource_map 預扣除改用 observed_output_rate（出口流量）
  [FIX-2] 確認 resource_map 帳目正確性（無需對來源 TM 做扣除）
  [FIX-3] affinity Step 3 Case A（原地不動）加回自身流量
  [FIX-4] _find_max_remaining_bw_tm 加入 Slot 上限檢查
  [FIX-5] CPU 負載均衡的 cpu_limit 門檻改為動態負載判斷
  [FIX-6] subtask_traffic 統一改用 observed_output_rate * 1.2
  [FIX-7] 驗證失敗阻擋遷移執行
  [FIX-8] Bundle 路徑分配成功後更新 pending_locations

v10 核心功能（完整保留）：
  1. Source 資料率歷史記錄（_source_rate_history）
  2. 秒級最佳遷移時間搜尋（_find_optimal_migration_start）
  3. State size 趨勢感知（_is_state_decreasing）
  4. evaluate_migration_trigger 三態回傳
  5. 動態冷卻期（_is_cooldown_done）
  6. auto_detect_and_migrate 排程重入邏輯

瓶頸原因代碼（與 v10 相同）：
  NETWORK_TM_SATURATED  → 1a：TM 實體頻寬已達 IQR fence 閾值
  NETWORK_BOTTLENECK    → 1b：outPool 高且下游 inPool 低，自身是窄點
  CPU_BOTTLENECK        → 1c：Z_busy 統計異常且 T_busy > T_bp
  LATENT_CPU            → 2：adjusted_headroom 低於 IQR 下界 fence
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
                 initial_placement_path=None,
                 migration_record_path=None,
                 job_config=None):
        self.base_url = prometheus_url
        self.flink_rest_url = flink_rest_url
        self.migration_plan_path = migration_plan_path
        self.savepoint_dir = savepoint_dir
        self.last_migration_time = 0
        self.migration_cooldown = 180
        self._bottleneck_subtasks = []
        self._task_info = {}
        self._ordered_operators = []

        # ── v10: 最佳遷移時間選擇狀態變數 ────────────────────────────────────
        # Source 資料率歷史（每筆 = 一個 CHECK_INTERVAL 的 observed_output_rate 總和）
        self._source_rate_history = []
        self._history_max_len = 300          # 最多 300 筆（Theorem 1 預測集大小 C 上限）

        # State size 歷史（用於 Window 算子趨勢感知，key=subtask_id, value=deque）
        self._state_size_history  = {}       # { subtask_id: [size_t-N, ..., size_t] }
        self._state_history_len   = 5        # 保留最近 5 筆判斷趨勢

        # 排程等待狀態機（與 baseline_v4 對稱設計）
        self._pending_migration_plan  = None  # 備妥等待執行的遷移計畫
        self._migration_scheduled     = False # 是否正在等待最佳時機
        self._migration_ready_time    = None  # 進入排程狀態的時刻
        self._migration_wait_timeout  = 120   # 最長等待秒數，超過強制執行
        self._scheduled_recheck_count = 0     # 排程等待期間的輪次計數器
        # 每 RECHECK_EVERY_N 輪重新 detect 一次確認瓶頸仍存在

        cfg = JOB_CONFIG[query_type]
        self.query_type = query_type
        self.job_name = cfg["job_name"]
        self.target_order = cfg["target_order"]

        output_dir = f"/home/yenwei/research/structure_setup/output/{output_id}/"
        os.makedirs(output_dir, exist_ok=True)
        self.log_file = os.path.join(output_dir, f"metrics_{query_type}.csv")
        self.detail_log = os.path.join(output_dir, f"migration_details_{query_type}.csv")

        print(f"[FlinkPropose v13] Monitoring job : {self.job_name}")
        print(f"[FlinkPropose v13] Metrics CSV    : {self.log_file}")
        print(f"[FlinkPropose v13] Details CSV    : {self.detail_log}")

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

        # ── 遷移觸發的 latency 門檻 ──
        # 冷卻期結束後，還需要當下 latency 超過此值才真正寫入 migration_plan 並遷移
        # 設為 0 表示不額外限制（只靠冷卻期）
        self.migration_latency_threshold = 20000   # ms

        # ── 初始配置備份 ──
        # initial_placement_path: 若指定，則在第一次 auto_detect_and_migrate 執行時，
        # 將當前 migration_plan.json（Flink 原生分配的結果，尚未被 propose 修改）
        # 備份到此路徑，格式與 migration_plan.json 完全相同，供 baseline 使用。
        self.initial_placement_path = initial_placement_path
        self._initial_placement_saved = False   # 確保只備份一次

        # ── 遷移完整記錄檔 ──
        # 每次觸發遷移後，將所有資訊（subtask 遷移前後位置、state size、
        # bottleneck 原因、中斷時間）整合寫入此檔案，供計算遷移開銷用。
        # 若未指定則寫到 output_dir/migration_record.txt
        if migration_record_path:
            self.migration_record_path = migration_record_path
        else:
            self.migration_record_path = os.path.join(output_dir, "migration_record.txt")

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
        tm_out_saturated, saturation_rates, _mu_sat, _sigma_sat, \
            tm_cpu_sat_rates, mu_cpu, sigma_cpu = \
            self._compute_tm_network_saturation(task_info, subtask_locations)

        # Phase 1：兩層並行，永遠都跑
        print("\n" + "=" * 100)
        print("STEP 1: 顯性瓶頸偵測（不依賴 bp 反推）")
        print("=" * 100)
        explicit_set = self._detect_explicit(
            task_info, operator_groups, ordered_operators, subtask_locations,
            tm_out_saturated, saturation_rates,
            tm_cpu_sat_rates, mu_cpu, sigma_cpu
        )

        print("\n" + "=" * 100)
        print("STEP 2: 隱性瓶頸偵測（headroom = CPU裕度 × TM單位負載 × 上游積壓折扣）")
        print("=" * 100)
        implicit_set = self._detect_implicit(
            task_info, operator_groups, ordered_operators,
            subtask_locations, explicit_set, tm_cpu_sat_rates
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

        # v10：記錄 Source 資料率與 state size 歷史（供時間選擇使用）
        self._record_source_rate(task_info)
        self._record_state_sizes(task_info)

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
        計算各 TM 出口流量飽和率，並以 Z-score 識別統計異常高的 TM。

        閾值設計（準則一 - 來源可解釋）：
          改用 Z-score 取代原有的 IQR Tukey fence，原因：
            IQR fence 在叢集網路飽和率呈雙峰分佈時失效。
            當部分 TM 飽和率低（7~38%）、部分高（85~90%），
            IQR 橫跨整個分佈，fence 被推到物理無意義的高值（>1.5），
            再被上限截斷為固定值，失去動態判定的能力。

          Z-score 不依賴分佈形狀假設：
            Z = (tm_sat - μ) / σ
            Z > 1.0 代表該 TM 的飽和率在統計上顯著高於叢集均值（前 ~16%），
            即使雙峰分佈也能正確識別高飽和群的 TM。

          SIGMA_MIN = 0.05：
            σ < 0.05 代表所有 TM 飽和率非常接近，Z-score 無鑑別力
            （例如全部 TM 都在 30~35%，此時無需判定異常）。
            停用 Z-score，所有 TM 視為正常，不觸發 N1。

        回傳：
          tm_out_saturated  : { tm_id: bool }   Z-score > 1.0 的 TM
          saturation_rates  : { tm_id: float }  網路飽和率（0~1+）
          mu_sat            : float             叢集飽和率均值
          sigma_sat         : float             叢集飽和率標準差
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

        # Z-score 計算
        sat_vals = list(saturation_rates.values())
        mu_sat    = sum(sat_vals) / len(sat_vals) if sat_vals else 0.0
        sigma_sat = (sum((x - mu_sat) ** 2 for x in sat_vals) / len(sat_vals)) ** 0.5                     if len(sat_vals) >= 2 else 0.0

        SIGMA_MIN   = 0.05   # σ 過小代表叢集負載均勻，Z-score 無鑑別力
        Z_NET_THRESHOLD = 1.0

        tm_out_saturated = {}
        print(f"\n📊 [TM 網路狀態] Z-score 偵測 (μ={mu_sat:.2%} σ={sigma_sat:.2%}"
              f" σ_min={SIGMA_MIN:.2f}):")
        for tm_id, rate in saturation_rates.items():
            limit = self.tm_bandwidth_map.get(tm_id, self.default_bandwidth)
            if sigma_sat >= SIGMA_MIN:
                Z_sat = (rate - mu_sat) / sigma_sat
                is_saturated = Z_sat > Z_NET_THRESHOLD
                z_label = f"Z={Z_sat:.2f}"
            else:
                is_saturated = False
                z_label = "σ太小停用"
            tm_out_saturated[tm_id] = is_saturated
            icon = "🔴" if is_saturated else "🟢"
            print(f"   {tm_id}: {tm_traffic[tm_id]['out']/1e6:.2f}MB/s {icon} "
                  f"(Limit: {limit/1e6:.1f}MB/s, NetSat={rate:.2%}, {z_label})")

        # ── TM CPU 飽和率計算 ─────────────────────────────────────────────────
        # tm_cpu_sat = Σ T_busy（同 TM 所有 subtask）/ cpu_cores
        # 物理意義：所有執行緒需求的 CPU 時間總和 vs 實體核心數
        # > 1.0 代表需求超過供給，subtask 需等待排程
        _cpu_capacity = {"tm_20c_1": 2.0, "tm_20c_2": 2.0, "tm_20c_3": 2.0,
                         "tm_20c_4": 1.0, "tm_20c_5": 1.0}

        tm_total_busy = {}
        for sid, info in task_info.items():
            tm_id = subtask_locations.get(sid, "unknown")
            if tm_id == "unknown":
                continue
            tm_total_busy[tm_id] = tm_total_busy.get(tm_id, 0.0) + info["T_busy"]

        tm_cpu_sat_rates = {}
        for tm_id, total_busy in tm_total_busy.items():
            cores = _cpu_capacity.get(tm_id, 1.0)
            tm_cpu_sat_rates[tm_id] = total_busy / cores if cores > 0 else total_busy

        cpu_sat_vals = list(tm_cpu_sat_rates.values())
        mu_cpu    = sum(cpu_sat_vals) / len(cpu_sat_vals) if cpu_sat_vals else 0.0
        sigma_cpu = (sum((x - mu_cpu) ** 2 for x in cpu_sat_vals) / len(cpu_sat_vals)) ** 0.5                     if len(cpu_sat_vals) >= 2 else 0.0

        print(f"\n📊 [TM CPU 狀態] Z-score 偵測 "
              f"(μ={mu_cpu:.2%} σ={sigma_cpu:.2%} σ_min={SIGMA_MIN:.2f}):")
        for tm_id, sat in tm_cpu_sat_rates.items():
            if sigma_cpu >= SIGMA_MIN:
                Z_cpu = (sat - mu_cpu) / sigma_cpu
                is_cpu_high = Z_cpu > Z_NET_THRESHOLD
                z_label = f"Z={Z_cpu:.2f}"
            else:
                is_cpu_high = False
                z_label = "σ太小停用"
            icon = "🔴" if is_cpu_high else "🟢"
            print(f"   {tm_id}: CpuSat={sat:.2%} {icon} ({z_label})")

        return tm_out_saturated, saturation_rates, mu_sat, sigma_sat, tm_cpu_sat_rates, mu_cpu, sigma_cpu

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 1a/b/c：顯性瓶頸偵測
    # ──────────────────────────────────────────────────────────────────────────

    def _detect_explicit(self, task_info, operator_groups, ordered_operators,
                         subtask_locations, tm_out_saturated, saturation_rates,
                         tm_cpu_sat_rates, mu_cpu, sigma_cpu):
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

        後處理邏輯（1b loop 結束後、1c 開始前執行）：
          - 此時 explicit_set 只含 NETWORK_BOTTLENECK，掃一遍：
            若某算子的下游鏈（含多跳）存在任何 NETWORK_BOTTLENECK，
            代表真正的窄點在更下游，自身從 explicit_set 移除。
            _has_net_in_downstream_chain 本身是多跳查詢，不需要 BFS while 迴圈。

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

            # ── 1a：TM 層級網路飽和  這不合理─────────────────────────────────────────
            #if tm_out_saturated.get(tm_id, False):
            #    explicit_set.append((sid, "NETWORK_TM_SATURATED",
            #                         info.get("_tm_sat_rate", 1.0)))
            #    print(f"  [1a NET_TM] {sid}: TM {tm_id} 飽和")
            #    continue

            # ── 1b：outPool 因果鏈（三階段判斷）────────────────────────────────
            #
            # 設計依據：
            #   網路積壓在拓撲上由下游往上游傳播。真正的網路窄點（NETWORK_BOTTLENECK）
            #   是「TM 頻寬本身飽和」的那個 subtask；它的所有上游都是受害者。
            #   舊邏輯只用 outPool ≥ 0.7 進入判斷，但 TM 頻寬未飽和的情況下
            #   outPool 高只代表「下游施壓」，不代表本 TM 是窄點。
            #
            # 兩階段：
            #   階段一（入場門檻）：outPool ≥ 1 AND tm_sat > 50%
            #     兩個條件都要滿足才進入 NETWORK_BOTTLENECK 候選判斷。
            #     tm_sat > 50% 的依據：TM 頻寬超過一半才算「有壓力」，
            #     低於此值的 outPool 高幾乎確定是下游施壓而非自身問題。
            #
            #   階段二（condA/D 受害者過濾）：沿用原有邏輯，在通過入場門檻後判斷。
            #     condA：下游 max_inPool > 0.5（下游積壓 → 本 subtask 是受害者）
            #     condD：outPool >= 1.0 AND tm_sat > median_sat（TM 自身積壓確認）
            #     is_victim = condA AND NOT condD
            #
            #   後處理（在所有 subtask 跑完後執行）：
            #     - 後處理一（BFS）：只保留最下游的 NETWORK_BOTTLENECK，
            #       上游有相同標記的 subtask 降為 VICTIM。
            #     - 後處理二（上游鏈過濾）：explicit_set 中任何 subtask
            #       的下游鏈有 NETWORK_BOTTLENECK → 從 explicit_set 移除。

            if out_pool >= 0.9:
                tm_sat   = saturation_rates.get(tm_id, 0.0)
                sat_vals = sorted(saturation_rates.values())
                median_sat = statistics.median(sat_vals) if sat_vals else 0.0

                # 階段一入場門檻：TM 頻寬需超過 85%
                TM_SAT_ENTRY = 0.5
                if tm_sat <= TM_SAT_ENTRY:
                    # TM 頻寬未飽和 → 不可能是自身造成的網路窄點，靜默跳過
                    print(f"  [1b SKIP] {sid}: outPool={out_pool:.2f} 但"
                          f" tm_sat={tm_sat:.1%} ≤ {TM_SAT_ENTRY:.0%}，TM 頻寬未飽和")
                    continue

                downstream_op = downstream_of.get(op_name)
                if downstream_op:
                    ds_subtasks   = operator_groups.get(downstream_op, [])
                    ds_infos      = [task_info[dst] for dst in ds_subtasks if dst in task_info]

                    if ds_infos:
                        # 階段二：condA / condD 受害者過濾
                        ds_max_inpool = max(d["in_pool_usage"] for d in ds_infos)

                        # condA：下游輸入積壓，代表下游消化不了 → 受害者信號
                        condA = ds_max_inpool > 0.5

                        # condD：outPool 已耗盡 exclusive buffer + TM 頻寬高於中位數
                        #   兩個條件同時成立才能確認是 TM 自身的網路瓶頸，
                        #   而非被下游施壓（否定 condA 的受害者判定）
                        condD = out_pool >= 1.0 and tm_sat > median_sat

                        is_victim = condA and not condD

                        if is_victim:
                            info["bottleneck_cause"] = "BACKPRESSURE_VICTIM"
                            print(f"  [VICTIM] {sid}: condA={condA} condD={condD}"
                                  f"  outPool={out_pool:.2f}"
                                  f"  ds_max_inPool={ds_max_inpool:.2f}"
                                  f"  tm_sat={tm_sat:.1%}")
                            continue
                        else:
                            out_pool_norm = min(out_pool, 1.5) / 1.5
                            d_overload    = max(tm_sat, out_pool_norm)
                            info["_net_d_overload"] = d_overload
                            explicit_set.append((sid, "NETWORK_BOTTLENECK", d_overload))
                            reason = "condD(TM積壓)" if condD else "outPool積壓(condA=False)"
                            print(f"  [NETWORK] {sid}: {reason}"
                                  f"  tm_sat={tm_sat:.1%} outPool={out_pool:.2f}"
                                  f"  d_overload={d_overload:.3f}")
                            continue

                # 無下游（理論上不應發生，Sink outPool 幾乎為 0）→ 自身問題
                out_pool_norm = min(out_pool, 1.5) / 1.5
                explicit_set.append((sid, "NETWORK_BOTTLENECK", out_pool_norm))
                print(f"  [NETWORK] {sid}: outPool={out_pool:.2f}（無下游，自身是窄點）")
                continue

        # ── 1b 後處理：只保留最下游的網路瓶頸 ──────────────────────────────────
        #
        # 此時 explicit_set 只含 NETWORK_BOTTLENECK（1c 尚未執行），
        # 因此直接掃一遍：若某算子的下游鏈（含多跳）存在任何 NETWORK_BOTTLENECK，
        # 代表真正卡住的點在更下游，自身從 explicit_set 移除。
        # 不需要 BFS while 迴圈，因為 _has_net_in_downstream_chain 本身就是多跳查詢。
        _net_ops_after_1b = {
            task_info[s]["task_name"]
            for s, cause, _ in explicit_set if cause == "NETWORK_BOTTLENECK"
        }

        def _has_net_in_downstream_chain(op_name):
            """沿拓撲往下查，任何一層下游算子有 NET 即返回 True"""
            cur = downstream_of.get(op_name)
            while cur:
                if cur in _net_ops_after_1b:
                    return True
                cur = downstream_of.get(cur)
            return False

        _to_remove = [item for item in explicit_set
                      if _has_net_in_downstream_chain(task_info[item[0]]["task_name"])]
        for item in _to_remove:
            explicit_set.remove(item)
            print(f"  [1b 過濾→移除] {item[0]}: 下游鏈有 NETWORK_BOTTLENECK，"
                  f"非最下游窄點，移出 explicit_set")

        # ── 1c：Z-score busy 異常 + TM CPU 飽和（兩個視角同時成立）──────
        # subtask 層級：Z_busy > 1.0 AND T_busy > T_bp
        #   「這個 subtask 在全局分佈中統計異常忙碌，且忙碌不是因為等下游」
        #
        # TM 層級（新增）：Z_cpu_sat > 1.0
        #   「這個 subtask 所在的 TM，CPU 需求在叢集中統計異常高」
        #   計算方式：Z_cpu_sat = (tm_cpu_sat - μ_cpu) / σ_cpu
        #   tm_cpu_sat = Σ T_busy（同 TM 所有 subtask）/ cpu_cores
        #
        # 為什麼需要 TM 視角？
        #   Sink 等 busy=0ms 的 subtask 大量拉低全局 μ_busy 和 σ_busy，
        #   導致雙核 TM 上 busy=560ms（每核利用率 28%）的 subtask
        #   Z_busy 就超過 1.0，但 TM 整體 CPU 利用率只有 44%，
        #   並不是真正的 CPU 瓶頸。
        #   加入 TM CPU Z-score 條件後，只有 TM CPU 也統計異常高時才命中，
        #   排除「TM 很閒但 subtask 相對較忙」的假陽性。
        for sid, info in task_info.items():
            T_busy = info["T_busy"]
            T_bp   = info["T_bp"]
            tm_id  = subtask_locations.get(sid, "unknown")
            if sigma_busy >= SIGMA_MIN:
                Z_busy = (T_busy - mu_busy) / sigma_busy
                if Z_busy > Z_THRESHOLD and T_busy > T_bp:
                    # TM CPU 飽和率 Z-score
                    SIGMA_MIN_CPU = 0.05
                    tm_cpu_sat    = tm_cpu_sat_rates.get(tm_id, 0.0)
                    Z_cpu_sat     = ((tm_cpu_sat - mu_cpu) / sigma_cpu
                                     if sigma_cpu >= SIGMA_MIN_CPU else 0.0)
                    if Z_cpu_sat > Z_THRESHOLD:
                        free_time  = max(1.0 - T_bp, 0.001)
                        u_eff      = min(T_busy / free_time, 1.0)
                        d_overload = max(T_busy, u_eff)
                        explicit_set.append((sid, "CPU_BOTTLENECK", d_overload))
                        print(f"  [1c CPU] {sid}: Z_busy={Z_busy:.2f}>{Z_THRESHOLD}，"
                              f"T_busy={T_busy*1000:.0f}ms>T_bp={T_bp*1000:.0f}ms，"
                              f"Z_cpu_sat={Z_cpu_sat:.2f}>{Z_THRESHOLD}")
                    else:
                        print(f"  [1c SKIP] {sid}: Z_busy={Z_busy:.2f}>{Z_THRESHOLD} "
                              f"但 Z_cpu_sat={Z_cpu_sat:.2f}≤{Z_THRESHOLD}（TM CPU 正常），"
                              f"非真正 CPU 瓶頸")

        print(f"\n  ✅ 顯性瓶頸共 {len(explicit_set)} 個")
        return explicit_set

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 2：隱性瓶頸偵測（adjusted_headroom 統一公式）
    # ──────────────────────────────────────────────────────────────────────────

    def _detect_implicit(self, task_info, operator_groups, ordered_operators,
                         subtask_locations, explicit_set, tm_cpu_sat_rates):
        """
        隱性瓶頸偵測：語意 = 「顯性瓶頸解除後，最容易成為下一個瓶頸的 subtask」。

        設計依據（三個維度統一在一個 adjusted_headroom 公式中）：

        維度 A — subtask 自身 CPU 裕度：u_eff = T_busy / (1 - T_bp)
            排除反壓後的真實 CPU 利用率，(1 - u_eff) 代表自身還剩多少空間。

        維度 B — TM 單位負載（每核 busy time）：
            tm_unit_load = Σ T_busy（同 TM 所有 subtask）/ cpu_cores
            物理意義：剔除核心數差異後的真實 CPU 壓力。
            tm_unit_load_norm = tm_unit_load / max(叢集各 TM unit_load)
            歸一化到 [0, 1]，(1 - tm_unit_load_norm) 代表 TM 的資源裕度。
            ─ 為何不用舊的 tm_cpu_sat_rates（Σ T_busy / cores）？
              兩者計算方式相同，但此處在函式內重新計算是為了明確「歸一化」語意，
              避免 tm_cpu_sat_rates > 1.0 時公式出現負號。

        維度 C — 上游積壓折扣（流量衝擊預測）：
            upstream_pressure = median(上游算子各 subtask 的 outPool)
            當顯性瓶頸被遷走後，上游的 outPool 積壓瞬間釋放，
            流量衝擊直接打到下游同算子的其他 subtask 和更下游算子。
            outPool=1.0（完全積壓）折扣 50%；outPool=0.3 折扣 15%。
            係數 0.5 的意義：積壓完全解除時，預期流量增幅約為當前的一倍，
            headroom 折半是保守但可解釋的下界估計。

        最終公式：
            base_headroom     = (1 - u_eff) × (1 - tm_unit_load_norm)
            adjusted_headroom = base_headroom × (1 - upstream_pressure × 0.5)

        篩選方式：Tukey IQR 下界 fence（Tukey 1977）
            adjusted_headroom < Q1 - 1.5 × IQR
            代表該 subtask 的資源裕度在統計上顯著低於叢集其他非顯性 subtask，
            是分佈中的脆弱異常值。

        不排除顯性瓶頸的同算子 subtask：
            同算子 subtask 遷移後的負載平衡取決於目標 TM 的資源能力，
            若某 subtask 留在單核 TM（如 tm_20c_5），其 unit_load_norm 依然偏高，
            headroom 自然偏低，會被公式正確識別，不需要人工排除規則。

        回傳：[(subtask_id, "LATENT_CPU", 1 - adjusted_headroom), ...]
        """
        # ── Step A：TM 單位負載計算（每核 busy time，剔除核心數異質性）─────────
        # 用各 TM 上所有 subtask 的 T_busy 總和除以核心數，
        # 得到「每個 CPU 核心平均需要工作多少 ms/s」這個核心維度指標。
        _cpu_capacity = {
            "tm_20c_1": 2.0, "tm_20c_2": 2.0, "tm_20c_3": 2.0,
            "tm_20c_4": 1.0, "tm_20c_5": 1.0,
        }

        tm_total_busy = {}
        for sid, info in task_info.items():
            tm_id = subtask_locations.get(sid, "unknown")
            if tm_id == "unknown":
                continue
            tm_total_busy[tm_id] = tm_total_busy.get(tm_id, 0.0) + info["T_busy"]

        tm_unit_load = {}
        for tm_id, total_busy in tm_total_busy.items():
            cores = _cpu_capacity.get(tm_id, 1.0)
            # 單位：ms/core（0 ~ 1000+），值愈高代表每核心愈忙
            tm_unit_load[tm_id] = total_busy / cores

        # 歸一化到 [0, 1]，除以叢集最大值
        max_unit_load = max(tm_unit_load.values()) if tm_unit_load else 1.0

        print(f"\n  [TM 單位負載（ms/core）]  max={max_unit_load:.1f}ms")
        for tm_id, ul in sorted(tm_unit_load.items(), key=lambda x: -x[1]):
            norm = ul / max(max_unit_load, 0.001)
            print(f"    {tm_id}: unit_load={ul:.1f}ms  norm={norm:.3f}")

        # ── Step B：各算子的直接上游 outPool 積壓壓力 ──────────────────────────
        # 建立 op → 上游 op 的對照表（只看相鄰上游，不做多跳傳播）。
        # 上游 outPool 的中位數代表「一旦顯性瓶頸解除，這個算子將承接多少額外流量衝擊」。
        # 用中位數而非最大值：避免單個 subtask 的暫態峰值扭曲估計。
        upstream_of = {}
        for i in range(len(ordered_operators) - 1):
            upstream_of[ordered_operators[i + 1]] = ordered_operators[i]

        op_upstream_pressure = {}
        print(f"\n  [上游 outPool 積壓壓力]")
        for op_name in ordered_operators:
            up_op = upstream_of.get(op_name)
            if up_op:
                up_subtasks = operator_groups.get(up_op, [])
                up_outpools = [task_info[st]["out_pool_usage"]
                               for st in up_subtasks if st in task_info]
                pressure = statistics.median(up_outpools) if up_outpools else 0.0
            else:
                pressure = 0.0   # Source 算子無上游
            op_upstream_pressure[op_name] = pressure
            if up_op:
                print(f"    {op_name} ← {up_op}: upstream_outPool_median={pressure:.3f}")

        # ── Step C：計算每個 non-explicit subtask 的 adjusted_headroom ─────────
        # 排除條件：
        #   1. 顯性瓶頸本身（explicit_sids）：已確認需遷移，無需再計算 headroom。
        #   2. T_busy < 0.05（50ms/s）：Sink 等完全空閒的算子，u_eff 無意義。
        #
        # 不排除顯性瓶頸的同算子夥伴：
        #   若夥伴留在資源緊的 TM，tm_unit_load_norm 偏高，headroom 自然偏低，
        #   不需要人工排除規則，公式會正確反映其脆弱性。
        explicit_sids = {sid for sid, _, _ in explicit_set}

        headroom_list = []   # [(sid, adj_headroom, u_eff, unit_load_norm, upstream_p)]

        print(f"\n  [headroom 計算]  排除 explicit={len(explicit_sids)} 個")
        for sid, info in task_info.items():
            if sid in explicit_sids:
                continue   # 顯性瓶頸本身跳過

            op_name = info["task_name"]
            t_busy  = info["T_busy"]
            t_bp    = info["T_bp"]
            tm_id   = subtask_locations.get(sid, "unknown")

            # Sink 等完全空閒的算子跳過（T_busy 幾乎為 0，u_eff 沒有意義）
            if t_busy < 0.05:
                continue

            # 維度 A：subtask 自身 CPU 裕度
            free_time = max(1.0 - t_bp, 0.001)
            u_eff = min(t_busy / free_time, 1.0)

            # 維度 B：TM 單位負載歸一化
            unit_load_norm = tm_unit_load.get(tm_id, 0.0) / max(max_unit_load, 0.001)
            # 歸一化後 clamp 到 [0, 1]，避免因浮點誤差略超 1.0 導致 headroom 負值
            unit_load_norm = min(unit_load_norm, 1.0)

            # 基礎 headroom = CPU 裕度 × TM 資源裕度
            base_headroom = (1.0 - u_eff) * (1.0 - unit_load_norm)

            # 維度 C：上游積壓折扣
            # upstream_pressure ∈ [0, 1]；係數 0.5 意義：
            #   outPool=1.0 時折扣 50%（完全積壓解除後流量翻倍，headroom 減半）
            #   outPool=0.3 時折扣 15%（輕度積壓，影響有限）
            upstream_p = op_upstream_pressure.get(op_name, 0.0)
            adjusted_headroom = base_headroom * (1.0 - upstream_p * 0.5)

            headroom_list.append((sid, adjusted_headroom, u_eff, unit_load_norm, upstream_p))
            print(f"    {sid}: u_eff={u_eff:.3f}  unit_load_norm={unit_load_norm:.3f}"
                  f"  upstream_p={upstream_p:.3f}  base={base_headroom:.4f}"
                  f"  adj={adjusted_headroom:.4f}"
                  f"  [TM={tm_id}]")

        if len(headroom_list) < 2:
            print("  ⚠️ 可用 subtask 數不足（< 2），隱性偵測跳過")
            return []

        # ── Step D：篩選 headroom 最低的 subtask ────────────────────────────────
        #
        # 主策略（母體 ≥ 4）：IQR 下界 fence（Tukey 1977）
        #   low_fence = Q1 - 1.5 × IQR
        #   低於此值代表在非顯性 subtask 中資源最脆弱的統計異常值。
        #   IQR = 0（所有 headroom 完全相同）時改用 Q1 × 0.5 作為 fallback。
        #
        # 小母體 fallback（母體 < 4）：取最低 headroom 的 lowest-25%（至少 1 個）
        #   IQR 方法在 n < 4 時鑑別力極差：
        #     n=2 → Q1=Q3=中位數，IQR=0，fallback Q1×0.5 的 fence 值不穩定；
        #     n=3 → IQR 受單一極端值影響過大，fence 可能遠低於所有樣本。
        #   改用「最低 25%（ceiling）」：直接對最脆弱的那個 subtask 發出警告，
        #   不依賴分佈形狀假設，語意清晰（「這是目前裕度最低的 subtask」）。
        #   小母體本身代表顯性遷移後叢集已較均衡，隱性偵測保守一個就夠。
        import math
        h_sorted = sorted(x[1] for x in headroom_list)
        n_h      = len(h_sorted)

        if n_h >= 4:
            # 主策略：IQR Tukey fence
            q1_h      = _quantile_linear(h_sorted, 0.25)
            q3_h      = _quantile_linear(h_sorted, 0.75)
            iqr_h     = q3_h - q1_h
            low_fence = (q1_h - 1.5 * iqr_h) if iqr_h > 0 else (q1_h * 0.5)
            strategy  = f"IQR fence  Q1={q1_h:.4f} Q3={q3_h:.4f} IQR={iqr_h:.4f}"
        else:
            # 小母體 fallback：最低 25%（ceiling），至少 1 個
            k         = max(1, math.ceil(n_h * 0.25))
            low_fence = h_sorted[k - 1]   # threshold = 第 k 小的值
            strategy  = f"小母體 fallback（n={n_h}）→ lowest-{k} 個"

        print(f"\n  [headroom 篩選]  策略={strategy}  low_fence={low_fence:.4f}")

        implicit_set = []
        for sid, adj_h, u_eff, unit_load_norm, upstream_p in headroom_list:
            if adj_h <= low_fence:
                # d_overload 用 (1 - adjusted_headroom)，使其與顯性的 d_overload 語意一致：
                # 值愈接近 1.0 代表裕度愈低、過載風險愈高。
                d_overload = 1.0 - adj_h
                # 存回 task_info，供 get_prioritized_list 直接讀取，
                # 避免該函式被迫重算單維 u_eff 而丟失 TM 負載與上游積壓折扣資訊。
                task_info[sid]["_latent_d_overload"] = d_overload
                implicit_set.append((sid, "LATENT_CPU", d_overload))
                print(f"  [LATENT_CPU] {sid}: adj_headroom={adj_h:.4f} ≤ fence={low_fence:.4f}"
                      f"  d_overload={d_overload:.4f}")

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

    # ══════════════════════════════════════════════════════════════════════════
    # v10 Section III-C：最佳遷移時間選擇
    # ══════════════════════════════════════════════════════════════════════════

    def _record_source_rate(self, task_info):
        """
        從 task_info 取出所有 Source subtask 的 observed_output_rate 加總，
        記錄至 _source_rate_history。

        propose 不使用 CAOM 的 BFS 反推，直接用 observed_output_rate（Source
        在無下游反壓時 observed 即等於 actual），這在 propose 的偵測假設下
        語意正確。
        """
        source_rates = [
            info.get("observed_output_rate", 0.0)
            for info in task_info.values()
            if "source" in info.get("task_name", "").lower()
        ]
        if source_rates:
            total = sum(source_rates)
            self._source_rate_history.append(total)
            if len(self._source_rate_history) > self._history_max_len:
                self._source_rate_history.pop(0)
            print(f"📈 [v10-Timing] Source 產生率已記錄: {total:.1f} B/s "
                  f"(歷史筆數: {len(self._source_rate_history)}/{self._history_max_len})")

    def _record_state_sizes(self, task_info):
        """
        記錄各 subtask 的 state size 歷史，供 _is_state_decreasing 判斷趨勢。
        Window 算子的 state 在 window 結束時最小，下降趨勢代表可能即將到谷底。
        """
        for sid, info in task_info.items():
            sz = info.get("state_size", 0)
            if sz > 0:
                if sid not in self._state_size_history:
                    self._state_size_history[sid] = []
                hist = self._state_size_history[sid]
                hist.append(sz)
                if len(hist) > self._state_history_len:
                    hist.pop(0)

    def _is_state_decreasing(self, bottleneck_subtasks):
        """
        判斷瓶頸 subtask 的 state size 是否整體處於下降趨勢。

        物理意義（對應 StateAware 精神）：
          Window 算子每 1 秒一個 rolling window，state 在 window 結束瞬間最小。
          若最近 N 筆 state 呈下降趨勢，代表 window 快滾完，此時遷移 state 最小。

        判斷方式：
          對每個瓶頸 subtask，若最新值 < 歷史最大值 × 0.8（已縮小 20%），
          視為下降趨勢。超過半數瓶頸 subtask 呈下降時回傳 True。

        回傳: (is_decreasing: bool, info_str: str)
        """
        if not bottleneck_subtasks:
            return False, "無瓶頸 subtask"

        decreasing_count = 0
        details = []
        for sid, _, _ in bottleneck_subtasks:
            hist = self._state_size_history.get(sid, [])
            if len(hist) < 2:
                details.append(f"{sid}: 歷史不足")
                continue
            latest   = hist[-1]
            peak     = max(hist)
            ratio    = latest / peak if peak > 0 else 1.0
            is_dec   = ratio < 0.8   # 縮小超過 20% 視為下降趨勢
            if is_dec:
                decreasing_count += 1
            details.append(f"{sid}: latest={latest//1024}KB peak={peak//1024}KB "
                           f"ratio={ratio:.2f} {'↓' if is_dec else '→'}")

        total = len(bottleneck_subtasks)
        is_decreasing = decreasing_count > total / 2
        info_str = f"{decreasing_count}/{total} subtask state 下降; " + "; ".join(details)
        return is_decreasing, info_str

    def _predict_source_rates(self, horizon_sec, sample_interval_sec):
        """
        用指數平滑（含線性趨勢修正）預測未來 horizon_sec 秒的 Source 資料產生率。

        history 中每一筆代表 sample_interval_sec 秒的觀測值；輸出以「秒」為單位
        （長度 = horizon_sec），使搜尋步長可以獨立設定，不受監控週期長短影響。

        趨勢斜率換算成「每秒變化量」，消除 sample_interval_sec 的影響。
        """
        history = self._source_rate_history
        n_hist  = len(history)

        if n_hist < 5:
            last_val = history[-1] if history else 0.0
            return [last_val] * horizon_sec

        # 指數平滑（alpha=0.3，偏向歷史穩定值）
        alpha    = 0.3
        smoothed = history[0]
        for val in history[1:]:
            smoothed = alpha * val + (1.0 - alpha) * smoothed

        # 線性趨勢：換算為每秒斜率
        trend_window   = min(10, n_hist)
        recent         = history[-trend_window:]
        total_sec      = max((trend_window - 1) * sample_interval_sec, 1)
        trend_per_sec  = (recent[-1] - recent[0]) / total_sec

        # 逐秒預測，趨勢隨秒數指數衰減避免長期失真
        predictions = []
        base = smoothed
        for sec in range(horizon_sec):
            base = base + trend_per_sec * (0.9 ** sec)
            predictions.append(max(0.0, base))
        return predictions

    def _estimate_migration_duration(self, bottleneck_subtasks):
        """
        估算遷移持續時間 T_Ω（秒）。

        對應論文公式 (3)：
          T_Ω = max(serialize) + max(deserialize) + 2×Σ Ser(S_i)/B0 + σ

        propose 只遷移瓶頸 subtask（不像 baseline 全部遷），
        因此以「所有待遷移 subtask 中最大的 state」計算傳輸時間（Flink 並行序列化）。

        B0 取遷移 subtask 所在 TM 中最慢的頻寬限制（保守估計）。
        """
        NETWORK_BW_BYTES_PER_SEC = 6.25 * 1024 * 1024  # 50 Mbps（最慢 TM 的保守下界）
        SERIALIZE_OVERHEAD_SEC   = 2.0
        ALIGNMENT_OVERHEAD_SEC   = 1.0

        max_state_bytes = 0
        if bottleneck_subtasks and self._task_info:
            for sid, _, _ in bottleneck_subtasks:
                sz = self._task_info.get(sid, {}).get("state_size", 0)
                if sz > max_state_bytes:
                    max_state_bytes = sz

        if max_state_bytes > 0:
            transfer_sec = (2.0 * max_state_bytes) / NETWORK_BW_BYTES_PER_SEC
            estimated    = SERIALIZE_OVERHEAD_SEC + transfer_sec + ALIGNMENT_OVERHEAD_SEC
        else:
            estimated = SERIALIZE_OVERHEAD_SEC + ALIGNMENT_OVERHEAD_SEC

        result = max(3.0, estimated)
        print(f"⏱️  [v10-Timing] 遷移時間估算: {result:.1f}s "
              f"(max_state={max_state_bytes//(1024*1024) if max_state_bytes >= 1024*1024 else max_state_bytes//1024}{'MB' if max_state_bytes >= 1024*1024 else 'KB'})")
        return result

    def _find_optimal_migration_start(self, migration_duration_sec, monitor_interval_sec):
        """
        以「秒」為搜尋單位，在預測窗口內找最佳遷移起始時間。

        搜尋步長固定 SEARCH_STEP_SEC=5 秒，與 monitor_interval_sec 完全無關，
        使決策精度不受監控週期長短影響，可精確對準資料率谷底。

        對應論文公式 (8) 與 Theorem 1：
          min Σ_{k=s}^{e} D_{S,k}   (s.t. e = s + T_Ω)

        積壓項（公式 5）：
          Σ_{k<s} max(0, D_{S,k} - Δ)  ← 等待期間超出消費能力的積壓

        回傳: (best_sec, best_cost, immediate_cost)
          best_sec      : 最佳起始秒數（0 = 立即遷移）
          best_cost     : 最佳時間的積壓成本
          immediate_cost: 立即遷移的積壓成本（基準）
        """
        SEARCH_STEP_SEC = 5

        if len(self._source_rate_history) < 5:
            return 0, float('inf'), float('inf')

        # 預測窗口：至少 3 倍遷移時長，最少 120 秒
        horizon_sec     = max(int(migration_duration_sec * 3), 120)
        predicted_rates = self._predict_source_rates(horizon_sec, monitor_interval_sec)

        # Δ：pipeline 最小消費能力（用非瓶頸 subtask max_capacity 最小值）
        delta = 0.0
        if self._task_info:
            bottleneck_sids = {s for s, _, _ in self._bottleneck_subtasks}
            caps = [
                info.get("max_capacity", 0.0)
                for sid, info in self._task_info.items()
                if sid not in bottleneck_sids and info.get("max_capacity", 0.0) > 0
            ]
            if caps:
                delta = min(caps)

        dur = int(migration_duration_sec)

        def compute_cost(start_sec):
            end_sec        = min(start_sec + dur, len(predicted_rates) - 1)
            migration_data = sum(predicted_rates[start_sec:end_sec + 1])
            pre_backlog    = sum(max(0.0, r - delta) for r in predicted_rates[:start_sec])
            return migration_data + pre_backlog

        immediate_cost = compute_cost(0)
        best_sec  = 0
        best_cost = immediate_cost

        for start_sec in range(SEARCH_STEP_SEC,
                               max(len(predicted_rates) - dur, 1),
                               SEARCH_STEP_SEC):
            cost = compute_cost(start_sec)
            if cost < best_cost:
                best_cost = cost
                best_sec  = start_sec

        print(f"🔍 [v10-Timing] 最佳時間搜尋:")
        print(f"   預測窗口    : {horizon_sec}s，步長 {SEARCH_STEP_SEC}s")
        print(f"   Δ (pipeline 消費率): {delta:.1f} B/s")
        print(f"   立即遷移成本: {immediate_cost:.1f}")
        print(f"   最佳起始時間: +{best_sec}s，成本: {best_cost:.1f}")
        return best_sec, best_cost, immediate_cost

    def _is_cooldown_done(self, current_latency_ms=None):
        """
        動態冷卻判斷：三種情況允許提前或正常結束冷卻期。

        情況 A — latency 已回落（< LATENCY_RECOVERED）：
            遷移成功，系統已穩定，可以提前結束，準備應對下一個瓶頸。

        情況 B — latency 持續惡化（> LATENCY_WORSENING）：
            遷移計畫不完整（例如只遷了部分瓶頸），或遷移目標選錯，
            需要允許提前再試一次，不應強迫等滿 180 秒。
            門檻設 50 秒（50000ms），避免遷移後的暫態峰值誤觸。

        情況 C — 等滿 migration_cooldown（預設 180 秒）：
            latency 在中間懸著（10~50 秒），觀察期結束，正常觸發。

        絕對最短冷卻 MIN_COOLDOWN_SEC=60 秒在所有情況下都強制執行，
        給系統足夠時間讓 savepoint/restore 完成並恢復穩定。
        """
        MIN_COOLDOWN_SEC   = 60
        LATENCY_RECOVERED  = 10000   # ms：回落門檻，低於此視為成功
        LATENCY_WORSENING  = 50000   # ms：惡化門檻，高於此視為需要再試

        elapsed = time.time() - self.last_migration_time
        if elapsed < MIN_COOLDOWN_SEC:
            return False

        if current_latency_ms is not None:
            if current_latency_ms < LATENCY_RECOVERED:
                print(f"✅ [v10-Cooldown] 情況A：latency 已回落 "
                      f"{current_latency_ms:.0f}ms < {LATENCY_RECOVERED}ms，"
                      f"提前結束冷卻（已等 {elapsed:.0f}s）")
                return True
            if current_latency_ms > LATENCY_WORSENING:
                print(f"⚠️  [v10-Cooldown] 情況B：latency 持續惡化 "
                      f"{current_latency_ms:.0f}ms > {LATENCY_WORSENING}ms，"
                      f"允許提前再遷移（已等 {elapsed:.0f}s）")
                return True

        if elapsed >= self.migration_cooldown:
            print(f"✅ [v10-Cooldown] 情況C：等滿冷卻期 {elapsed:.0f}s >= "
                  f"{self.migration_cooldown}s")
            return True
        return False

    def _log_timing_decision(self, best_sec, best_cost, immediate_cost,
                             improvement_ratio, decision):
        """將時間選擇決策寫入 detail_log CSV。"""
        try:
            detail_exists = os.path.isfile(self.detail_log)
            with open(self.detail_log, "a", newline="") as f:
                writer = csv.writer(f)
                if not detail_exists:
                    writer.writerow(["timestamp", "decision_step", "subtask_id",
                                     "priority_rank", "from_tm", "to_tm", "decision_reason"])
                writer.writerow([
                    time.time(), "TimingDecision", "ALL_BOTTLENECKS", "", "", "",
                    (f"v10_TIMING: decision={decision}, best_sec={best_sec}s, "
                     f"immediate_cost={immediate_cost:.1f}, best_cost={best_cost:.1f}, "
                     f"improvement={improvement_ratio * 100:.1f}%")
                ])
        except Exception as e:
            print(f"⚠️ 時間決策記錄失敗: {e}")

    def _write_log(self, task_info, subtask_locations):
        """將本輪所有 subtask 狀態寫入 CSV log。"""
        _CAUSE_TO_STATUS = {
            "CPU_BOTTLENECK":       "CPU_Bottleneck",
            "NETWORK_BOTTLENECK":   "NET_Bottleneck",
            "NETWORK_TM_SATURATED": "NET_Bottleneck",
            "LATENT_CPU":           "Latent_Bottleneck",
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

    def _compute_d_overload(self, sid, cause, bottleneck_sids):
        """
        計算單一 subtask 的 D_overload，供 evaluate_migration_trigger 和
        get_prioritized_list 共用，確保兩個函式使用完全相同的指標體系。

        ── 設計原則 ────────────────────────────────────────────────────────────
        D_overload 定義為「相比健康同儕基準，每秒超出的損失時間比例」，
        再正規化到 [0, 1]，使 NETWORK 和 CPU 在跨類型排序時量綱一致。

        NETWORK_BOTTLENECK：
          橋接量 = T_bp（每秒因網路積壓損失的處理時間，單位 s/s）
          基準   = 同算子非瓶頸 subtask 的 T_bp 中位數
                   → fallback：全局 T_bp < 0.5 的健康 subtask 中位數
          正規化 = raw / (1 - T_bp_safe)，代表「可惡化空間內已超出多少比例」
          語意   = 0 表示剛好達到健康水位；1 表示反壓已達理論最大值

        CPU_BOTTLENECK：
          橋接量 = T_busy（每秒主動消耗的 CPU 時間，單位 s/s）
          基準   = 同算子非瓶頸 subtask 的 T_busy 中位數
                   → fallback：全局 T_busy 中位數
          正規化 = raw / (1 - T_busy_safe)，語意同上

        LATENT_CPU：
          沿用 _detect_implicit 已計算的三維合成指標 (1 - adjusted_headroom)，
          值域本身已是 [0, 1]，不需另行正規化。

        W_eval 在此不乘入：W_eval 是所有 subtask 共用的常數，
        對排序結果沒有影響，省略以保持 D_overload 語意純粹。
        """
        info      = self._task_info.get(sid, {})
        task_name = info.get("task_name", "")

        # 同算子「健康（非瓶頸）」subtask 的 info 列表
        healthy_peer_infos = [
            pi for ps, pi in self._task_info.items()
            if pi.get("task_name") == task_name and ps not in bottleneck_sids
        ]

        if cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
            T_bp = info.get("T_bp", 0.0)

            if healthy_peer_infos:
                # 第一層：同算子健康同儕中位數
                T_bp_safe = statistics.median(
                    [pi.get("T_bp", 0.0) for pi in healthy_peer_infos]
                )
                safe_src = "健康同儕"
            else:
                # 第二層 fallback：全局 T_bp < 0.5 的 subtask 中位數
                # （整個算子全部淪陷，退回全局健康水位）
                global_healthy = [
                    pi.get("T_bp", 0.0)
                    for pi in self._task_info.values()
                    if pi.get("T_bp", 0.0) < 0.5
                ]
                T_bp_safe = statistics.median(global_healthy) if global_healthy else 0.0
                safe_src  = "全局fallback"

            raw = max(0.0, T_bp - T_bp_safe)
            # 正規化：超出健康基準後，剩餘可惡化空間已用掉多少
            headroom = max(1.0 - T_bp_safe, 0.001)
            d_overload = min(raw / headroom, 1.0)
            detail = f"T_bp={T_bp:.3f} safe={T_bp_safe:.3f}({safe_src}) raw={raw:.3f}"

        elif cause == "CPU_BOTTLENECK":
            T_busy = info.get("T_busy", 0.0)

            if healthy_peer_infos:
                T_busy_safe = statistics.median(
                    [pi.get("T_busy", 0.0) for pi in healthy_peer_infos]
                )
                safe_src = "健康同儕"
            else:
                # fallback：全局 T_busy 中位數
                global_all = [pi.get("T_busy", 0.0)
                              for pi in self._task_info.values()]
                T_busy_safe = statistics.median(global_all) if global_all else 0.0
                safe_src    = "全局fallback"

            raw = max(0.0, T_busy - T_busy_safe)
            headroom   = max(1.0 - T_busy_safe, 0.001)
            d_overload = min(raw / headroom, 1.0)
            detail = f"T_busy={T_busy:.3f} safe={T_busy_safe:.3f}({safe_src}) raw={raw:.3f}"

        else:
            # LATENT_CPU：三維合成值，值域已是 [0, 1]，直接使用
            d_overload = info.get("_latent_d_overload", 0.0)
            detail     = f"latent_risk={d_overload:.3f}"

        return d_overload, detail

    def get_prioritized_list(self, filtered_ids):
        """
        多維度優先級排序：Priority_Score = D_overload × (1 + R_impact)

        D_overload 由 _compute_d_overload() 統一計算，與 evaluate_migration_trigger
        使用完全相同的指標體系，確保觸發評估和排序決策語意一致：

          NETWORK_* → 正規化後的 T_bp 超出健康基準幅度，值域 [0, 1]
          CPU_*     → 正規化後的 T_busy 超出健康基準幅度，值域 [0, 1]
          LATENT_*  → 1 - adjusted_headroom（三維合成），值域 [0, 1]

        兩者統一到 [0, 1] 後跨類型比較量綱一致，排序結果不受原始指標值域差異影響。
        W_eval 不乘入：W_eval 是常數，不影響排序，省略保持語意純粹。
        """
        if not filtered_ids:
            return []

        # 本輪所有瓶頸 subtask 的 sid 集合，供 _compute_d_overload 識別健康同儕
        bottleneck_sids = set(filtered_ids)

        entries = []
        print(f"\n🎯 多維度優先級計算 (Priority = D_overload × (1 + R_impact)):")
        print(f"{'Subtask ID':<60} {'Cause':<22} {'D_overload':<12} {'R_impact':<10} "
              f"{'Priority':<10} {'Detail'}")
        print("=" * 130)

        for sid in filtered_ids:
            if sid not in self._task_info:
                continue
            info  = self._task_info[sid]
            cause = info.get("bottleneck_cause", "CPU_BOTTLENECK")

            d_overload, detail = self._compute_d_overload(sid, cause, bottleneck_sids)
            r_impact            = self.calculate_topology_impact(sid)
            priority_score      = d_overload * (1.0 + r_impact)

            entries.append({
                "subtask_id":    sid,
                "cause":         cause,
                "d_overload":    d_overload,
                "r_impact":      r_impact,
                "priority_score": priority_score,
            })
            print(f"{sid:<60} {cause:<22} {d_overload:>10.3f}  "
                  f"{r_impact:>8.2f}  {priority_score:>8.3f}  {detail}")

        entries.sort(key=lambda x: x["priority_score"], reverse=True)
        prioritized_list = [(e["subtask_id"], e["priority_score"]) for e in entries]
        print(f"\n📊 優先級排序完成: {len(prioritized_list)} 個 Subtask")
        return prioritized_list

    # ══════════════════════════════════════════════════════════════════════════
    # 遷移觸發評估
    # ══════════════════════════════════════════════════════════════════════════

    def evaluate_migration_trigger(self, bottleneck_subtasks, monitor_interval_sec=30):
        """
        Job 級別全局評估：時間領域成本效益分析（v10 三態回傳版）。

        回傳: (decision, reason, best_sec)
          decision  : "REJECT" | "EXECUTE_NOW" | "SCHEDULE_DELAY"
          reason    : 人類可讀說明字串
          best_sec  : SCHEDULE_DELAY 時的建議等待秒數；其他情況為 0

        ── 第一層：C_stay vs C_mig（值不值得遷移）────────────────────────────
        C_mig = T_fixed + max(各待遷移 subtask 狀態大小) / BW_internal
                Flink savepoint/restore 是 job 級別一次性操作，狀態並行傳輸，
                瓶頸是最大的那份狀態，與遷移幾個 subtask 無關。

        C_stay = Σ_groups max_in_group( max(0, d_overload - d_safe) × W_eval )
                分組 key 依根因獨立性（與 v9 相同）。

        W_eval = migration_cooldown - C_mig（論文原始定義，不再硬編碼 180）

        ── 第二層：時間選擇（什麼時候成本最低）───────────────────────────────
        通過第一層後，進行以下評估：
          1. State 下降趨勢：若 Window 算子 state 在縮小（快到 window 底），
             延遲到 state 谷底可同時降低傳輸量（StateAware 精神）。
          2. 資料率谷底搜尋：_find_optimal_migration_start() 在預測窗口內
             找積壓成本最小的 5 秒精度起始時間。
          兩個條件任一滿足改善幅度 > IMPROVEMENT_THRESHOLD，返回 SCHEDULE_DELAY。
        """
        IMPROVEMENT_THRESHOLD = 0.05   # 5% 改善才值得延遲
        MIN_HISTORY_FOR_PREDICT = 10   # 預測所需最少歷史筆數

        if not bottleneck_subtasks:
            return "REJECT", "無任何瓶頸，系統健康", 0

        # ── C_mig：並行傳輸，瓶頸是最大狀態 ──────────────────────────────────
        max_state_mb = max(
            self._task_info.get(sid, {}).get("state_size", 0.0) / (1024 * 1024)
            for sid, _, _ in bottleneck_subtasks
        )
        T_fixed      = 4.0
        BW_internal  = 150.0 / 8    # 150 Mbps = 18.75 MB/s（最慢 TM 頻寬）
        C_mig        = T_fixed + (max_state_mb / BW_internal)
        # W_eval：不遷移時在下次可觸發前持續承受損失的時間窗口（論文定義）
        W_eval       = max(self.migration_cooldown - C_mig, 1.0)

        current_locations = self.get_subtask_locations()

        # ── C_stay：組內取最大，組間加總 ─────────────────────────────────────
        #
        # 設計對齊說明：
        #   C_stay 與 get_prioritized_list 共用 _compute_d_overload() 計算 D_overload。
        #   D_overload 已正規化到 [0, 1]，代表「可惡化空間內已超出健康基準的比例」：
        #     NETWORK → T_bp 超出健康同儕基準，再除以剩餘可惡化空間
        #     CPU     → T_busy 超出健康同儕基準，再除以剩餘可惡化空間
        #
        #   C_stay = D_overload × W_eval
        #   量綱推導：D_overload 是無單位比例（0~1），W_eval 是秒，
        #   但 D_overload 的分子（T_bp 或 T_busy 超出量）本身就是「秒/秒」的時間比例，
        #   所以 C_stay 的物理語意是「W_eval 秒內，相比健康水位多損失的處理時間佔比
        #   乘以窗口長度」，量綱等效為秒，可與 C_mig（秒）直接比較。
        #
        #   d_safe 已內建在 _compute_d_overload 的 raw = max(0, metric - safe) 裡，
        #   此處不再另外設 d_safe，D_overload = 0 時貢獻自然為 0。
        #
        # 分組規則（根因獨立性原則）：
        #   NETWORK_* → "NET:{tm_id}"   同 TM 共享頻寬，損失不疊加，組內取最大
        #   CPU_*     → "CPU:{task}"    同算子競爭同批核心，損失不疊加，組內取最大
        #   LATENT_*  → "LATENT:{task}" 獨立預防性評估
        #   組間加總：不同物理根因損失相互獨立
        bottleneck_sids = {sid for sid, _, _ in bottleneck_subtasks}
        group_max = {}

        print(f"\n  [TRIGGER v10 時間成本分析]")
        print(f"  - 停機代價  (C_mig) : {C_mig:.2f}s "
              f"(最大狀態 {max_state_mb:.1f}MB，並行傳輸)")
        print(f"  - W_eval            : {W_eval:.1f}s "
              f"(cooldown={self.migration_cooldown}s - C_mig={C_mig:.1f}s)")
        print(f"  {'Subtask':<45} {'Cause':<22} {'Group':<22} "
              f"{'D_overload':>10} {'C_stay貢獻':>10} {'Detail'}")
        print("  " + "-" * 130)

        for subtask_id, cause, _ in bottleneck_subtasks:
            info      = self._task_info.get(subtask_id, {})
            task_name = info.get("task_name", "")
            tm_id     = current_locations.get(subtask_id, "unknown")

            # 與 get_prioritized_list 共用同一個計算函式
            d_overload, detail = self._compute_d_overload(
                subtask_id, cause, bottleneck_sids
            )

            if cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                group_key = f"NET:{tm_id}"
            elif cause == "CPU_BOTTLENECK":
                group_key = f"CPU:{task_name}"
            else:
                group_key = f"LATENT:{task_name}"

            contribution = d_overload * W_eval
            print(f"  {subtask_id:<45} {cause:<22} {group_key:<22} "
                  f"{d_overload:>10.3f} {contribution:>9.2f}s  {detail}")

            if group_key not in group_max or contribution > group_max[group_key]:
                group_max[group_key] = contribution

        C_stay = sum(group_max.values())

        print(f"\n  [分組彙總]")
        for gk, gv in sorted(group_max.items()):
            print(f"    {gk:<30} {gv:.2f}s")
        print(f"  - 延遲代價  (C_stay): {C_stay:.2f}s（組內取最大，組間加總）")

        # ── 第一層判斷：值不值得遷移 ────────────────────────────────────────
        if C_stay <= C_mig:
            print(f"  ❌ [REJECT] C_mig ({C_mig:.2f}s) >= C_stay ({C_stay:.2f}s)，維持現狀")
            return "REJECT", f"C_mig({C_mig:.1f}s) >= C_stay({C_stay:.1f}s)", 0

        print(f"  ✅ [值得遷移] C_stay ({C_stay:.2f}s) > C_mig ({C_mig:.2f}s)")

        # ── 第二層判斷：什麼時候成本最低 ────────────────────────────────────
        print(f"\n  [v10 時間選擇分析]")
        n_history = len(self._source_rate_history)

        if n_history < MIN_HISTORY_FOR_PREDICT:
            print(f"  歷史資料不足 ({n_history}/{MIN_HISTORY_FOR_PREDICT} 筆)，立即執行（冷啟動）")
            self._log_timing_decision(0, 0, 0, 0.0, "EXECUTE_NOW_COLDSTART")
            return "EXECUTE_NOW", f"冷啟動（歷史{n_history}筆不足{MIN_HISTORY_FOR_PREDICT}）", 0

        migration_duration_sec = self._estimate_migration_duration(bottleneck_subtasks)

        # ── 子評估 A：State 下降趨勢（Window 算子谷底）──────────────────────
        state_dec, state_info = self._is_state_decreasing(bottleneck_subtasks)
        print(f"  State 趨勢: {'↓ 下降中' if state_dec else '→ 穩定/上升'} — {state_info}")

        # ── 子評估 B：資料率谷底搜尋 ─────────────────────────────────────────
        best_sec, best_cost, immediate_cost = self._find_optimal_migration_start(
            migration_duration_sec, monitor_interval_sec
        )
        if immediate_cost > 0:
            rate_improvement = (immediate_cost - best_cost) / immediate_cost
        else:
            rate_improvement = 0.0

        print(f"\n  {'─'*58}")
        print(f"  歷史資料筆數  : {n_history} 筆（取樣間隔 {monitor_interval_sec}s）")
        print(f"  遷移預計耗時  : {migration_duration_sec:.1f}s")
        print(f"  立即遷移成本  : {immediate_cost:.1f}")
        print(f"  最佳時間成本  : {best_cost:.1f}（+{best_sec}s 後）")
        print(f"  資料率改善幅度: {rate_improvement * 100:.1f}% "
              f"(門檻: {IMPROVEMENT_THRESHOLD * 100:.0f}%)")
        print(f"  State 下降趨勢: {'是' if state_dec else '否'}")
        print(f"  {'─'*58}")

        # 決策：State 下降或資料率有顯著改善 → 排程等待
        should_delay = False
        delay_reason = ""

        if state_dec:
            should_delay = True
            delay_reason = f"State 下降趨勢（等 window 谷底降低傳輸量）"

        if best_sec > 0 and rate_improvement > IMPROVEMENT_THRESHOLD:
            should_delay = True
            rate_reason  = (f"資料率谷底在 +{best_sec}s "
                            f"（節省 {rate_improvement*100:.1f}%）")
            delay_reason = (f"{delay_reason}; {rate_reason}"
                            if delay_reason else rate_reason)

        if should_delay:
            # 如果 state 在下降但資料率搜尋建議立即（best_sec=0），
            # 設一個最短等待 10 秒，給 state 多一輪時間縮小
            effective_best_sec = best_sec if best_sec > 0 else 10
            print(f"  ⏸️  [SCHEDULE_DELAY] 等待 {effective_best_sec}s: {delay_reason}")
            self._log_timing_decision(effective_best_sec, best_cost, immediate_cost,
                                      rate_improvement, "SCHEDULE_DELAY")
            return "SCHEDULE_DELAY", delay_reason, effective_best_sec
        else:
            print(f"  🚀 [EXECUTE_NOW] 現在是最佳時機（改善不顯著且 state 穩定）")
            self._log_timing_decision(0, best_cost, immediate_cost,
                                      rate_improvement, "EXECUTE_NOW")
            return "EXECUTE_NOW", "現在是最佳時機", 0

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
            return None, {}, {}

        bottleneck_info = self._task_info[bottleneck_subtask_id]
        _, downstream_keywords = self.get_neighbors(bottleneck_subtask_id)
        if not downstream_keywords:
            return None, {}, {}

        downstream_subtasks = [
            sid for sid, info in self._task_info.items()
            if any(kw in info["task_name"] for kw in downstream_keywords)
        ]
        if not downstream_subtasks:
            return None, {}, {}

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
                return None, {}, {}
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
                # [FIX-3] Case A（原地不動）：
                # resource_map 初始化時已預扣除瓶頸 subtask 自身的流量，
                # 若留在原 TM，必須把自身流量加回來才能正確估算頻寬壓力。
                # 原本 expected_network_added = 0.0 會低估原地選項的實際佔用，
                # 使「原地不動」選項偏向通過頻寬約束檢查。
                expected_network_added = b_total_traffic
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
            # [FIX-4] 加入 Slot 上限檢查，避免回傳已滿槽的 TM
            # MAX_SLOTS_PER_TM 與 generate_migration_plan 保持一致（皆為 5）
            _MAX_SLOTS = 5
            best_rid, max_remaining = None, -float('inf')
            for rid, res in resource_map.items():
                if res['slots'] + 1 > _MAX_SLOTS:   # FIX-4
                    continue
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
                return fb_tm, affinity_scores, {}  # fallback 路徑不提供 net_added_map，由呼叫端使用 subtask_traffic
            else:
                print(f"   ⚠️ [{bottleneck_subtask_id}] Step 4 — 無有效 TM，退回 CPU 負載均衡")
                return None, affinity_scores, {}

        best_tm = max(valid_tms, key=lambda rid: valid_tms[rid])
        print(f"   ✅ [{bottleneck_subtask_id}] Step 4 — "
              f"拓撲親和力選定目標 TM: {best_tm} (AffinityScore={valid_tms[best_tm]/1e6:.4f}MB/s)")

        # [FIX-10] 計算每個通過 Step 3 的 TM 的淨增量，供 generate_migration_plan
        # 用同一個值更新 resource_map，確保 Step 3 通過門檻 ↔ resource_map 帳目一致。
        net_added_map = {}
        for rid, affinity in valid_tms.items():
            if rid == b_current_tm_id:
                net_added_map[rid] = b_total_traffic           # Case A：加回自身完整流量
            else:
                net_added_map[rid] = b_total_traffic - 2.0 * affinity  # Case B：扣掉親和力節省
        return best_tm, affinity_scores, net_added_map

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

        # 初始化資源地圖（預扣除瓶頸 subtask，保留靜態 subtask 的已用資源）
        # [FIX-1] 流量統一改用 observed_output_rate（出口速率），
        #         與 tc qdisc tbf 限制 eth0 出口的物理語意一致，
        #         避免 Window 算子的大量 obs_in 被誤算為 TM 出口負擔。
        resource_map = {}
        for rid, info in tm_info.items():
            static_slots = static_busy = static_net = 0
            for sid, loc_rid in current_locations.items():
                if loc_rid == rid and sid not in prioritized_subtask_ids and sid in self._task_info:
                    static_slots += 1
                    static_busy  += self._task_info[sid]['T_busy'] * 1000
                    static_net   += self._task_info[sid]['observed_output_rate']   # FIX-1
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

        # ── Backpressure-aware traffic inflation ──────────────────────────────
        # Under heavy backpressure (avg_bp > 0.6), all Prometheus traffic metrics
        # collapse to near-zero, causing severe underestimation of actual traffic
        # once the job restarts at full speed. Inflate by min(1/(1-avg_bp), 5.0).
        bp_vals = [info['T_bp'] for info in self._task_info.values()]
        avg_bp = sum(bp_vals) / len(bp_vals) if bp_vals else 0.0
        bp_mult = min(1.0 / max(1.0 - avg_bp, 0.01), 5.0) if avg_bp > 0.6 else 1.0
        if bp_mult > 1.0:
            print(f"\n⚠️ 系統反壓 avg_bp={avg_bp:.2%}，流量估算倍率 ×{bp_mult:.2f}")
            for res in resource_map.values():
                res['network_traffic'] *= bp_mult

        # ── Hard operator count limit for narrow-BW TMs ───────────────────────
        # Window_Join (Q7) needs ~12 MB/s/subtask; Window_Auction_Count (Q5)
        # needs ~32 MB/s/subtask. Narrow TMs (≤20 MB/s) cannot safely host >1
        # Window_Join or any Window_Auction_Count subtask.
        NARROW_BW_THRESHOLD = int(20 * 1024 * 1024)
        HEAVY_OP_LIMITS = {"Window_Join": 1, "Window_Auction_Count": 1}
        narrow_tms = {rid for rid, lim in self.tm_bandwidth_map.items()
                      if lim <= NARROW_BW_THRESHOLD}
        heavy_op_count = {rid: 0 for rid in resource_map}
        for sid, loc_rid in current_locations.items():
            if sid in prioritized_subtask_ids or loc_rid not in heavy_op_count:
                continue
            task_nm = self._task_info.get(sid, {}).get('task_name', '')
            for kw in HEAVY_OP_LIMITS:
                if kw in task_nm:
                    heavy_op_count[loc_rid] += 1
                    break

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
                    # [FIX-8] 更新 pending_locations，供後續 subtask 的親和力計算感知 bundle 結果
                    pending_locations[upstream_id]   = best_bundle_tm
                    pending_locations[downstream_id] = best_bundle_tm
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
                            # [FIX-6] bundle fallback 同樣改用 observed_output_rate
                            resource_map[orig_rid]['network_traffic'] += sd.get('observed_output_rate', 0) * 1.2
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
            # [FIX-6] 流量估算改用 observed_output_rate（出口速率），對應 tc tbf 出口限速
            # 乘以 bp_mult 補償反壓期間 Prometheus 指標崩塌導致的嚴重低估
            subtask_traffic = task_detail['observed_output_rate'] * 1.2 * bp_mult
            per_busy_rate   = busy_time_ms * cpu_limit

            # Network 瓶頸優先使用拓撲親和力演算法
            network_affinity_success = False
            best_tm = None
            best_score = -float('inf')

            if cause in ("NETWORK_BOTTLENECK", "NETWORK_TM_SATURATED"):
                print(f"\n🌐 [{subtask_id}] 偵測為 Network 瓶頸 ({cause})，啟動拓撲親和力演算法...")
                affinity_best_tm, _, affinity_net_added_map = self.calculate_topology_affinity(
                    subtask_id, resource_map, current_locations, pending_locations
                )
                if affinity_best_tm and resource_map.get(affinity_best_tm, {}).get('slots', MAX_SLOTS_PER_TM) + 1 <= MAX_SLOTS_PER_TM:
                    # Hard limit check: reject affinity result if it violates narrow-TM limits
                    task_nm_aff = task_detail.get('task_name', '')
                    affinity_blocked = False
                    for kw, max_cnt in HEAVY_OP_LIMITS.items():
                        if kw in task_nm_aff and affinity_best_tm in narrow_tms:
                            if heavy_op_count.get(affinity_best_tm, 0) >= max_cnt:
                                affinity_blocked = True
                                print(f"   ⛔ 拓撲親和力目標 {affinity_best_tm} 重量算子上限 "
                                      f"({kw} ≥{max_cnt})，改用 CPU 負載均衡")
                                break
                    if not affinity_blocked:
                        best_tm = affinity_best_tm
                        network_affinity_success = True
                        print(f"   ✅ 拓撲親和力演算法成功，直接選定 TM: {best_tm}")
                else:
                    affinity_net_added_map = {}
                    print(f"   ⚠️ 拓撲親和力退回，改用 CPU 負載均衡")

            if not network_affinity_success:
                task_nm_cpu = task_detail.get('task_name', '')
                for rid, res in resource_map.items():
                    if res['slots'] + 1 > MAX_SLOTS_PER_TM:
                        continue
                    # Hard limit: block heavy operators from narrow-BW TMs
                    heavy_blocked = False
                    for kw, max_cnt in HEAVY_OP_LIMITS.items():
                        if kw in task_nm_cpu and rid in narrow_tms:
                            if heavy_op_count.get(rid, 0) >= max_cnt:
                                heavy_blocked = True
                                break
                    if heavy_blocked:
                        continue
                    busy_on_target    = per_busy_rate / res['cpu_limit']
                    projected_busy    = res['busy_time'] + busy_on_target
                    projected_traffic = res['network_traffic'] + subtask_traffic
                    tm_limit  = self.tm_bandwidth_map.get(rid, self.default_bandwidth)
                    net_score = 1 - (projected_traffic / tm_limit)
                    cpu_score = 1 - (projected_busy / (res['cpu_limit'] * 1000))
                    # [FIX-5] 移除靜態 cpu_limit 門檻（res['cpu_limit'] >= cpu_limit）：
                    # 核心數少但空閒的 TM 是好遷移目標，用靜態核心數篩選與負載均衡目標矛盾。
                    # cpu_score 已內含負載壓力，讓排序自然篩選最合適的 TM。
                    if net_score > 0.0 and cpu_score > best_score:
                        best_score, best_tm = cpu_score, rid

            if best_tm:
                migration_plan[subtask_id] = best_tm
                pending_locations[subtask_id] = best_tm   # 更新 pending，供後續親和力計算使用
                resource_map[best_tm]['slots']           += 1
                resource_map[best_tm]['busy_time']       += busy_time_ms
                # [FIX-10] 方案A：使用與 Step 3 完全相同的淨增量更新 resource_map。
                # network_affinity_success 時從 affinity_net_added_map 取得淨增量，
                # CPU 路徑或 fallback 路徑則沿用 subtask_traffic（obs_out × 1.2）。
                if network_affinity_success and best_tm in affinity_net_added_map:
                    net_to_add = affinity_net_added_map[best_tm]
                else:
                    net_to_add = subtask_traffic
                resource_map[best_tm]['network_traffic'] += net_to_add
                # Update heavy_op_count for the destination TM
                task_nm_placed = task_detail.get('task_name', '')
                for kw in HEAVY_OP_LIMITS:
                    if kw in task_nm_placed:
                        heavy_op_count[best_tm] = heavy_op_count.get(best_tm, 0) + 1
                        break
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
                # 同步更新 pending_locations
                pending_locations[subtask_id] = fallback_rid

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
            # [FIX-7] 驗證失敗時阻擋遷移執行，回傳 None 讓 caller 判斷為無有效計畫
            print(f"   ⛔ 中止遷移計畫，不觸發 trigger_migration")
            return None, 0
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
            process = subprocess.Popen(docker_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.DEVNULL,text=True)
            print(f"🚀 已發送提交指令，正在確認 Job 狀態...")
            poll_start = time.time()
            while time.time() - poll_start < 60:
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

            # 輪詢超時後檢查 process 是否報錯
            stdout, stderr = process.communicate(timeout=5)
            if process.returncode != 0:
                print(f"❌ 命令執行失敗: {stderr}")
                return None
            return True  # Job 可能已提交成功但還沒偵測到 RUNNING

        except subprocess.TimeoutExpired:
            print(f"⚠️ 重新提交 Job 超時 (超過 60 秒)")
            print(f"   💡 Job 可能已經成功提交，請檢查 Flink Web UI")
            return True  # 即使超時也回傳 True，Job 可能已成功提交
        except Exception as e:
            print(f"❌ 提交失敗: {e}")
            return None


    def _wait_for_slots_freed(self, expected_slots: int, timeout: int = 30) -> bool:
        """
        [FIX-9] 等待全叢集 freeSlots 總數達到預期值。

        設計依據：
          Flink 的 /jobs/{id}/stop 完成（Savepoint COMPLETED）和
          Job 狀態變成 CANCELED/FINISHED，都早於各 TM 實際將 Slot
          從 ALLOCATED 改回 FREE 的時間點。
          若立刻呼叫 flink run，JobManager 在 Slot offer 視窗內
          看不到足夠的 freeSlots，會跳過未回報的 TM（尤其弱節點）。

        判斷依據（/taskmanagers REST API）：
          total_free = Σ tm.freeSlots（所有 TM）
          expected_slots = 本次停止 Job 所持有的 Slot 總數（= parallelism）

        為什麼用 freeSlots 而非 Job 狀態？
          Job 狀態是 Flink JobManager 層面的狀態機，
          Slot 釋放是 TaskManager 層面的資源管理，兩者非同步。
          只有查 /taskmanagers 的 freeSlots 才能確認 TM 真的準備好了。

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

          此方法逐一確認每個 TM 都有 freeSlots > 0，
          才允許執行 submit_job_from_savepoint。

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
                # 每個 TM 都必須有至少 1 個 freeSlot，弱節點不能被漏掉
                not_ready = [
                    (tm.get("id", "?"), tm.get("freeSlots", 0))
                    for tm in tms if tm.get("freeSlots", 0) == 0
                ]
                if not not_ready:
                    print(f"✅ [FIX-9] 所有 {len(tms)} 個 TM 均有 freeSlot，可以安全重啟")
                    return True
                # 只在有變化時才印出，避免洗版
                print(f"   等待 TM freeSlot: 尚未就緒={[tm_id for tm_id, _ in not_ready]}")
            except Exception as e:
                print(f"   ⚠️ 查詢 /taskmanagers 失敗: {e}")
            time.sleep(1)
        print(f"⚠️ [FIX-9] 等待所有 TM 就緒超時（{timeout}s），強制繼續（有風險）")
        return False

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

    def fetch_current_latency(self):
        """
        從 Prometheus 即時讀取當下的 total latency（ms）。
        與 latency_monitor.py 使用相同的查詢式。
        回傳 float，若查詢失敗回傳 None。
        """
        query = (
            f'max(flink_taskmanager_job_task_operator_currentEmitEventTimeLag{{job_name="{self.job_name}"}})'
            f' + '
            f'max(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{{job_name="{self.job_name}"}})'
        )
        try:
            resp = requests.get(f"{self.base_url}/api/v1/query",
                                params={'query': query}, timeout=5)
            data = resp.json()
            if data['status'] == 'success' and data['data']['result']:
                return float(data['data']['result'][0]['value'][1])
        except Exception as e:
            print(f"⚠️ 讀取即時 latency 失敗: {e}")
        return None

    def trigger_migration(self, migration_plan, job_id=None, auto_restart=True,
                          current_latency_ms=None):
        # v10：動態冷卻判斷（支援 latency 回落後提前結束）
        if not self._is_cooldown_done(current_latency_ms):
            elapsed   = time.time() - self.last_migration_time
            remaining = self.migration_cooldown - elapsed
            print(f"⏳ 遷移冷卻中，已等 {elapsed:.0f}s，"
                  f"剩餘約 {max(remaining, 0):.0f}s（動態冷卻，latency 回落可提前結束）")
            return False

        # 冷卻期結束，檢查當下 latency 是否超過門檻
        if self.migration_latency_threshold > 0:
            current_latency = self.fetch_current_latency()
            if current_latency is None:
                print(f"⚠️ 無法讀取即時 latency，放棄本次遷移")
                return False
            if current_latency < self.migration_latency_threshold:
                print(f"✅ 當下 latency={current_latency:.0f} ms < 門檻 {self.migration_latency_threshold} ms，"
                      f"瓶頸已緩解，不觸發遷移")
                return False
            print(f"🔴 當下 latency={current_latency:.0f} ms ≥ 門檻 {self.migration_latency_threshold} ms，"
                  f"確認需要遷移")

        if not self.write_migration_plan(migration_plan):
            return False

        if job_id is None:
            running_jobs = self.get_running_jobs()
            if not running_jobs:
                return False
            job_id = running_jobs[0]

        try:
            resp = requests.get(f"{self.flink_rest_url}/jobs/{job_id}")
            current_state = resp.json().get('state', '')
            if current_state != 'RUNNING':
                print(f"⚠️ Job {job_id} 狀態為 {current_state}，跳過遷移")
                return False
        except Exception as e:
            print(f"⚠️ 無法確認 job 狀態: {e}")

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

        # ── [FIX-9] 確認所有 TM Slot 實際釋放後再重啟 ──────────────────────
        # wait_for_job_termination 只確認 Job 狀態機到達終態，
        # 但 TM Slot 釋放是非同步的，弱節點（單核 TM）在高負載後
        # 可能因 JVM GC 導致 Slot 回報延遲，被 JobManager 跳過。
        # parallelism 即為本次 Job 所持有的 Slot 總數（每個 subtask 佔一個 slot）。
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

        # ── 寫入整合遷移記錄 ──
        try:
            with open(self.migration_record_path, "a") as rec:
                rec.write("\n" + "=" * 70 + "\n")
                rec.write(f"遷移時間: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(migration_ts))}\n")
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
                rec.write(f"  {'-'*55} {'-'*12} {'-'*12} {'-'*12} {'-'*20}\n")
                for sid, target_tm in migration_plan.items():
                    from_tm = current_locs.get(sid, "unknown")
                    to_tm   = target_tm
                    info    = self._task_info.get(sid, {})
                    state_bytes = info.get("state_size", 0)
                    cause   = info.get("bottleneck_cause", "N/A")
                    # 只記錄真正有移動的 subtask
                    if from_tm != target_tm:
                        if state_bytes >= 1024 * 1024:
                            state_str = f"{state_bytes / 1024 / 1024:.2f} MB"
                        elif state_bytes >= 1024:
                            state_str = f"{state_bytes / 1024:.2f} KB"
                        else:
                            state_str = f"{state_bytes} B" if state_bytes else "N/A"
                        rec.write(f"  {sid:<55} {from_tm:<12} {to_tm:<12} {state_str:>12} {cause}\n")
                rec.write("=" * 70 + "\n")
            print(f"📝 遷移記錄已寫入: {self.migration_record_path}")
        except Exception as e:
            print(f"⚠️ 寫入遷移記錄失敗: {e}")

        if new_job_id:
            self.last_migration_time = time.time()
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
        subtask_locations = self.get_subtask_locations()
        out_pool_map  = self.query_metric_by_task(
            'max_over_time(flink_taskmanager_job_task_buffers_outPoolUsage[15s])'
        )
        in_pool_map   = self.query_metric_by_task(
            'max_over_time(flink_taskmanager_job_task_buffers_inPoolUsage[15s])'
        )

        # ── 初始配置備份（只執行一次）──
        # 此時 Flink 已完成原生 Subtask 分配，migration_plan.json 尚未被 propose 修改。
        # get_subtask_locations() 回傳的 key/value 格式與 migration_plan.json 相同：
        #   key  : task_name_subtask_index（例如 Window_Max____Map_0）
        #   value: tm_id（例如 tm_20c_1）
        if (self.initial_placement_path
                and not self._initial_placement_saved
                and subtask_locations):
            try:
                os.makedirs(os.path.dirname(self.initial_placement_path), exist_ok=True)
                with open(self.initial_placement_path, 'w') as _f:
                    json.dump(subtask_locations, _f, indent=2, ensure_ascii=False)
                self._initial_placement_saved = True
                print(f"[initial_placement] ✅ 初始配置已備份 → {self.initial_placement_path}")
                print(f"[initial_placement]    共 {len(subtask_locations)} 個 subtask")
            except Exception as _e:
                print(f"[initial_placement] ⚠️ 備份失敗: {_e}")

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


    def auto_detect_and_migrate(self, monitor_interval_sec=30, current_latency_ms=None):
        """
        v10 完整自動偵測與遷移流程（含 Section III-C 最佳時間選擇）。

        執行順序：
          階段 1（排程重入）：若已有排程中的計畫，重新評估是否到達最佳時機。
          階段 2（正常偵測）：detect_bottleneck → evaluate_migration_trigger（三態）。
          階段 3（執行或排程）：EXECUTE_NOW 立即觸發；SCHEDULE_DELAY 存入排程等待。

        參數：
          monitor_interval_sec : 監控週期（秒），影響預測精度（歷史取樣粒度）；
                                 搜尋步長固定 5 秒，與此值無關。
          current_latency_ms   : 當前 latency（ms），用於動態冷卻判斷；
                                 可由呼叫端從 latency CSV 或 Prometheus 讀取傳入，
                                 若為 None 則退回固定冷卻時間。
        """
        IMPROVEMENT_THRESHOLD = 0.05
        # 排程等待期間每隔幾輪重新 detect 一次（確認瓶頸仍存在）
        # 設 2 代表每 2 × monitor_interval_sec 秒做一次完整偵測
        RECHECK_EVERY_N = 2

        # ── 階段 1：若已有排程中的計畫，每輪重新評估是否到時機 ──────────────
        if self._migration_scheduled and self._pending_migration_plan:
            elapsed = time.time() - self._migration_ready_time
            self._scheduled_recheck_count += 1
            print(f"\n⏳ [v10-Timing] 排程等待中 ({elapsed:.0f}s / "
                  f"max {self._migration_wait_timeout}s，"
                  f"recheck #{self._scheduled_recheck_count})")

            # ── 週期性重新偵測：確認瓶頸是否仍存在 ──────────────────────────
            # 每 RECHECK_EVERY_N 輪跑一次完整的 detect_bottleneck，
            # 避免在瓶頸已自然消失後仍執行不必要的遷移。
            # 代價：每 2 × 30 = 60 秒多一次 Prometheus 查詢。
            if self._scheduled_recheck_count % RECHECK_EVERY_N == 0:
                print(f"🔄 [v10-Timing] 週期性重新偵測（每 {RECHECK_EVERY_N} 輪一次）...")
                self.detect_bottleneck()   # 副作用：更新 _bottleneck_subtasks / _task_info
                if not self._bottleneck_subtasks:
                    print("✅ [v10-Timing] 瓶頸已自然消失，取消排程，不執行遷移")
                    self._migration_scheduled     = False
                    self._pending_migration_plan  = None
                    self._scheduled_recheck_count = 0
                    return False
                print(f"🔥 [v10-Timing] 確認仍有 {len(self._bottleneck_subtasks)} 個瓶頸，"
                      f"繼續等待最佳時機")

            # ── 重新搜尋最佳起始時間（每輪更新預測，反映最新資料率）──────────
            migration_duration_sec = self._estimate_migration_duration(
                self._bottleneck_subtasks
            )
            best_sec, best_cost, immediate_cost = self._find_optimal_migration_start(
                migration_duration_sec, monitor_interval_sec
            )
            state_dec, _ = self._is_state_decreasing(self._bottleneck_subtasks)

            if (best_sec == 0 and not state_dec) or elapsed >= self._migration_wait_timeout:
                reason = ("到達最佳時機" if elapsed < self._migration_wait_timeout
                          else f"等待超時 ({elapsed:.0f}s)，強制執行")
                print(f"🚀 [v10-Timing] {reason}，執行遷移！")
                self._log_timing_decision(best_sec, best_cost, immediate_cost,
                                          0.0, f"EXECUTE({reason})")
                self._migration_scheduled     = False
                plan = self._pending_migration_plan
                self._pending_migration_plan  = None
                self._scheduled_recheck_count = 0
                return self.trigger_migration(plan, current_latency_ms=current_latency_ms)
            else:
                rate_imp = ((immediate_cost - best_cost) / immediate_cost
                            if immediate_cost > 0 else 0.0)
                print(f"   繼續等待，best_sec={best_sec}s，"
                      f"rate_imp={rate_imp*100:.1f}%，state_dec={state_dec}")
                return False

        # ── 階段 2：正常偵測 ───────────────────────────────────────────────
        print("\n" + "=" * 100)
        print("Propose v13 檢查叢集狀態...")

        self.print_subtask_status()

        print("=" * 100)
        print("STEP 1 & 2: 瓶頸偵測")
        print("=" * 100)
        reports = self.detect_bottleneck()

        if not reports:
            print("⚠️ 無法獲取監控數據")
            return False

        if not self._bottleneck_subtasks:
            print("✅ 未檢測到瓶頸，系統運行正常")
            # 瓶頸消失：取消排程（避免對已不存在的問題遷移）
            if self._migration_scheduled:
                print("ℹ️  [v10-Timing] 瓶頸已消失，取消排程中的遷移計畫")
                self._migration_scheduled     = False
                self._pending_migration_plan  = None
                self._scheduled_recheck_count = 0
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
        print("STEP 3: 全局觸發評估 (Gatekeeper v11)")
        print("=" * 100)
        decision, reason, best_sec = self.evaluate_migration_trigger(
            self._bottleneck_subtasks,
            monitor_interval_sec=monitor_interval_sec
        )

        if decision == "REJECT":
            print(f"⚠️ 全局觸發條件未滿足 ({reason})，放棄本次遷移")
            return False

        print("\n" + "=" * 100)
        print("STEP 4: 多維度優先級排序")
        print("=" * 100)
        bottleneck_ids   = [sid for sid, _, _ in self._bottleneck_subtasks]
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
        # [FIX-7] generate_migration_plan 驗證失敗時回傳 (None, 0)，此處明確攔截
        if migration_plan is None:
            print("\n⛔ generate_migration_plan 驗證失敗，本輪不執行遷移")
            return False

        if migrated_count == 0:
            print("\n✨ 決策結果: 分配算法認為維持現狀是最佳選擇 (migrated_count = 0)")
            return False

        # ── 記錄決策過程至 detail_log CSV ─────────────────────────────────
        detail_exists     = os.path.isfile(self.detail_log)
        event_time        = time.time()
        current_locations = self.get_subtask_locations()

        with open(self.detail_log, "a", newline="") as f:
            writer = csv.writer(f)
            if not detail_exists:
                writer.writerow(["timestamp", "decision_step", "subtask_id",
                                 "priority_rank", "from_tm", "to_tm", "decision_reason"])
            for subtask_id, _, _ in self._bottleneck_subtasks:
                info  = self._task_info.get(subtask_id, {})
                cause = info.get("bottleneck_cause", "UNKNOWN")
                label = ("CPU Overload" if "CPU" in cause
                         else "Network Overload" if "NETWORK" in cause
                else "Latent Bottleneck")
                writer.writerow([event_time, "Diagnosis", subtask_id, "",
                                 current_locations.get(subtask_id, "unknown"), "",
                                 f"Detected {label}"])
            for rank, (subtask_id, priority_score) in enumerate(prioritized_list, start=1):
                writer.writerow([event_time, "Prioritization", subtask_id, rank,
                                 current_locations.get(subtask_id, "unknown"), "",
                                 f"Priority={priority_score:.3f} (Rank {rank})"])
            for subtask_id, target_tm in migration_plan.items():
                original_tm = current_locations.get(subtask_id, "unknown")
                if target_tm != original_tm:
                    cause  = self._task_info.get(subtask_id, {}).get("bottleneck_cause", "UNKNOWN")
                    rsn    = ("CPU load balance" if "CPU" in cause
                              else "network topology affinity" if "NETWORK" in cause
                    else "latent bottleneck resolution")
                    writer.writerow([event_time, "Assignment", subtask_id, "",
                                     original_tm, target_tm, f"{target_tm} selected: {rsn}"])

        # ── 階段 3：根據 evaluate 決策執行或排程 ──────────────────────────
        if decision == "SCHEDULE_DELAY":
            print(f"\n⏸️  [v10-Timing] 排程延遲遷移，等待 {best_sec}s: {reason}")
            self._pending_migration_plan  = migration_plan
            self._migration_scheduled     = True
            self._migration_ready_time    = time.time()
            self._scheduled_recheck_count = 0   # 重置計數器，從第 1 輪開始計
            return False

        # EXECUTE_NOW
        print(f"\n🚀 [v10-Timing] 立即執行遷移: {reason}")
        return self.trigger_migration(migration_plan, current_latency_ms=current_latency_ms)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CAOM Flink Bottleneck Detector v13")
    parser.add_argument("--query", required=True, choices=["q4", "q5", "q7"],
                        help="Nexmark query to monitor (q4, q5, or q7)")
    parser.add_argument("--id", dest="output_id", default="t16",
                        help="Experiment output folder name (default: t16)")
    args = parser.parse_args()

    CHECK_INTERVAL = 30   # 秒，與 auto_propose.py 的 CHECK_INTERVAL 保持一致
    detector = FlinkPropose(query_type=args.query, output_id=args.output_id)

    while True:
        current_latency = detector.fetch_current_latency()
        detector.auto_detect_and_migrate(
            monitor_interval_sec=CHECK_INTERVAL,
            current_latency_ms=current_latency,
        )
        time.sleep(CHECK_INTERVAL)