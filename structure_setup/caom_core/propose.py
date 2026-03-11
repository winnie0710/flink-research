import requests
# import numpy as np
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

class FlinkPropose:
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

    def get_subtask_state_sizes(self):
        """
        查詢每個 subtask 的狀態大小（用於評估遷移代價）
        返回: { "task_name": { subtask_index: state_size_bytes, ... }, ... }
        """
        try:
            # Flink 狀態大小指標（可能的指標名稱）
            state_metrics = [
                'flink_taskmanager_job_task_operator_currentStateSize',  # 當前狀態大小
                'flink_taskmanager_job_task_operator_lastCheckpointSize',  # 最後 checkpoint 大小
                'flink_taskmanager_job_task_checkpointStartDelayNanos',  # 如果上面沒有，嘗試其他指標
            ]

            state_size_map = {}

            # 嘗試不同的狀態指標
            for metric in state_metrics:
                result = self.query_metric_by_task(metric)
                if result:
                    state_size_map = result
                    print(f"✅ 使用指標 {metric} 獲取狀態大小")
                    break

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
        # 除了 In，也要抓取 Out 指標
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
                    "state_size": state_size  # bytes
                }

        # Step A: Recover actual source rate
        # Identify source operators (those with "Source" in name)
        source_tasks = {k: v for k, v in task_info.items() if "Source" in v["task_name"]}

        for subtask_id, info in source_tasks.items():
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            observed_rate = info["observed_rate"]

            if T_busy > 0:
                # λ̂_Source = λ_Source × (1 + T_bp / T_busy)
                actual_source_rate = observed_rate * (1 + T_bp / T_busy)
                info["actual_input_rate"] = actual_source_rate
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
                    task_info[subtask_id]["actual_input_rate"] = upstream_avg_actual * (observed_rate / upstream_avg_observed) if upstream_avg_observed > 0 else observed_rate

        # Step C & D: Calculate max capacity and identify bottlenecks
        bottleneck_subtasks = []

        for subtask_id, info in task_info.items():
            T_busy = info["T_busy"]
            T_bp = info["T_bp"]
            T_idle = info["T_idle"]
            observed_rate = info["observed_rate"]
            actual_input_rate = info["actual_input_rate"]

            # Calculate max processing capacity: λ^a = λ + ((T_bp + T_idle) / T_busy) × λ
            if T_busy > 0 and observed_rate > 0:
                max_capacity = observed_rate + ((T_bp + T_idle) / T_busy) * observed_rate
            else:
                max_capacity = observed_rate

            info["max_capacity"] = max_capacity

            # Identify bottleneck: actual_input_rate > max_capacity
            if actual_input_rate > max_capacity and max_capacity > 0:
                info["is_bottleneck"] = True
                bottleneck_subtasks.append((subtask_id, actual_input_rate, max_capacity))

        # Generate report
        report_list = []
        for op_name in ordered_operators:
            subtasks = operator_groups[op_name]

            # Aggregate statistics for the operator
            bottleneck_count = sum(1 for st in subtasks if task_info[st]["is_bottleneck"])
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

    def get_taskmanager_info(self):
        """
        查詢所有 TaskManager 的資訊和當前負載，包含 CPU 容量限制
        使用 Prometheus 獲取負載資訊，並自動添加已註冊但空閒的 TM
        返回: { resource_id: {"host": "192.168.1.100", "current_load": 0.5, "cpu_limit": 1.5}, ... }
        """
        try:
            # CPU capacity mapping based on docker-compose.yml
            # Note: Prometheus metrics use underscore format (tm_20c_1) not hyphen (tm-20c-1)
            cpu_capacity_map = {
                "tm_20c_1": 2.0,
                "tm_20c_2_net": 2.0,
                "tm_10c_3_cpu": 1.0,
                "tm_20c_4": 1.0
            }

            # Step 1: Query Prometheus for load information from busy TMs
            query = 'avg(flink_taskmanager_job_task_busyTimeMsPerSecond) by (resource_id, tm_id, host)'
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

            # [有問題] Step 2: Add any missing TMs from cpu_capacity_map with zero load
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

    def generate_migration_plan(self, overloaded_subtasks=None):
        """
        Generate capacity-aware migration plan respecting hardware limits
        - Slot Constraint: Max 6 subtasks per TM
        - Heavy-First Sorting: Process bottlenecks by actual_input_rate (descending)
        - Normalized Load: (Current BusyTime + New Load) / CPU Limit
        Returns: { "subtask_id": "target_resource_id", ... }
        """
        # Use bottlenecks from CAOM detection if no explicit list provided
        if overloaded_subtasks is None:
            if not hasattr(self, '_bottleneck_subtasks') or not self._bottleneck_subtasks:
                print("⚠️ 未檢測到瓶頸，無需產生遷移計畫")
                return None
            # Extract subtask IDs from CAOM detection results
            overloaded_subtasks = [(subtask_id, actual_rate)
                                   for subtask_id, actual_rate, max_cap in self._bottleneck_subtasks]

        tm_info = self.get_taskmanager_info()
        current_locations = self.get_subtask_locations()

        if not tm_info:
            print("⚠️ 無法獲取 TaskManager 資訊，無法產生遷移計畫")
            return None

        if not overloaded_subtasks:
            print("⚠️ 未檢測到過載的 subtask，無需產生遷移計畫")
            return None

        # Constants
        MAX_SLOTS_PER_TM = 6

        # Initialize slot occupancy tracker from current locations
        slot_occupancy = {rid: 0 for rid in tm_info.keys()}
        for subtask_id, resource_id in current_locations.items():
            if resource_id in slot_occupancy:
                slot_occupancy[resource_id] += 1

        print(f"\n📊 初始 Slot 佔用情況:")
        for rid, count in slot_occupancy.items():
            cpu_limit = tm_info[rid]['cpu_limit']
            print(f"   {rid}: {count}/{MAX_SLOTS_PER_TM} slots, CPU limit: {cpu_limit}")

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
            else:
                bottleneck_list.append((subtask_id, actual_rate, 0, 0))

        # Sort by actual_input_rate descending (heavy first)
        bottleneck_list.sort(key=lambda x: x[1], reverse=True)
        bottleneck_subtask_ids = {subtask_id for subtask_id, _, _, _ in bottleneck_list}

        print(f"\n🔍 檢測到 {len(bottleneck_subtask_ids)} 個瓶頸 Subtask (Heavy-First Order)")

        # Initialize migration plan with all subtasks staying in place
        migration_plan = {}
        for subtask_id, current_resource_id in current_locations.items():
            migration_plan[subtask_id] = current_resource_id

        # Track normalized load for each TM (for migration decisions)
        tm_load_tracker = {rid: info['current_load'] for rid, info in tm_info.items()}

        # Process bottleneck subtasks (heavy first)
        for subtask_id, actual_rate, busy_time, state_size in bottleneck_list:
            current_resource_id = current_locations.get(subtask_id, 'unknown')

            if current_resource_id not in tm_info:
                print(f"⚠️ {subtask_id} 的 resource_id {current_resource_id} 不在 TM 列表中，跳過")
                continue

            # Calculate potential load contribution (use busy_time as proxy)
            potential_new_load = busy_time if busy_time > 0 else 100

            # Migration cost based on state size (for display purposes)
            state_size_mb = state_size / (1024 * 1024) if state_size > 0 else 0

            # Find best target TM
            target_resource_id = None
            best_normalized_load = float('inf')

            for rid, info in tm_info.items():
                # Skip current TM (must migrate)
                if rid == current_resource_id:
                    continue

                # Check slot availability
                if slot_occupancy[rid] >= MAX_SLOTS_PER_TM:
                    continue

                # Calculate normalized load: (current_load + potential_new_load) / cpu_limit
                current_load = tm_load_tracker[rid]
                cpu_limit = info['cpu_limit']
                normalized_load = (current_load + potential_new_load) / cpu_limit

                if normalized_load < best_normalized_load:
                    best_normalized_load = normalized_load
                    target_resource_id = rid

            if target_resource_id:
                # Update migration plan
                migration_plan[subtask_id] = target_resource_id

                # Update slot occupancy (remove from old, add to new)
                if current_resource_id in slot_occupancy:
                    slot_occupancy[current_resource_id] -= 1
                slot_occupancy[target_resource_id] += 1

                # Update load tracker
                tm_load_tracker[target_resource_id] += potential_new_load

                # Get detailed info from CAOM detection
                if hasattr(self, '_task_info') and subtask_id in self._task_info:
                    task_detail = self._task_info[subtask_id]
                    max_capacity = task_detail['max_capacity']
                    overload_pct = ((actual_rate - max_capacity) / max_capacity * 100) if max_capacity > 0 else 0

                    print(f"📋 計畫遷移: {subtask_id}")
                    print(f"   從: {current_resource_id} (CPU: {tm_info[current_resource_id]['cpu_limit']}) -> 到: {target_resource_id} (CPU: {tm_info[target_resource_id]['cpu_limit']})")
                    print(f"   目標 host: {tm_info[target_resource_id]['host']}, Normalized Load: {best_normalized_load:.2f}")
                    print(f"   Slots: {slot_occupancy[target_resource_id]}/{MAX_SLOTS_PER_TM}")
                    print(f"   實際輸入: {actual_rate:.2f} rec/s, 最大容量: {max_capacity:.2f} rec/s")
                    print(f"   過載程度: {overload_pct:.1f}%")
                    if state_size > 0:
                        print(f"   💾 State 大小: {format_bytes(state_size)} (遷移代價)")
                else:
                    print(f"📋 計畫遷移: {subtask_id}")
                    print(f"   從: {current_resource_id} -> 到: {target_resource_id}")
                    print(f"   Normalized Load: {best_normalized_load:.2f}, Slots: {slot_occupancy[target_resource_id]}/{MAX_SLOTS_PER_TM}")
                    if state_size > 0:
                        print(f"   💾 State 大小: {format_bytes(state_size)}")
            else:
                # No suitable target found, keep in place
                print(f"⚠️ 無可用 TM 遷移: {subtask_id} (所有 TM 已達 slot 上限或負載過高), 維持原位")

        print(f"\n📊 最終 Slot 佔用情況:")
        for rid, count in slot_occupancy.items():
            cpu_limit = tm_info[rid]['cpu_limit']
            normalized_load = tm_load_tracker[rid] / cpu_limit if cpu_limit > 0 else 0
            print(f"   {rid}: {count}/{MAX_SLOTS_PER_TM} slots, CPU: {cpu_limit}, Normalized Load: {normalized_load:.2f}")

        print(f"\n✅ 遷移計畫包含 {len(migration_plan)} 個 subtask")
        migrated_count = sum(1 for sid in bottleneck_subtask_ids if migration_plan[sid] != current_locations.get(sid))
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


    def trigger_migration(self, migration_plan, job_id=None, auto_restart=False):
        """
        觸發完整的遷移流程

        Args:
            migration_plan: 遷移計畫字典
            job_id: Job ID (如果為 None 則自動獲取)
            auto_restart: 是否自動從 savepoint 重新提交 job (預設 True)
        """
        # 檢查冷卻時間
        current_time = time.time()
        if current_time - self.last_migration_time < self.migration_cooldown:
            remaining = self.migration_cooldown - (current_time - self.last_migration_time)
            print(f"⏳ 遷移冷卻中，剩餘 {remaining:.0f} 秒")
            return False

        # 1. 寫入遷移計畫
        if not self.write_migration_plan(migration_plan):
            return False

        # 2. 獲取正在運行的 Job
        if job_id is None:
            running_jobs = self.get_running_jobs()
            if not running_jobs:
                print("⚠️ 沒有正在運行的 Job")
                return False
            job_id = running_jobs[0]  # 使用第一個 Job

        # 3. Stop with Savepoint
        savepoint_path = self.stop_job_with_savepoint(job_id)
        if not savepoint_path:
            return False

        # 4. 等待 Job 完全停止
        print("⏳ 等待 Job 停止...")
        time.sleep(2)

        # 5. 自動從 Savepoint 重新提交 Job
        if auto_restart:
            new_job_id = self.submit_job_from_savepoint(savepoint_path)
            if new_job_id:
                print(f"✅ 遷移流程完成！新 Job 已啟動")
                self.last_migration_time = current_time
                return True
            else:
                print(f"❌ 自動重啟失敗，請手動重新提交 Job:")
                print(f"   Savepoint 路徑: {savepoint_path}")
                return False
        else:
            print("⚠️ 請手動使用以下指令重新提交 Job:")
            print(f"   docker exec -it jobmanager bash -c \"")
            print(f"     export NEXMARK_CONF_DIR={self.job_config['nexmark_conf_dir']} && \\")
            print(f"     /opt/flink/bin/flink run \\")
            print(f"     -s {savepoint_path} \\")
            print(f"     -d \\")
            print(f"     -c {self.job_config['entry_class']} \\")
            print(f"     -p {self.job_config['parallelism']} \\")
            print(f"     {self.job_config['jar_path']} \\")
            print(f"     {' '.join(self.job_config['program_args'])}")
            print(f"   \"")

            self.last_migration_time = current_time
            return True

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

        # Optionally trigger migration (commented out for safety)
        return self.trigger_migration(migration_plan)

        print("\n✅ 遷移計畫已生成並寫入檔案")
        return True