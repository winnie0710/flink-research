import requests
import numpy as np
import json
import time
import os

class FlinkDetector:
    def __init__(self, prometheus_url="http://localhost:9090",
                 flink_rest_url="http://localhost:8081",
                 migration_plan_path="/home/yenwei/research/structure_setup/plan/migration_plan.json",
                 savepoint_dir="file:///opt/flink/savepoints"):
        self.base_url = prometheus_url
        self.flink_rest_url = flink_rest_url
        self.migration_plan_path = migration_plan_path
        self.savepoint_dir = savepoint_dir
        self.last_migration_time = 0
        self.migration_cooldown = 300  # 冷卻時間 5 分鐘，避免頻繁重啟

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
        核心邏輯：查詢所有 Task 的狀態，並針對每個 Task 計算 Busy 和 BP
        """
        # 1. 查詢所有 Task 的 Busy Time (移除 task_name 過濾)
        busy_data_map = self.query_metric_by_task('flink_taskmanager_job_task_busyTimeMsPerSecond')

        # 2. 查詢所有 Task 的 Backpressure (移除 task_name 過濾)
        bp_data_map = self.query_metric_by_task('flink_taskmanager_job_task_backPressuredTimeMsPerSecond')

        if not busy_data_map:
            return [] # 回傳空清單代表無數據

        report_list = []

        # 針對每一個抓到的 Task 進行分析
        for task_name, subtasks_busy in busy_data_map.items():

            # 取得該 Task 對應的 Backpressure 數據 (如果有的話)
            subtasks_bp = bp_data_map.get(task_name, {})

            # 轉成 List 計算統計值
            busy_values = list(subtasks_busy.values())
            bp_values = list(subtasks_bp.values())

            max_busy = max(busy_values) if busy_values else 0
            avg_busy = sum(busy_values) / len(busy_values) if busy_values else 0
            max_bp = max(bp_values) if bp_values else 0

            # 計算傾斜度
            skew = max_busy - avg_busy

            # 狀態判斷
            status = "UNKNOWN"
            if max_busy > 500:
                if max_busy > 700:
                    status = "🔴 OVERLOAD" # 過載
                else:
                    status = "🟢 COMPUTE"  # 純計算
            else:
                if max_bp > 500:
                    status = "🟠 IO/WAIT"  # 下游塞住
                else:
                    status = "⚪ IDLE"     # 閒置

            # 整理詳細數據 (排序後的 List)
            details_busy = [int(v) for k, v in sorted(subtasks_busy.items())]

            report_list.append({
                "task_name": task_name,
                "status": status,
                "max_busy": round(max_busy, 1),
                "max_bp": round(max_bp, 1), # 新增 BP 顯示
                "skew": round(skew, 1),
                "details": details_busy
            })

        # 定義你想要的順序關鍵字 (越前面優先級越高)
        target_order = ["Source", "Window_Max", "Window_Join", "Sink"]

        def get_sort_priority(report):
            name = report['task_name']
            # 遍歷關鍵字，如果 Task Name 包含該關鍵字，回傳它的索引值 (0, 1, 2...)
            for i, keyword in enumerate(target_order):
                if keyword in name:
                    return i
            return 999  # 如果都不符合，排在最後面

        # 執行排序
        report_list.sort(key=get_sort_priority)

        return report_list

    def get_taskmanager_info(self):
        """
        查詢所有 TaskManager 的資訊和當前負載
        返回: { tm_id: {"host": "192.168.1.100", "load": 0.5}, ... }
        """
        try:
            # 查詢每個 TaskManager 的平均 CPU/負載
            query = 'avg(flink_taskmanager_job_task_busyTimeMsPerSecond) by (tm_id, host)'
            response = requests.get(f"{self.base_url}/api/v1/query", params={'query': query})
            data = response.json()

            if data['status'] != 'success':
                print(f"❌ 查詢 TaskManager 資訊失敗: {data.get('error')}")
                return {}

            tm_info = {}
            for r in data['data']['result']:
                tm_id = r['metric'].get('tm_id', 'unknown')
                host = r['metric'].get('host', 'unknown')

                # 修正 IP 格式：如果包含下劃線，替換為點號
                if '_' in host and not '.' in host:
                    host = host.replace('_', '.')

                load = float(r['value'][1])

                tm_info[tm_id] = {
                    "host": host,
                    "load": load
                }

            return tm_info

        except Exception as e:
            print(f"⚠️ 獲取 TaskManager 資訊失敗: {e}")
            return {}

    def get_subtask_locations(self):
        """
        查詢每個 subtask 當前所在的 TaskManager host
        返回: { "task_name_0": "192.168.1.100", ... }
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
                host = r['metric'].get('host', 'unknown')

                subtask_id = f"{task_name}_{subtask_index}"
                subtask_locations[subtask_id] = host

            return subtask_locations

        except Exception as e:
            print(f"⚠️ 獲取 Subtask 位置失敗: {e}")
            return {}

    def generate_migration_plan(self, overloaded_subtasks):
        """
        根據過載的 subtask 產生遷移計畫
        選擇負載最低的 TaskManager 作為目標
        """
        tm_info = self.get_taskmanager_info()
        current_locations = self.get_subtask_locations()

        if not tm_info:
            print("⚠️ 無法獲取 TaskManager 資訊，無法產生遷移計畫")
            return None

        # 按負載排序 TaskManager
        sorted_tms = sorted(tm_info.items(), key=lambda x: x[1]['load'])

        migration_plan = {}

        for subtask_id, current_load in overloaded_subtasks:
            current_host = current_locations.get(subtask_id, 'unknown')

            # 找到負載最低且不是當前位置的 TM
            target_tm = None
            for tm_id, info in sorted_tms:
                if info['host'] != current_host:
                    target_tm = info
                    break

            if target_tm:
                migration_plan[subtask_id] = target_tm['host']
                print(f"📋 計畫遷移: {subtask_id} 從 {current_host} -> {target_tm['host']} (負載: {target_tm['load']:.1f})")

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

    def submit_job_from_savepoint(self, jar_path, savepoint_path, entry_class=None, program_args=None):
        """
        從 Savepoint 重新提交 Job
        """
        try:
            # 先上傳 JAR (如果需要)
            # 然後使用 savepoint 啟動
            url = f"{self.flink_rest_url}/jars/{jar_path}/run"

            payload = {
                "savepointPath": savepoint_path
            }

            if entry_class:
                payload["entryClass"] = entry_class
            if program_args:
                payload["programArgs"] = program_args

            print(f"🚀 從 Savepoint 重新提交 Job...")
            response = requests.post(url, json=payload)
            data = response.json()

            new_job_id = data.get('jobid')
            print(f"✅ Job 已重新提交: {new_job_id}")
            return new_job_id

        except Exception as e:
            print(f"❌ 重新提交 Job 失敗: {e}")
            return None

    def trigger_migration(self, migration_plan, job_id=None):
        """
        觸發完整的遷移流程
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
        time.sleep(5)

        # 5. 重新提交 (這裡需要知道原本的 JAR 和參數)
        # 注意: 實際使用時需要根據你的環境調整
        # jar_path = "your-job.jar"
        # new_job_id = self.submit_job_from_savepoint(jar_path, savepoint_path)

        print("⚠️ 請手動使用以下指令重新提交 Job:")
        print(f"   flink run -s {savepoint_path} -d your-job.jar")

        self.last_migration_time = current_time
        return True

    def auto_detect_and_migrate(self, busy_threshold, skew_threshold):
        """
        自動檢測並觸發遷移
        """
        reports = self.detect_bottleneck()

        if not reports:
            return False

        overloaded_subtasks = []

        # 找出需要遷移的 subtask
        for report in reports:
            if report['status'] == "🔴 OVERLOAD" or report['skew'] > skew_threshold:
                task_name = report['task_name']
                details = report['details']

                # 找出負載最高的 subtask
                for idx, busy in enumerate(details):
                    if busy > busy_threshold:
                        subtask_id = f"{task_name}_{idx}"
                        overloaded_subtasks.append((subtask_id, busy))

        if not overloaded_subtasks:
            return False

        print(f"\n🔥 檢測到 {len(overloaded_subtasks)} 個過載的 Subtask")

        # 生成遷移計畫
        migration_plan = self.generate_migration_plan(overloaded_subtasks)

        if not migration_plan:
            return False

        # 觸發遷移
        return self.trigger_migration(migration_plan)