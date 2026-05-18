import requests
import time
import csv
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import os

# === Query 對應表 ===
QUERY_JOB_MAP = {
    "q4": "Nexmark_Q4_Isolated__Benchmark_Driver_",
    "q5": "Nexmark_Q5_Isolated__Migration_Test_",
    "q7": "Nexmark_Q7_Isolated__Benchmark_Driver_",
}

# === 解析命令列參數 ===
def parse_args():
    parser = argparse.ArgumentParser(description="Flink Latency Monitor")
    parser.add_argument(
        "--query", "-q",
        required=True,
        choices=QUERY_JOB_MAP.keys(),
        help="實驗 Query 名稱，例如: q4, q5, q7"
    )
    parser.add_argument(
        "--id", "-i",
        required=True,
        help="實驗 ID，用於輸出資料夾命名，例如: t16_2"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=None,
        help="指定輸出目錄（覆蓋預設的 BASE_OUTPUT_DIR/id 路徑）。"
             "實驗腳本用此參數把 CSV 寫到 results/ 下的正確位置。"
    )
    return parser.parse_args()

# === 設定區 ===
PROM_URL = "http://localhost:9090"
FLINK_REST_URL = "http://localhost:8081"
SAMPLE_INTERVAL = 2
BASE_OUTPUT_DIR = "/home/yenwei/research/structure_setup/output"


def get_current_job_id():
    """
    從 Flink REST API 取得目前唯一 RUNNING job 的 job_id。
    不比對 job name：REST API 回傳的是原始名稱（含空格），
    而 Prometheus label 是 sanitized 格式（空格→底線），兩者無法直接比對。
    實驗環境同時只有一個 job 在跑，直接取第一個 RUNNING job 即可。
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/jobs", timeout=5)
        for job in resp.json().get('jobs', []):
            if job.get('status') == 'RUNNING':
                return job['id']
    except Exception as e:
        print(f"⚠️ 無法取得 job_id: {e}")
    return None


def build_queries(job_name, job_id=None):
    """
    建立 Prometheus 查詢字串。
    若有 job_id，加入 label filter 確保只查當前 job 的 metrics，
    排除 Prometheus TSDB 中舊 job 殘留的 stale series（5 分鐘 staleness window）。
    """
    id_filter = f', job_id="{job_id}"' if job_id else ''

    query_total_latency = f"""
      max(flink_taskmanager_job_task_operator_currentEmitEventTimeLag{{job_name="{job_name}"{id_filter}}})
      +
      max(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{{job_name="{job_name}"{id_filter}}})
    """
    throughput_q = (
        f'sum(flink_taskmanager_job_task_numRecordsOutPerSecond'
        f'{{job_name="{job_name}", task_name=~".*Source.*"{id_filter}}})'
    )
    return query_total_latency, throughput_q


def fetch_metric(query):
    try:
        response = requests.get(f"{PROM_URL}/api/v1/query", params={'query': query}, timeout=5)
        data = response.json()
        if data['status'] == 'success' and data['data']['result']:
            return float(data['data']['result'][0]['value'][1])
    except Exception as e:
        print(f"查詢錯誤: {e}")
    return None


def fetch_latency_with_fallback(query_with_id, query_without_id):
    """
    latency 查詢：先用 job_id 過濾；若某些 metric 不含 job_id label 導致無資料，
    才退回 job_name-only 查詢（latency 無資料時寫不了 CSV，fallback 可接受）。
    """
    result = fetch_metric(query_with_id)
    if result is None and query_with_id != query_without_id:
        result = fetch_metric(query_without_id)
    return result


def fetch_throughput_strict(query_with_id, query_without_id):
    """
    throughput 查詢：
    - 若有 job_id → 只用 job_id 版，無資料回傳 0（新 job 剛啟動尚未 scrape，正確值就是 0）
      不可 fallback：fallback 會拿到舊 job 的 stale 高值，反而污染資料
    - 若無 job_id（get_current_job_id 失敗）→ 退回 job_name-only，接受 stale 風險
    """
    if query_with_id != query_without_id:
        # 有 job_id filter：無資料 = 新 job 尚未有 metrics，視為 0
        result = fetch_metric(query_with_id)
        return result if result is not None else 0.0
    else:
        # 無 job_id：只能用 job_name，接受 stale 風險
        return fetch_metric(query_without_id)


def monitor(job_name, output_file, output_csv):
    # 取得當前 job_id，用於 Prometheus 查詢過濾
    job_id = get_current_job_id()
    if job_id:
        print(f"✅ 目前 Job ID: {job_id}（Prometheus 查詢將加入 job_id 過濾）")
    else:
        print("⚠️ 無法取得 job_id，Prometheus 查詢將僅用 job_name 過濾")
        print("   （可能有舊 job stale metrics 污染，staleness window = 5 分鐘）")

    query_latency_with_id, throughput_with_id = build_queries(job_name, job_id)
    query_latency_no_id,  throughput_no_id  = build_queries(job_name, None)

    print(f"開始監控 Job: {job_name}")
    print(f"數據將存儲至: {output_file}")

    os.makedirs(output_file, exist_ok=True)
    file_exists = os.path.isfile(output_csv)
    start_time = time.time()

    # 偵測遷移後 job 重啟：throughput 連續為 0 但 latency 有值 → job_id 可能已換
    zero_tp_streak = 0
    ZERO_TP_REFRESH_THRESHOLD = 5  # 連續 5 次（10 秒）throughput=0 且有 latency → 刷新 job_id

    with open(output_csv, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'unix_timestamp', 'relative_time', 'total_latency_ms', 'throughput'])

        try:
            while True:
                val = fetch_latency_with_fallback(query_latency_with_id, query_latency_no_id)
                throughput = fetch_throughput_strict(throughput_with_id, throughput_no_id)

                display_throughput = throughput if throughput is not None else 0.0

                # 偵測 job 重啟（遷移後新 job_id）：throughput 連續 0 但 latency 仍有值
                if display_throughput == 0.0 and val is not None and job_id is not None:
                    zero_tp_streak += 1
                    if zero_tp_streak >= ZERO_TP_REFRESH_THRESHOLD:
                        new_id = get_current_job_id()
                        if new_id and new_id != job_id:
                            job_id = new_id
                            query_latency_with_id, throughput_with_id = build_queries(job_name, job_id)
                            print(f"🔄 偵測到 Job 重啟，更新 Job ID: {job_id}")
                        zero_tp_streak = 0
                else:
                    zero_tp_streak = 0

                if val is not None:
                    current_unix = time.time()
                    elapsed = int(current_unix - start_time)
                    writer.writerow([time.strftime("%H:%M:%S"), current_unix, elapsed, val, display_throughput])
                    f.flush()
                    print(f"[{time.strftime('%H:%M:%S')}] Time: {elapsed}s | Total Latency: {val:.2f} ms | Throughput: {display_throughput:.2f} records/s")
                else:
                    print("等待 Flink 指標數據 (Job 可能正在重啟中)...")

                time.sleep(SAMPLE_INTERVAL)
        except KeyboardInterrupt:
            print("\n監控停止，準備繪圖...")
            plot_results(output_file, output_csv)

def plot_results(output_file, output_csv):
    if not os.path.exists(output_csv) or os.stat(output_csv).st_size == 0:
        print("❌ CSV 檔案不存在或為空，無法繪圖。")
        return

    try:
        df = pd.read_csv(output_csv)

        # --- 圖表 1: Latency 延遲圖 ---
        plt.figure(figsize=(10, 6))
        plt.plot(df['relative_time'], df['total_latency_ms'], label='Proposed (Latency)', color='#1f77b4', linewidth=2)
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Latency (ms)', fontsize=12)
        plt.title('Total Latency Variation (Including Migration Cost)', fontsize=14)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.savefig(os.path.join(output_file, 'experiment_latency_plot.png'))
        print(f"✅ 延遲圖表已生成: {os.path.join(output_file, 'experiment_latency_plot.png')}")

        # --- 圖表 2: Throughput 吞吐量圖 ---
        plt.figure(figsize=(10, 6))
        plt.plot(df['relative_time'], df['throughput'], label='Proposed (Throughput)', color='#2ca02c', linewidth=2)
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Throughput (records/s)', fontsize=12)
        plt.title('Throughput Performance During Migration', fontsize=14)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.savefig(os.path.join(output_file, 'experiment_throughput_plot.png'))
        print(f"✅ 吞吐量圖表已生成: {os.path.join(output_file, 'experiment_throughput_plot.png')}")

    except Exception as e:
        print(f"❌ 繪圖過程中發生錯誤: {e}")

if __name__ == "__main__":
    args = parse_args()

    # 根據參數動態決定 JOB_NAME 與輸出路徑
    JOB_NAME = QUERY_JOB_MAP[args.query.lower()]

    # --output-dir 優先；未指定時沿用原本的 BASE_OUTPUT_DIR/id 行為
    if args.output_dir:
        OUTPUT_FILE = args.output_dir
    else:
        OUTPUT_FILE = os.path.join(BASE_OUTPUT_DIR, args.id)
    OUTPUT_CSV = os.path.join(OUTPUT_FILE, "latency_data.csv")

    print(f"✅ Query: {args.query.upper()} → Job: {JOB_NAME}")
    print(f"✅ 輸出路徑: {OUTPUT_FILE}")
    print(f"✅ CSV 路徑: {OUTPUT_CSV}")

    monitor(JOB_NAME, OUTPUT_FILE, OUTPUT_CSV)
