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
    return parser.parse_args()

# === 設定區 ===
PROM_URL = "http://localhost:9090"
SAMPLE_INTERVAL = 2
BASE_OUTPUT_DIR = "/home/yenwei/research/structure_setup/output"

def build_queries(job_name):
    query_total_latency = f"""
      max(flink_taskmanager_job_task_operator_currentEmitEventTimeLag{{job_name="{job_name}"}})
      +
      max(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{{job_name="{job_name}"}})
    """
    throughput_q = f'sum(flink_taskmanager_job_task_numRecordsOutPerSecond{{job_name="{job_name}", task_name=~".*Source.*"}})'
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

def monitor(job_name, output_file, output_csv):
    query_total_latency, throughput_q = build_queries(job_name)

    print(f"開始監控 Job: {job_name}")
    print(f"數據將存儲至: {output_file}")

    os.makedirs(output_file, exist_ok=True)
    file_exists = os.path.isfile(output_csv)
    start_time = time.time()

    with open(output_csv, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'unix_timestamp', 'relative_time', 'total_latency_ms', 'throughput'])

        try:
            while True:
                val = fetch_metric(query_total_latency)
                throughput = fetch_metric(throughput_q)

                display_throughput = throughput if throughput is not None else 0.0

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
    OUTPUT_FILE = os.path.join(BASE_OUTPUT_DIR, args.id)
    OUTPUT_CSV = os.path.join(OUTPUT_FILE, "latency_data.csv")

    print(f"✅ Query: {args.query.upper()} → Job: {JOB_NAME}")
    print(f"✅ 輸出路徑: {OUTPUT_FILE}")

    monitor(JOB_NAME, OUTPUT_FILE, OUTPUT_CSV)