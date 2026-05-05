import requests
import time
import csv
import pandas as pd
import matplotlib.pyplot as plt
import os

# === 設定區 ===
PROM_URL = "http://localhost:9090"
#JOB_NAME = "Nexmark_Q7_Isolated__Benchmark_Driver_"
JOB_NAME = "Nexmark_Q5_Isolated__Migration_Test_"
#JOB_NAME = "Nexmark_Q4_Isolated__Benchmark_Driver_"
# TODO: 每次實驗結果放入不同資料夾
OUTPUT_FILE = "/home/yenwei/research/structure_setup/output/t17_4"
OUTPUT_CSV = "/home/yenwei/research/structure_setup/output/t17_4/latency_data.csv"
SAMPLE_INTERVAL = 2

QUERY_TOTAL_LATENCY = f"""
  max(flink_taskmanager_job_task_operator_currentEmitEventTimeLag{{job_name="{JOB_NAME}"}})
  +
  max(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{{job_name="{JOB_NAME}"}})
"""

# QUERY_TOTAL_LATENCY = f'max(flink_taskmanager_job_task_operator_currentEmitEventTimeLag{{job_name="{JOB_NAME}"}}) '
#Inner_latency = f'max(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{{job_name="{JOB_NAME}"}})'
THROUGHPUT_Q = f'sum(flink_taskmanager_job_task_numRecordsOutPerSecond{{job_name="{JOB_NAME}", task_name=~".*Source.*"}})'

# TODO : throughput 到底是看輸入還是輸出 因為資料會放大 輸出會變成20倍
def fetch_metric(query):
    try:
        response = requests.get(f"{PROM_URL}/api/v1/query", params={'query': query}, timeout=5)
        data = response.json()
        if data['status'] == 'success' and data['data']['result']:
            return float(data['data']['result'][0]['value'][1])
    except Exception as e:
        print(f"查詢錯誤: {e}")
    return None

def monitor():
    print(f"開始監控 Job: {JOB_NAME}")
    print(f"數據將存儲至: {OUTPUT_FILE}")

    # 確保輸出目錄存在
    os.makedirs(OUTPUT_FILE, exist_ok=True)
    file_exists = os.path.isfile(OUTPUT_CSV)
    start_time = time.time()

    with open(OUTPUT_CSV, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'unix_timestamp', 'relative_time', 'total_latency_ms', 'throughput'])

        try:
            while True:
                val = fetch_metric(QUERY_TOTAL_LATENCY)
                throughput = fetch_metric(THROUGHPUT_Q)

                # 💡 修正點：若 throughput 為 None 則預設為 0.0，避免 print 報錯
                display_throughput = throughput if throughput is not None else 0.0

                if val is not None:
                    current_unix = time.time()
                    elapsed = int(current_unix - start_time)
                    # 紀錄數據
                    writer.writerow([time.strftime("%H:%M:%S"), current_unix, elapsed, val, display_throughput])
                    f.flush()
                    print(f"[{time.strftime('%H:%M:%S')}] Time: {elapsed}s | Total Latency: {val:.2f} ms | Throughput: {display_throughput:.2f} records/s")
                else:
                    print("等待 Flink 指標數據 (Job 可能正在重啟中)...")

                time.sleep(SAMPLE_INTERVAL)
        except KeyboardInterrupt:
            print("\n監控停止，準備繪圖...")
            plot_results()

def plot_results():
    if not os.path.exists(OUTPUT_CSV) or os.stat(OUTPUT_CSV).st_size == 0:
        print("❌ CSV 檔案不存在或為空，無法繪圖。")
        return

    try:
        df = pd.read_csv(OUTPUT_CSV)

        # --- 圖表 1: Latency 延遲圖 ---
        plt.figure(figsize=(10, 6))
        plt.plot(df['relative_time'], df['total_latency_ms'], label='Proposed (Latency)', color='#1f77b4', linewidth=2)
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Latency (ms)', fontsize=12)
        plt.title('Total Latency Variation (Including Migration Cost)', fontsize=14)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.savefig(os.path.join(OUTPUT_FILE, 'experiment_latency_plot.png'))
        print(f"✅ 延遲圖表已生成: {os.path.join(OUTPUT_FILE, 'experiment_latency_plot.png')}")

        # --- 圖表 2: Throughput 吞吐量圖 ---
        plt.figure(figsize=(10, 6))
        plt.plot(df['relative_time'], df['throughput'], label='Proposed (Throughput)', color='#2ca02c', linewidth=2)
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Throughput (records/s)', fontsize=12)
        plt.title('Throughput Performance During Migration', fontsize=14)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.savefig(os.path.join(OUTPUT_FILE, 'experiment_throughput_plot.png'))
        print(f"✅ 吞吐量圖表已生成: {os.path.join(OUTPUT_FILE, 'experiment_throughput_plot.png')}")

    except Exception as e:
        print(f"❌ 繪圖過程中發生錯誤: {e}")

if __name__ == "__main__":
    monitor()