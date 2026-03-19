"""
包含資料在外部 Kafka 排隊 + Savepoint 停機時間 + Flink 處理時間。Savepoint 期間，外部資料（如 Kafka）仍在堆積也算延遲（遷移帶來的成本）
總延遲 = (進入 Flink 前的排隊時間) + (Flink 內部的處理路徑時間)
跨 Job 連續性: 遷移後 Job ID 會改變，Prometheus 的舊指標會失效
查詢語句使用 job_name 作為過濾標籤。只要你的 Nexmark Job 名稱不變，腳本就能在 Job B 啟動後的幾秒內自動抓到新數據，並在 CSV 中銜接，不會因為 Job ID 改變而導致數據斷掉。
"""
import requests
import time
import csv
import pandas as pd
import matplotlib.pyplot as plt
import os

# === 設定區 ===
PROM_URL = "http://localhost:9090"
# 請確保此名稱與你 Grafana 截圖中的 job_name 一致
JOB_NAME = "Nexmark_Q7_Isolated__Benchmark_Driver_"
OUTPUT_FILE = "/home/yenwei/research/structure_setup/output"
OUTPUT_CSV = "/home/yenwei/research/structure_setup/output/latency_data.csv"
SAMPLE_INTERVAL = 2  # 每 2 秒採樣一次

# Prometheus 查詢語句 (加總外部 Lag 與 內部 Latency)
# 使用 max() 確保抓取的是 Critical Path (最慢的路徑)
QUERY_TOTAL_LATENCY = f"""
  max(flink_taskmanager_job_task_operator_currentEmitEventTimeLag{{job_name="{JOB_NAME}"}}) 
  + 
  max(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{{job_name="{JOB_NAME}"}})
"""
THROUGHPUT_Q = f'sum(flink_taskmanager_job_task_numRecordsInPerSecond{{job_name="{JOB_NAME}", task_name=~".*Sink.*"}})'

# TODO : 這個程式的throughput計算方式和 內建metric 不同 35K vs 800K
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

    file_exists = os.path.isfile(OUTPUT_CSV)
    # 紀錄絕對時間 供不同 csv 檔做時間對齊
    start_time = time.time()

    with open(OUTPUT_CSV, 'a', newline='') as f:
        writer = csv.writer(f)
        # 若檔案不存在 創新csv時要先寫標頭
        if not file_exists:
            writer.writerow(['timestamp', 'unix_timestamp', 'relative_time', 'total_latency_ms', 'throughput'])

        try:
            while True:
                val = fetch_metric(QUERY_TOTAL_LATENCY)
                throughput = fetch_metric(THROUGHPUT_Q)
                # 遷移中斷時間可能沒有 throughput 預設0
                display_throughput = throughput if throughput is not None else 0.0
                if val is not None:
                    current_unix = time.time() # 💡 獲取精確到小數點的 Unix Timestamp
                    elapsed = int(current_unix - start_time)
                    # unix_timestamp：用於跨檔案對齊。 relative_time：用於繪圖的橫軸（秒）。
                    writer.writerow([time.strftime("%H:%M:%S"), current_unix, elapsed, val, throughput])
                    f.flush()
                    print(f"[{time.strftime('%H:%M:%S')}] Time: {elapsed}s | Total Latency: {val:.2f} ms | Throughput: {display_throughput:.2f} records/s")
                else:
                    print("等待 Flink 指標數據 (Job 可能正在重啟中)...")

                time.sleep(SAMPLE_INTERVAL)
        except KeyboardInterrupt:
            # 按下 ctrl+c
            print("\n監控停止，準備繪圖...")
            plot_results()


# TODO latency & throughput 分開畫成2張圖 各自跟 native flink & caom 比較
def plot_results():
    if not os.path.exists(OUTPUT_CSV):
        return

    df = pd.read_csv(OUTPUT_CSV)

    # 設置繪圖風格 (模擬圖 10)
    plt.figure(figsize=(10, 6))
    plt.plot(df['relative_time'], df['total_latency_ms'], label='Proposed ', color='#1f77b4', linewidth=2)

    # 圖表格式化
    plt.xlabel('Time windows (1s)', fontsize=12)
    plt.ylabel('Latency in actual pattern (ms)', fontsize=12)
    plt.title('Latency Variation During Migration', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()

    # 儲存圖片
    plt.savefig('latency_experiment_plot.png')
    print("圖表已生成: latency_experiment_plot.png")

if __name__ == "__main__":
    monitor()


