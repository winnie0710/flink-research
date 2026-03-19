import pandas as pd
import matplotlib.pyplot as plt

# 1. 讀取數據
latency_df = pd.read_csv("/home/yenwei/research/structure_setup/output/latency_data.csv")
perf_df = pd.read_csv("/home/yenwei/research/structure_setup/output/propose_migration_performance.csv")

# 2. 取得基準時間 (監控開始的時間)
t_start = latency_df['unix_timestamp'].iloc[0]

# 3. 繪製延遲曲線
plt.plot(latency_df['unix_timestamp'] - t_start, latency_df['total_latency_ms'], label='Latency')

# 4. 畫出遷移起始線 (垂直線)
for _, row in perf_df.iterrows():
    migration_x = row['event_timestamp'] - t_start
    # 標註遷移開始
    plt.axvline(x=migration_x, color='r', linestyle='--', label='Migration Start')
    # 標註中斷區間 (Downtime)
    plt.axvspan(migration_x, migration_x + row['total_downtime'], color='red', alpha=0.2, label='Downtime')

plt.xlabel("Elapsed Time (s)")
plt.ylabel("Latency (ms)")
plt.legend()
plt.show()