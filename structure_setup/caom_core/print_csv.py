import pandas as pd
import matplotlib.pyplot as plt
import os

# === 設定區 ===
# 請在此替換成你實際的檔案路徑
FILES = {
    'Native Flink': '/home/yenwei/research/structure_setup/output/t6_native/latency_data.csv',
    'Proposed': '/home/yenwei/research/structure_setup/output/t6_caom_2/latency_data.csv',
    'CAOM': '/home/yenwei/research/structure_setup/output/t6_2/latency_data.csv'
}

OUTPUT_DIR = "/home/yenwei/research/structure_setup/output/compare"
X_LIMIT = (0, 300)  # 固定橫軸範圍

def load_and_plot():
    # 確保輸出目錄存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 初始化兩張圖表
    fig_lat, ax_lat = plt.subplots(figsize=(10, 6))
    fig_tp, ax_tp = plt.subplots(figsize=(10, 6))

    # 定義顏色循環
    colors = ['#7f7f7f', '#ff7f0e', '#1f77b4'] # 灰色, 橘色, 藍色 (模擬圖10配色)

    for (label, path), color in zip(FILES.items(), colors):
        if not os.path.exists(path):
            print(f"⚠️ 找不到檔案: {path}，跳過...")
            continue

        # 讀取數據
        df = pd.read_csv(path)

        # 確保數據在 0-300s 範圍內
        df_filtered = df[(df['relative_time'] >= X_LIMIT[0]) & (df['relative_time'] <= X_LIMIT[1])]

        # --- 繪製 Latency 圖 ---
        ax_lat.plot(df_filtered['relative_time'], df_filtered['total_latency_ms'],
                    label=label, color=color, linewidth=2)

        # --- 繪製 Throughput 圖 ---
        ax_tp.plot(df_filtered['relative_time'], df_filtered['throughput'],
                   label=label, color=color, linewidth=2)

    # --- 格式化 Latency 圖表 ---
    ax_lat.set_xlim(X_LIMIT)
    ax_lat.set_xlabel('Time (s)', fontsize=12)
    ax_lat.set_ylabel('Total Latency (ms)', fontsize=12)
    ax_lat.set_title('Total Latency Variation Comparison (0-300s)', fontsize=14)
    ax_lat.grid(True, linestyle='--', alpha=0.6)
    ax_lat.legend()
    fig_lat.tight_layout()
    fig_lat.savefig(os.path.join(OUTPUT_DIR, 'comparison_latency_300s_1.png'))

    # --- 格式化 Throughput 圖表 ---
    ax_tp.set_xlim(X_LIMIT)
    ax_tp.set_xlabel('Time (s)', fontsize=12)
    ax_tp.set_ylabel('Throughput (records/s)', fontsize=12)
    ax_tp.set_title('Throughput Performance Comparison (0-300s)', fontsize=14)
    ax_tp.grid(True, linestyle='--', alpha=0.6)
    ax_tp.legend()
    fig_tp.tight_layout()
    fig_tp.savefig(os.path.join(OUTPUT_DIR, 'comparison_throughput_300s_1.png'))

    print(f"\n✅ 對比圖表已生成於: {OUTPUT_DIR}")
    print(f"   1. comparison_latency_300s.png")
    print(f"   2. comparison_throughput_300s.png")

if __name__ == "__main__":
    load_and_plot()