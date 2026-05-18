import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

# === 設定區 ===
RESULTS_DIR = '/home/yenwei/research/structure_setup/results'
OUTPUT_BASE = '/home/yenwei/research/structure_setup/results'
X_LIMIT = (0, 900)  # 固定橫軸範圍

# 批次模式：支援萬用字元，例如 'run_q*' 會處理所有 run_q 開頭的資料夾
# 留空則處理 RESULTS_DIR 下所有 run_* 資料夾
BATCH_RUNS = [
    'run_q*',
]

# 每個 run 資料夾內的子目錄對應關係（label: 子目錄名稱）
SUBDIR_MAP = {
    'Baseline': 'baseline',
    'Proposed': 'propose',
}
COLORS = {'Baseline': '#1f77b4', 'Proposed': '#ff7f0e'}


def plot_run(run_name):
    run_dir = os.path.join(RESULTS_DIR, run_name)
    output_dir = os.path.join(OUTPUT_BASE, run_name)
    os.makedirs(output_dir, exist_ok=True)

    fig_lat, ax_lat = plt.subplots(figsize=(10, 6))
    fig_tp, ax_tp = plt.subplots(figsize=(10, 6))

    for label, subdir in SUBDIR_MAP.items():
        path = os.path.join(run_dir, subdir, 'latency_data.csv')
        if not os.path.exists(path):
            print(f"  ⚠️ 找不到: {path}，跳過")
            continue

        df = pd.read_csv(path)
        df_filtered = df[(df['relative_time'] >= X_LIMIT[0]) & (df['relative_time'] <= X_LIMIT[1])]
        color = COLORS.get(label, None)

        ax_lat.plot(df_filtered['relative_time'], df_filtered['total_latency_ms'],
                    label=label, color=color, linewidth=2)
        ax_tp.plot(df_filtered['relative_time'], df_filtered['throughput'],
                   label=label, color=color, linewidth=2)

    for ax, ylabel, title, fname in [
        (ax_lat, 'Total Latency (ms)', f'Total Latency Comparison – {run_name}', 'comparison_latency.png'),
        (ax_tp,  'Throughput (records/s)', f'Throughput Comparison – {run_name}', 'comparison_throughput.png'),
    ]:
        ax.set_xlim(X_LIMIT)
        ax.set_xlabel('Time (s)', fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.set_title(title, fontsize=13)
        ax.grid(True, linestyle='--', alpha=0.6)
        ax.legend()

    fig_lat.tight_layout()
    fig_lat.savefig(os.path.join(output_dir, 'comparison_latency.png'))
    plt.close(fig_lat)

    fig_tp.tight_layout()
    fig_tp.savefig(os.path.join(output_dir, 'comparison_throughput.png'))
    plt.close(fig_tp)

    print(f"  ✅ {run_name} → {output_dir}")


def main():
    if BATCH_RUNS:
        # 支援萬用字元展開，例如 'run_q*'
        runs = []
        for pattern in BATCH_RUNS:
            matched = sorted(
                os.path.basename(p)
                for p in glob.glob(os.path.join(RESULTS_DIR, pattern))
                if os.path.isdir(p)
            )
            runs.extend(matched)
    else:
        # 留空時，處理所有 run_* 資料夾
        runs = sorted(
            os.path.basename(p) for p in glob.glob(os.path.join(RESULTS_DIR, 'run_*'))
            if os.path.isdir(p)
        )

    print(f"共處理 {len(runs)} 個 run 資料夾，輸出至 {OUTPUT_BASE}\n")
    for run in runs:
        plot_run(run)
    print(f"\n全部完成。")


if __name__ == "__main__":
    main()