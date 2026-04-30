#!/usr/bin/env python3
"""
CAOM 自動遷移腳本
持續監控 Flink 叢集，當檢測到過載時自動觸發遷移
"""

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from baseline import FlinkDetector

def main():
    print("=== CAOM 自動遷移系統 ===")

    # Job 配置（用於自動重新提交）
    job_config = {
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
            "--kafka-server", "kafka:9092",
            "--submit-only"
        ]
    }

    # 初始化 Detector
    detector = FlinkDetector(
        prometheus_url="http://localhost:9090",
        flink_rest_url="http://localhost:8081",
        migration_plan_path="/home/yenwei/research/structure_setup/plan/migration_plan.json",
        savepoint_dir="file:///opt/flink/savepoints",
        job_config=job_config
    )

    # 設定閾值
    BUSY_THRESHOLD = 700      # busyTime 超過 700ms 視為過載
    SKEW_THRESHOLD = 200      # 傾斜度超過 200 觸發遷移
    CHECK_INTERVAL = 20       # 每 20 秒檢查一次

    print(f"📊 監控參數:")
    print(f"   - Busy 閾值: {BUSY_THRESHOLD} ms/s")
    print(f"   - Skew 閾值: {SKEW_THRESHOLD} ms/s")
    print(f"   - 檢查間隔: {CHECK_INTERVAL} 秒")
    print(f"   - 遷移計畫路徑: {detector.migration_plan_path}")
    print(f"   - Savepoint 目錄: {detector.savepoint_dir}")
    print("\n開始監控...\n")
    time.sleep(10)

    try:
        while True:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}] 檢查叢集狀態...")

            # 顯示當前狀態
            reports = detector.detect_bottleneck()

            if reports:
                print("\n當前狀態 (Operator 級別):")
                for r in reports:
                    print(f"  {r['status']} {r['task_name']}:")
                    print(f"    - 總實際輸入速率 (Total Actual): {r['total_actual_rate']} rec/s")
                    print(f"    - 總最大處理容量 (Total Max): {r['total_max_capacity']} rec/s")
                    if r['is_bottleneck_operator']:
                        overload_pct = ((r['total_actual_rate'] - r['total_max_capacity']) / r['total_max_capacity'] * 100) if r['total_max_capacity'] > 0 else 0
                        print(f"    - ⚠️ 過載程度: {overload_pct:.1f}% (所有 {r['bottleneck_count']} 個 subtask 將被遷移)")
                    print(f"    - 平均速率: {r['avg_actual_rate']} rec/s (Actual) / {r['avg_max_capacity']} rec/s (Max)")
                    print(f"    - Busy: {r['max_busy']:.0f} ms/s, Backpressure: {r['max_bp']:.0f} ms/s")

                # 嘗試自動遷移
                migrated = detector.auto_detect_and_migrate(
                    busy_threshold=BUSY_THRESHOLD,
                    skew_threshold=SKEW_THRESHOLD
                )

                if migrated:
                    print("\n✅ 已觸發遷移流程")
                else:
                    print("\n✅ 叢集狀態正常，無需遷移")
            else:
                print("  ⏳ 等待 Flink 數據...")

            # 等待下次檢查
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n🛑 監控已停止")

if __name__ == "__main__":
    main()
