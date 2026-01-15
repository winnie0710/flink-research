#!/usr/bin/env python3
"""
CAOM 自動遷移腳本
持續監控 Flink 叢集，當檢測到過載時自動觸發遷移
"""

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from detector import FlinkDetector

def main():
    print("=== CAOM 自動遷移系統 ===")

    # 初始化 Detector
    detector = FlinkDetector(
        prometheus_url="http://localhost:9090",
        flink_rest_url="http://localhost:8081",
        migration_plan_path="/home/yenwei/research/structure_setup/plan/migration_plan.json",
        savepoint_dir="file:///opt/flink/savepoints"
    )

    # 設定閾值
    BUSY_THRESHOLD = 700      # busyTime 超過 700ms 視為過載
    SKEW_THRESHOLD = 200      # 傾斜度超過 200 觸發遷移
    CHECK_INTERVAL = 20       # 每 30 秒檢查一次

    print(f"📊 監控參數:")
    print(f"   - Busy 閾值: {BUSY_THRESHOLD} ms/s")
    print(f"   - Skew 閾值: {SKEW_THRESHOLD} ms/s")
    print(f"   - 檢查間隔: {CHECK_INTERVAL} 秒")
    print(f"   - 遷移計畫路徑: {detector.migration_plan_path}")
    print(f"   - Savepoint 目錄: {detector.savepoint_dir}")
    print("\n開始監控...\n")

    try:
        while True:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}] 檢查叢集狀態...")

            # 顯示當前狀態
            reports = detector.detect_bottleneck()

            if reports:
                print("\n當前狀態:")
                for r in reports:
                    print(f"  {r['status']} {r['task_name']}: Busy={r['max_busy']:.0f}, Skew={r['skew']:.0f}")

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
