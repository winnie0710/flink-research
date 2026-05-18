#!/usr/bin/env python3
"""
執行我的方法
持續監控 Flink 叢集，當檢測到過載時自動觸發遷移
"""

import sys
import os
import time
import argparse
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from propose_v10 import FlinkPropose, JOB_CONFIG   # ← v9 → v10

def main():
    parser = argparse.ArgumentParser(description="propose 自動遷移系統")
    parser.add_argument("--query", required=True, choices=list(JOB_CONFIG.keys()),
                        help="Nexmark query to monitor")
    parser.add_argument("--id", dest="output_id", default="t16",
                        help="Experiment output folder name (default: t16)")
    parser.add_argument("--initial-placement", dest="initial_placement_path", default=None,
                        help="若指定，第一次遷移前將 Flink 初始 Subtask 配置備份到此路徑")
    parser.add_argument("--migration-record", dest="migration_record_path", default=None,
                        help="每次遷移後將完整記錄（subtask前後位置/state/中斷時間/原因）寫入此路徑")
    args = parser.parse_args()

    print("=== propose 自動遷移系統 ===")

    cfg = JOB_CONFIG[args.query]

    # Job 配置（用於自動重新提交）
    job_config = {
        "container": "jobmanager",
        "entry_class": cfg["entry_class"],
        "parallelism": 4,
        "jar_path": "/opt/flink/usrlib/nexmark.jar",
        "nexmark_conf_dir": "/opt/nexmark",
        "program_args": [
            "--queries", cfg["queries_arg"],
            "--location", "/opt/nexmark",
            "--suite-name", "100m",
            "--category", "oa",
            "--kafka-server", "kafka:9092",
            "--submit-only"
        ]
    }

    # 初始化 propose
    propose = FlinkPropose(
        query_type=args.query,
        output_id=args.output_id,
        prometheus_url="http://localhost:9090",
        flink_rest_url="http://localhost:8081",
        migration_plan_path="/home/yenwei/research/structure_setup/plan/migration_plan.json",
        savepoint_dir="file:///opt/flink/savepoints",
        initial_placement_path=args.initial_placement_path,
        migration_record_path=args.migration_record_path,
        #job_config=job_config
    )

    CHECK_INTERVAL = 30   # 每 30 秒檢查一次
    # ⚠️  須與傳入 auto_detect_and_migrate 的
    #    monitor_interval_sec 保持一致，
    #    否則 v10 的時間換算（趨勢斜率/預測窗口）會出錯

    print(f"📊 監控參數:")
    print(f"   - 檢查間隔         : {CHECK_INTERVAL} 秒（歷史取樣粒度）")
    print(f"   - 時間搜尋步長     : 5 秒（固定，與檢查間隔無關）")
    print(f"   - 遷移計畫路徑     : {propose.migration_plan_path}")
    print(f"   - Savepoint 目錄   : {propose.savepoint_dir}")
    print("\n開始監控...\n")

    #time.sleep(20)

    try:
        while True:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}] Propose 檢查叢集狀態...")

            running_jobs = propose.get_running_jobs()

            if running_jobs:

                # ── v10：排程等待中時直接重入，跳過 get_running_jobs 以外的前置動作 ──
                # _migration_scheduled=True 代表 evaluate_migration_trigger 已判定
                # 值得遷移但建議延遲，auto_detect_and_migrate 會重新預測並判斷是否到時機。
                # 此路徑下不需要重新 detect_bottleneck（避免重複查詢 Prometheus），
                # 直接傳入最新 latency 讓動態冷卻與時機判斷使用。
                if propose._migration_scheduled and propose._pending_migration_plan:
                    elapsed = time.time() - propose._migration_ready_time
                    print(f"⏳ [v10-Timing] 排程等待中 ({elapsed:.0f}s)，重新評估最佳時機...")
                    current_latency = propose.fetch_current_latency()
                    migrated = propose.auto_detect_and_migrate(
                        monitor_interval_sec=CHECK_INTERVAL,
                        current_latency_ms=current_latency,
                    )
                    if migrated:
                        print("\n✅ 已觸發遷移流程")
                    time.sleep(CHECK_INTERVAL)
                    continue

                # ── 正常輪次：fetch latency 後呼叫完整流程 ──
                # current_latency 用途：
                #   1. trigger_migration 裡的動態冷卻（latency 回落可提前結束冷卻期）
                #   2. 未來可擴充：傳入 evaluate_migration_trigger 用於 C_stay 計算
                current_latency = propose.fetch_current_latency()
                if current_latency is not None:
                    print(f"   當前 latency: {current_latency:.0f} ms")

                migrated = propose.auto_detect_and_migrate(
                    monitor_interval_sec=CHECK_INTERVAL,
                    current_latency_ms=current_latency,
                )

                if migrated:
                    print("\n✅ 已觸發遷移流程")
                else:
                    print("\n✅ 叢集狀態正常或等待最佳遷移時機")

            else:
                print("  ⏳ 等待 Flink 數據...")

            # 等待下次檢查
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n🛑 監控已停止")

if __name__ == "__main__":
    main()