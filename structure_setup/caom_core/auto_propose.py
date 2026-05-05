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
from propose_v8 import FlinkPropose, JOB_CONFIG

def main():
    parser = argparse.ArgumentParser(description="propose 自動遷移系統")
    parser.add_argument("--query", required=True, choices=list(JOB_CONFIG.keys()),
                        help="Nexmark query to monitor")
    parser.add_argument("--id", dest="output_id", default="t16",
                        help="Experiment output folder name (default: t16)")
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
        job_config=job_config
    )

    CHECK_INTERVAL = 30       # 每 30 秒檢查一次

    print(f"📊 監控參數:")
    print(f"   - 檢查間隔: {CHECK_INTERVAL} 秒")
    print(f"   - 遷移計畫路徑: {propose.migration_plan_path}")
    print(f"   - Savepoint 目錄: {propose.savepoint_dir}")
    print("\n開始監控...\n")

    time.sleep(20)

    try:
        while True:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}] Propose 檢查叢集狀態...")

            # 確認有 job 在執行且有 checkpoint，避免與 auto_detect_and_migrate 重複執行 detect_bottleneck
            running_jobs = propose.get_running_jobs()


            if running_jobs:

                # 嘗試自動遷移
                migrated = propose.auto_detect_and_migrate()

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
