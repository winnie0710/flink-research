import requests
import pandas as pd
import time
import sys
import os
# 為了讓 Python 找得到 detector.py，有時候需要調整路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from detector import FlinkDetector

PROMETHEUS_URL = "http://localhost:9090"   # 設定 Prometheus URL
# 測試連線
def check_prometheus_connection():
    try:
        response = requests.get(f"{PROMETHEUS_URL}/-/healthy")
        if response.status_code == 200:
            print(f"✅ 成功連線到 Prometheus: {PROMETHEUS_URL}")
        else:
            print(f"⚠️ 連線怪怪的，狀態碼: {response.status_code}")
    except Exception as e:
        print(f"❌ 無法連線到 Prometheus: {e}")
        print("請檢查 Docker 是否有啟動 Prometheus？")

if __name__ == "__main__":
    print("--- CAOM Detector 啟動中 ---")
    print(f"Python 版本: {sys.version}")
    check_prometheus_connection()


# 開始監控
def start_monitoring_loop():
    detector = FlinkDetector()
    print("--- CAOM 全局監控模式 ---")

    # 標題欄位調整，增加 BP 欄位
    header = f"{'時間':<9} | {'Task Name':<20} | {'Busy':<6} | {'BP':<6} | {'Skew':<6} | {'狀態 & Details'}"
    print(header)
    print("-" * 100)

    try:
        while True:
            # 取得所有 Task 的分析報告 (這是一個 List)
            reports = detector.detect_bottleneck()
            timestamp = time.strftime("%H:%M:%S")

            if not reports:
                print(f"{timestamp:<9} | 等待 Flink 數據...")
            else:
                # 為了版面整潔，每次更新前可以考慮清空螢幕 (可選)
                # print("\033[H\033[J", end="") # 解開這行可以在 Linux/Mac 清空畫面
                # print(header)               # 若清空畫面，需重印標題

                for r in reports:
                    # 截短過長的 Task Name
                    short_name = (r['task_name'][:18] + '..') if len(r['task_name']) > 18 else r['task_name']

                    print(f"{timestamp:<9} | {short_name:<20} | {r['max_busy']:<6.0f} | {r['max_bp']:<6.0f} | {r['skew']:<6.0f} | {r['status']} {r['details']}")

                print("-" * 100) # 分隔線，區隔每一次的採樣

            time.sleep(5) # 建議縮短採樣時間讓畫面跳動更有感

    except KeyboardInterrupt:
        print("\n🛑 監控已停止")

if __name__ == "__main__":
    start_monitoring_loop()