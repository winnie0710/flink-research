import time
import math
import os
import sys
import argparse  # 1. 引入 argparse
from confluent_kafka import Producer

# --- 固定設定區域 ---
BOOTSTRAP_SERVERS = 'localhost:9093'
TOPIC_NAME = 'nexmark-events'
PERIOD_SECONDS = 30

# 路徑
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
DATA_FILE_PATH = os.path.join(PROJECT_ROOT, 'source', 'nexmark_data_3.json')

# Kafka 配置
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'python-optimized-producer',
    'linger.ms': 10,
    'batch.size': 131072,
    'compression.type': 'lz4',
    'queue.buffering.max.messages': 2000000,
    'queue.buffering.max.kbytes': 1048576,
}

# 2. 修改此函式，接收 offset 與 amplitude 作為參數
def get_target_tps(elapsed_time, offset, amplitude):
    sine_value = math.sin(2 * math.pi * elapsed_time / PERIOD_SECONDS)
    return int(offset + amplitude * sine_value)

def main():
    # 3. 設定命令列參數解析
    parser = argparse.ArgumentParser(description='Kafka Replayer with Dynamic TPS Control')
    parser.add_argument('--min', type=int, default=60000, help='Minimum TPS (default: 60000)')
    parser.add_argument('--max', type=int, default=100000, help='Maximum TPS (default: 100000)')

    args = parser.parse_args()

    # 4. 根據輸入參數計算 OFFSET 與 AMPLITUDE
    min_tps = args.min
    max_tps = args.max
    offset = (max_tps + min_tps) // 2
    amplitude = (max_tps - min_tps) // 2

    if not os.path.exists(DATA_FILE_PATH):
        print(f"❌ 找不到資料檔: {DATA_FILE_PATH}")
        return

    print(f"🚀 初始化 Kafka Producer...")
    producer = Producer(conf)

    print(f"🌊 開始串流發送 (低記憶體模式): {min_tps} ~ {max_tps} TPS")

    start_time = time.time()
    total_sent = 0
    BATCH_SIZE = 5000
    batch_count = 0
    batch_start_time = time.time()

    try:
        with open(DATA_FILE_PATH, 'rb') as f:
            while True:
                line = f.readline()
                if not line:
                    f.seek(0)
                    continue

                try:
                    producer.produce(TOPIC_NAME, line, on_delivery=None)
                except BufferError:
                    producer.poll(0.1)
                    producer.produce(TOPIC_NAME, line, on_delivery=None)

                batch_count += 1
                total_sent += 1

                if batch_count >= BATCH_SIZE:
                    producer.poll(0)

                    now = time.time()
                    elapsed_total = now - start_time
                    # 5. 傳入計算好的 offset 與 amplitude
                    current_target_tps = get_target_tps(elapsed_total, offset, amplitude)

                    expected_duration = batch_count / current_target_tps
                    actual_duration = now - batch_start_time

                    if actual_duration < expected_duration:
                        sleep_time = expected_duration - actual_duration
                        time.sleep(sleep_time)

                    batch_count = 0
                    batch_start_time = time.time()

                    if int(elapsed_total) % 5 == 0 and int(elapsed_total * 10) % 10 < 2:
                        real_tps = BATCH_SIZE / (actual_duration + (sleep_time if 'sleep_time' in locals() else 0))
                        sys.stdout.write(f"\r⏱️ {int(elapsed_total)}s | Target: {current_target_tps // 1000}k | Real: {int(real_tps // 1000)}k | Total: {total_sent}")
                        sys.stdout.flush()

    except KeyboardInterrupt:
        print("\n🛑 停止重放。")
    finally:
        print("正在將剩餘資料寫入 Kafka...")
        producer.flush()

if __name__ == "__main__":
    main()