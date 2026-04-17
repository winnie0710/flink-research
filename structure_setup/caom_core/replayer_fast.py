import time
import math
import os
import sys
from confluent_kafka import Producer

# --- 設定區域 ---
BOOTSTRAP_SERVERS = 'localhost:9093'
TOPIC_NAME = 'nexmark-events'

# 速率控制 (正弦波)
MIN_TPS = 20000
MAX_TPS = 50000
PERIOD_SECONDS = 30
OFFSET = (MAX_TPS + MIN_TPS) // 2
AMPLITUDE = (MAX_TPS - MIN_TPS) // 2

# 路徑
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
DATA_FILE_PATH = os.path.join(PROJECT_ROOT, 'source', 'nexmark_data_2.json')

# Kafka 配置 (維持你的高效能設定)
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'python-optimized-producer',
    'linger.ms': 10,
    'batch.size': 131072,
    'compression.type': 'gzip',
    'queue.buffering.max.messages': 2000000,
    'queue.buffering.max.kbytes': 1048576, # 降為 1GB，因為我們不再吃掉大量 RAM 存資料，可以留多一點給 Buffer
}

def get_target_tps(elapsed_time):
    sine_value = math.sin(2 * math.pi * elapsed_time / PERIOD_SECONDS)
    return int(OFFSET + AMPLITUDE * sine_value)

def main():
    if not os.path.exists(DATA_FILE_PATH):
        print(f"❌ 找不到資料檔: {DATA_FILE_PATH}")
        return

    print(f"🚀 初始化 Kafka Producer...")
    producer = Producer(conf)

    print(f"🌊 開始串流發送 (低記憶體模式): {MIN_TPS} ~ {MAX_TPS} TPS")

    start_time = time.time()
    total_sent = 0

    # 調整檢查頻率：每 5000 筆檢查一次速率，減少 math.sin 計算次數
    BATCH_SIZE = 5000
    batch_count = 0
    batch_start_time = time.time()

    try:
        # 使用 'rb' (Read Binary) 模式，極大降低 CPU 消耗
        # 因為 Kafka 傳輸的是 bytes，我們不需要將其解碼為 string 再編碼回去
        with open(DATA_FILE_PATH, 'rb') as f:
            while True:
                # 讀取一行 (包含換行符號 \n，這通常不影響 JSON 解析，或者 Consumer 端再處理)
                line = f.readline()

                # 如果讀到空 bytes，表示檔案結束，重置指標
                if not line:
                    f.seek(0)
                    continue

                # 發送 (非同步)
                try:
                    # 直接發送 bytes，完全沒有 CPU encode overhead
                    producer.produce(TOPIC_NAME, line, on_delivery=None)
                except BufferError:
                    producer.poll(0.1)
                    producer.produce(TOPIC_NAME, line, on_delivery=None)

                batch_count += 1
                total_sent += 1

                # 速率控制與 Poll
                if batch_count >= BATCH_SIZE:
                    # 1. 強制讓 Kafka client 處理網路 I/O 和 callbacks
                    producer.poll(0)

                    # 2. 計算速率
                    now = time.time()
                    elapsed_total = now - start_time
                    current_target_tps = get_target_tps(elapsed_total)

                    expected_duration = batch_count / current_target_tps
                    actual_duration = now - batch_start_time

                    if actual_duration < expected_duration:
                        sleep_time = expected_duration - actual_duration
                        # 精確睡眠，避免睡太久導致 Buffer 滿出來
                        if sleep_time > 0.05:
                            time.sleep(sleep_time)
                        else:
                            # 極短時間用 busy wait 或者 pass 可能是更好的選擇，但 Python sleep 精度有限
                            time.sleep(sleep_time)

                    # 重置計數
                    batch_count = 0
                    batch_start_time = time.time()

                    # 顯示 Log (每 5 秒)
                    if int(elapsed_total) % 5 == 0 and int(elapsed_total * 10) % 10 < 2:
                        # 計算這批次的實際 TPS (僅供參考)
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