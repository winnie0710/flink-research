import time
import math
import os
import sys
from kafka import KafkaProducer

# --- 設定區域 ---

# 1. Kafka 設定
# 注意：如果在 Docker 外部(本機)執行且沒改 /etc/hosts，這裡可能需要改回 'localhost:9093'
BOOTSTRAP_SERVERS = 'localhost:9093'
TOPIC_NAME = 'nexmark-events'

# 2. 速率控制 (正弦波: 5000 ~ 20000 TPS)
MIN_TPS = 100000
MAX_TPS = 300000
PERIOD_SECONDS = 60

# 計算正弦波參數
OFFSET = (MAX_TPS + MIN_TPS) // 2      # 中心點
AMPLITUDE = (MAX_TPS - MIN_TPS) // 2   # 振幅

# 3. 資料檔案路徑設定
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) # .../caom_core
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)              # .../structure_setup
DATA_FILE_PATH = os.path.join(PROJECT_ROOT, 'source', 'nexmark_data.json')

def get_target_tps(elapsed_time):
    """
    根據經過時間計算當下的目標 TPS (正弦波)
    """
    sine_value = math.sin(2 * math.pi * elapsed_time / PERIOD_SECONDS)
    target_tps = OFFSET + AMPLITUDE * sine_value
    return int(target_tps)

def main():
    # 檢查檔案是否存在
    if not os.path.exists(DATA_FILE_PATH):
        print(f"❌ 找不到資料檔: {DATA_FILE_PATH}")
        print(f"預期路徑為: {os.path.abspath(DATA_FILE_PATH)}")
        return

    print(f"🚀 初始化 Kafka Producer ({BOOTSTRAP_SERVERS})...")

    # ==========================================
    # 🔥 修改重點在這裡 (約第 46 行開始)
    # ==========================================
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            # --- 優化參數 ---
            linger_ms=20,           # 關鍵：延遲 20ms 以堆疊批次
            batch_size=131072,      # 關鍵：批次大小加大到 128KB
            buffer_memory=33554432, # 緩衝區加大到 32MB
            compression_type='gzip' # 開啟壓縮以節省頻寬
        )
        print("✅ 已啟用 Batch 優化模式 (linger_ms=20, compression=gzip)")
    except Exception as e:
        print(f"❌ 無法連線到 Kafka ({BOOTSTRAP_SERVERS}): {e}")
        print("💡 提示: 如果您在本機執行，請嘗試將位址改為 'localhost:9092'")
        return
    # ==========================================

    print(f"📂 準備讀取檔案: {DATA_FILE_PATH}")
    print(f"🌊 速率模式: 正弦波 ({MIN_TPS} ~ {MAX_TPS} TPS), 週期 {PERIOD_SECONDS}秒")
    print("--- 開始重放 (按 Ctrl+C 停止) ---")

    start_time = time.time()
    total_sent = 0

    # 批次控制 (Batch Control) - 這裡的 batch 是指 "檢查速率的頻率"
    # 建議稍微加大這個數值，減少呼叫 time.time() 的次數，進一步降低 CPU 負擔
    BATCH_SIZE = 20000
    batch_count = 0
    batch_start_time = time.time()

    try:
        with open(DATA_FILE_PATH, 'rb') as f:

            # === 無限循環開始 ===
            while True:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # 1. 發送資料 (現在這是非同步且高效的)
                    producer.send(TOPIC_NAME, value=line)

                    total_sent += 1
                    batch_count += 1

                    # 2. 速率控制
                    if batch_count >= BATCH_SIZE:
                        now = time.time()
                        elapsed_total = now - start_time

                        # 計算目標速率
                        current_target_tps = get_target_tps(elapsed_total)

                        # 計算應該花的時間 vs 實際花的時間
                        expected_duration = batch_count / current_target_tps
                        actual_duration = now - batch_start_time

                        # 如果發太快，就睡覺
                        if actual_duration < expected_duration:
                            time.sleep(expected_duration - actual_duration)

                        # 重置批次
                        batch_count = 0
                        batch_start_time = time.time()

                        # 顯示進度
                        if int(elapsed_total) % 5 == 0 and int(elapsed_total * 10) % 10 < 2:
                            print(f"⏱️ {int(elapsed_total)}s | Target: {current_target_tps:<5} | Total {total_sent}")

                # === 檔案讀取完畢，重置指標 ===
                # print("🔄 檔案讀取完畢，重置指標到開頭，繼續循環發送...") # 註解掉避免洗版
                f.seek(0)

    except KeyboardInterrupt:
        print("\n🛑 使用者中斷重放。")
    except Exception as e:
        print(f"\n❌ 發生錯誤: {e}")
    finally:
        if 'producer' in locals():
            print("正在將剩餘資料寫入 Kafka...")
            producer.flush()
            producer.close()
        print(f"總共發送: {total_sent} 筆數據。")

if __name__ == "__main__":
    main()