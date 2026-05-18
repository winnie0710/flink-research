import time
import math
import os
import sys
import argparse
import random
import json
from collections import deque
from confluent_kafka import Producer

# --- 固定設定區域 ---
BOOTSTRAP_SERVERS = 'localhost:9093'
TOPIC_NAME = 'nexmark-events'
PERIOD_SECONDS = 30

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
DATA_FILE_PATH = os.path.join(PROJECT_ROOT, 'source', 'nexmark_data_4.json')

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'python-optimized-producer',
    'linger.ms': 10,
    'batch.size': 131072,
    'compression.type': 'lz4',
    'queue.buffering.max.messages': 2000000,
    'queue.buffering.max.kbytes': 1048576,
}

# ──────────────────────────────────────────────────────────────────────────────
# TPS 模型：五種模式
# ──────────────────────────────────────────────────────────────────────────────

class SineModel:
    """原始正弦波模式（保留作為基準對照）"""
    def __init__(self, offset, amplitude):
        self.offset = offset
        self.amplitude = amplitude

    def get_tps(self, elapsed):
        return int(self.offset + self.amplitude * math.sin(2 * math.pi * elapsed / PERIOD_SECONDS))


class RandomWalkModel:
    """
    隨機遊走模式：TPS 像股價一樣在範圍內漂移，
    適合模擬沒有明顯週期的真實流量（例如 IoT 感測器、後台批次任務觸發）。
    """
    def __init__(self, offset, amplitude, volatility=0.03, mean_reversion=0.05):
        self.current = float(offset)
        self.offset = offset
        self.amplitude = amplitude
        self.volatility = volatility
        self.mean_reversion = mean_reversion

    def get_tps(self, elapsed):
        reversion = self.mean_reversion * (self.offset - self.current)
        noise = random.gauss(0, self.volatility * self.amplitude)
        self.current += reversion + noise
        self.current = max(self.offset - self.amplitude,
                           min(self.offset + self.amplitude, self.current))
        return int(self.current)


class BurstyModel:
    """
    突發尖峰模式：大部分時間在基線附近，偶爾觸發流量尖峰。
    適合模擬：電商大促、直播開播、整點報時等突然湧入場景。
    """
    def __init__(self, offset, amplitude,
                 burst_prob=0.003,
                 burst_multiplier=2.0,
                 burst_duration=5.0):
        self.offset = offset
        self.amplitude = amplitude
        self.burst_prob = burst_prob
        self.burst_multiplier = burst_multiplier
        self.burst_duration = burst_duration
        self.in_burst = False
        self.burst_end_time = 0.0
        self.base_noise_ratio = 0.08

    def get_tps(self, elapsed):
        now = time.monotonic()
        if not self.in_burst and random.random() < self.burst_prob:
            self.in_burst = True
            self.burst_end_time = now + self.burst_duration
            print(f"\n⚡ 尖峰觸發！持續 {self.burst_duration:.1f}s", flush=True)
        if self.in_burst and now > self.burst_end_time:
            self.in_burst = False
            print(f"\n✅ 尖峰結束，回到基線", flush=True)
        max_tps = self.offset + self.amplitude
        if self.in_burst:
            target = max_tps * self.burst_multiplier
            target *= (1 + random.uniform(-0.05, 0.05))
        else:
            target = self.offset + random.uniform(
                -self.amplitude * self.base_noise_ratio,
                self.amplitude * self.base_noise_ratio)
        return int(target)


class CompositeModel:
    """
    複合真實模式（推薦）：正弦長週期趨勢 + 隨機遊走中期擾動 + 偶發尖峰。
    這是最接近真實生產流量的模式。
    """
    def __init__(self, offset, amplitude,
                 sine_weight=0.5,
                 noise_weight=0.3,
                 burst_weight=0.2,
                 burst_prob=0.002,
                 burst_duration=4.0):
        self.offset = offset
        self.amplitude = amplitude
        self.sine_weight = sine_weight
        self.noise_weight = noise_weight
        self._rw = 0.0
        self._rw_vol = 0.02
        self.burst_prob = burst_prob
        self.burst_duration = burst_duration
        self.in_burst = False
        self.burst_end_time = 0.0
        self.burst_boost = amplitude * burst_weight * 2

    def get_tps(self, elapsed):
        now = time.monotonic()
        sine = self.sine_weight * self.amplitude * math.sin(
            2 * math.pi * elapsed / PERIOD_SECONDS)
        self._rw += random.gauss(0, self._rw_vol * self.amplitude)
        self._rw *= 0.95
        rw_limit = self.noise_weight * self.amplitude
        self._rw = max(-rw_limit, min(rw_limit, self._rw))
        if not self.in_burst and random.random() < self.burst_prob:
            self.in_burst = True
            self.burst_end_time = now + self.burst_duration
            print(f"\n⚡ 複合尖峰！+{self.burst_boost/1000:.0f}k TPS，持續 {self.burst_duration:.1f}s", flush=True)
        if self.in_burst and now > self.burst_end_time:
            self.in_burst = False
            print(f"\n✅ 尖峰結束", flush=True)
        burst = self.burst_boost if self.in_burst else 0.0
        tps = self.offset + sine + self._rw + burst
        tps = max(self.offset - self.amplitude, tps)
        return int(tps)


class ScenarioModel:
    """
    時間軸劇本模式 — 為「需要兩次遷移」的對照實驗設計。

    設計動機：
        composite 模式的 TPS 本質是單峰曲線，遷移一次後系統就在自然下降段恢復，
        無法製造「第一次遷移解決後，第二波流量再次打爆 baseline 但不打爆 propose」
        的對照場景。ScenarioModel 用明確的時間軸劇本控制 TPS，確保：

        1. 第一波高峰（t=peak1_start ~ peak1_end）：兩個方法都觸發遷移
        2. 緩和期（t=valley_start ~ valley_end）：系統看起來穩定了
        3. 第二波高峰（t=peak2_start ~ peak2_end）：
           - propose 因為第一次遷移決策較好（精準識別瓶頸 + 拓撲親和力），
             叢集資源已有餘裕，不需要再次遷移
           - baseline 因為第一次把整個算子的健康 subtask 也搬走，
             佈局次優，面對第二波時再次超載，需要第二次遷移

    TPS 曲線由線性插值段組成（keyframe 動畫概念）：
        每個 keyframe 是 (時間秒, TPS目標值)，以 30 秒為基本單位，
        get_tps() 根據當前 elapsed 找到所在的區間，做線性插值。

    內建劇本（--scenario 選擇），均設計為 900s 實驗總時長：
        double_peak   雙峰劇本（預設）：30s 快速爬坡 → 第一峰(60s) → 緩和 → 第二峰(330s，+10%)
        triple_peak   三峰劇本：同樣早峰 → 第二峰(300s，+10%) → 第三峰(570s，~75%，較小)
        step_stress   階梯壓力：逐步加壓，測試系統邊界

    noise_ratio：
        在線性插值結果上疊加微小隨機抖動（預設 ±3%），
        讓曲線看起來更自然，不像完美的折線。
        設為 0.0 可完全關閉，得到完全可重現的確定性曲線。
    """

    # 內建劇本定義：{ 名稱: [(elapsed_sec, tps_ratio), ...] }
    # tps_ratio 是相對於 max_tps 的比例（0.0 ~ 1.2 以上均可）
    # elapsed_sec 必須嚴格遞增，以 30 秒為基本單位
    BUILTIN_SCENARIOS = {

        # ── double_peak ────────────────────────────────────────────────────────
        # 實驗總時長 900s（EXPERIMENT_DURATION=900）
        # 參數建議：REPLAYER_MIN=60000  REPLAYER_MAX=140000
        #
        "double_peak": [
            # ── 暖機爬坡 ──
            (0,   0.14),   # 起始低速
            (30,  0.50),   # 快速爬坡
            (60,  0.70),   # 第一波峰值開始

            # ── 第一波峰值 ──
            (120, 1.00),   # 維持峰值

            # ── 緩和下降 ──
            (150, 1.00),
            (200, 0.65),   # 進入中間波動段

            # ── 中間段波動（200–500s，0.3–0.6 之間自然起伏）──
            (240, 0.50),
            (280, 0.33),
            (320, 0.52),
            (360, 0.38),
            (400, 0.60),
            (440, 0.31),
            (480, 0.48),
            (500, 0.42),   # 波動結束，準備爬坡

            # ── 第二波爬坡與峰值（500–600s）──
            (530, 0.75),
            (560, 1.10),   # 第二波峰值（比第一波高 10%）
            (600, 1.10),   # 維持峰值至 600s

            # ── 收尾緩降（600–900s）──
            (650, 0.55),
            (720, 0.38),
            (800, 0.28),
            (900, 0.20),
        ],

        # ── triple_peak ────────────────────────────────────────────────────────
        # 實驗總時長 900s（EXPERIMENT_DURATION=900）
        # 第三波明顯較小（~75%），適合驗證 propose 在第一次精準配置後
        # 面對中等流量第三波不需觸發額外遷移
        #
        # 時間軸（以 max_tps=140k 為例）：
        #   0s   : 20k  (暖機)
        #   30s  : 70k  (快速爬坡)
        #   60s  : 140k (第一波)
        #   120s : 140k (維持)
        #   150s : 50k  (緩和期一)
        #   240s : 50k
        #   270s : 126k (第二波爬坡)
        #   300s : 154k (第二波峰值)
        #   390s : 154k (維持)
        #   420s : 56k  (緩和期二)
        #   510s : 56k
        #   540s : 91k  (第三波爬坡，較小)
        #   570s : 105k (第三波峰值，~75%，比前兩波明顯小)
        #   660s : 105k (維持)
        #   690s : 60k  (收尾)
        #   900s : 60k  (實驗結束)
        "triple_peak": [
            (0,   0.14),   # 暖機
            (30,  0.50),   # 快速爬坡
            (60,  1.00),   # 第一波
            (120, 1.00),   # 維持
            (150, 0.36),   # 緩和期一
            (240, 0.36),
            (270, 0.90),   # 第二波爬坡
            (300, 1.10),   # 第二波峰值
            (390, 1.10),   # 維持
            (420, 0.40),   # 緩和期二
            (510, 0.40),
            (540, 0.65),   # 第三波爬坡（較小）
            (570, 0.75),   # 第三波峰值（比前兩波小 ~32%）
            (660, 0.75),   # 維持
            (690, 0.43),   # 收尾
            (900, 0.43),
        ],

        # ── step_stress ────────────────────────────────────────────────────────
        # 逐步階梯加壓，找出系統邊界
        # 實驗總時長建議 >= 800s
        "step_stress": [
            (0,   0.14),
            (60,  0.50),   # 50% 負載
            (180, 0.50),
            (181, 0.75),   # 75% 負載（瞬間跳升）
            (300, 0.75),
            (301, 1.00),   # 100% 負載
            (420, 1.00),
            (421, 1.15),   # 115% 負載（超載）
            (600, 1.15),
            (601, 0.43),   # 冷卻
            (800, 0.43),
        ],
    }

    def __init__(self, max_tps, min_tps, scenario="double_peak",
                 keyframes=None, noise_ratio=0.03):
        """
        max_tps      : TPS 上限（對應 ratio=1.0）
        min_tps      : TPS 下限（ratio=0.0 時使用，不低於此值）
        scenario     : 內建劇本名稱（double_peak / triple_peak / step_stress）
        keyframes    : 自訂 [(sec, ratio), ...]，若提供則忽略 scenario 參數
        noise_ratio  : 隨機抖動幅度（相對於當前 TPS，0.0 = 完全確定性）
        """
        self.max_tps = max_tps
        self.min_tps = min_tps
        self.noise_ratio = noise_ratio

        if keyframes is not None:
            self.keyframes = keyframes
        elif scenario in self.BUILTIN_SCENARIOS:
            self.keyframes = self.BUILTIN_SCENARIOS[scenario]
        else:
            raise ValueError(
                f"未知劇本名稱: '{scenario}'，"
                f"可選: {list(self.BUILTIN_SCENARIOS.keys())}"
            )

        # 列印劇本摘要
        print(f"\n📋 ScenarioModel 劇本摘要（{scenario}）：")
        print(f"   {'時間':>6s}  {'TPS':>8s}  {'比例':>6s}")
        print(f"   {'──────':>6s}  {'────────':>8s}  {'──────':>6s}")
        for sec, ratio in self.keyframes:
            tps_val = int(max_tps * ratio)
            tps_val = max(tps_val, min_tps)
            print(f"   {sec:>5d}s  {tps_val:>8,}  {ratio:>5.0%}")
        print()

    def get_tps(self, elapsed):
        kf = self.keyframes

        # 超出劇本末尾：維持最後一個值
        if elapsed >= kf[-1][0]:
            ratio = kf[-1][1]
        # 劇本開始之前：使用第一個值
        elif elapsed <= kf[0][0]:
            ratio = kf[0][1]
        else:
            # 找到 elapsed 所在的區間，做線性插值
            for i in range(len(kf) - 1):
                t0, r0 = kf[i]
                t1, r1 = kf[i + 1]
                if t0 <= elapsed < t1:
                    frac = (elapsed - t0) / (t1 - t0)
                    ratio = r0 + frac * (r1 - r0)
                    break

        tps = int(self.max_tps * ratio)
        tps = max(tps, self.min_tps)

        # 疊加隨機抖動
        if self.noise_ratio > 0:
            noise = random.gauss(0, tps * self.noise_ratio)
            tps = max(self.min_tps, int(tps + noise))

        return tps


# ──────────────────────────────────────────────────────────────────────────────
# 資料大小 Padding
# ──────────────────────────────────────────────────────────────────────────────

def pad_event(line: bytes, target_bytes: int) -> bytes:
    """
    將一行 JSON 事件的 extra 欄位填充至目標位元組數。
    target_bytes <= 0 表示不做 padding，直接回傳原始資料。
    """
    if target_bytes <= 0 or len(line) >= target_bytes:
        return line
    try:
        obj = json.loads(line)
        current_len = len(line)
        pad_len = target_bytes - current_len - 20
        if pad_len > 0 and "bid" in obj:
            obj["bid"]["extra"] = obj["bid"]["extra"] + ("X" * pad_len)
        result = json.dumps(obj, separators=(',', ':')).encode() + b'\n'
        return result
    except Exception:
        return line


# ──────────────────────────────────────────────────────────────────────────────
# 主程式
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Kafka Replayer — 支援多種 TPS 模式與資料大小控制',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
TPS 模式說明：
  sine       原始正弦波（規律週期，適合作為基準對照）
  random     隨機遊走（無週期性漂移，適合模擬 IoT / 後台流量）
  bursty     基線 + 偶發尖峰（適合模擬電商、直播等突發場景）
  composite  正弦 + 隨機遊走 + 尖峰疊加（最貼近真實生產流量）
  scenario   時間軸劇本（雙峰/三峰/階梯壓力，推薦用於對照實驗）

scenario 劇本選項（--scenario 指定），均以 900s 為設計總時長：
  double_peak  30s 爬坡 → 第一峰(60s) → 緩和 → 第二峰(330s，+10%)
  triple_peak  同樣早峰結構 → 第二峰(300s，+10%) → 第三峰(570s，~75%，較小)
  step_stress  逐步階梯加壓，找出系統邊界（實驗總時長建議 800s）

資料大小範例：
  --size 256    每筆事件填充至約 256 bytes
  --size 1024   每筆事件填充至約 1 KB
  --size 0      不做 padding（使用原始資料大小，預設）

推薦的對照實驗設定（貼到 run_experiment.sh）：
  EXPERIMENT_DURATION=900          # double_peak / triple_peak 均設計為 900s
  LATENCY_THRESHOLD=35000
  REPLAYER_MIN=60000
  REPLAYER_MAX=140000
  REPLAYER_MODE=scenario
  REPLAYER_SIZE=256
  REPLAYER_SCENARIO=double_peak    # 或 triple_peak
        """
    )
    parser.add_argument('--min',  type=int, default=60000,
                        help='最低 TPS（預設 60000）')
    parser.add_argument('--max',  type=int, default=100000,
                        help='最高 TPS（預設 100000）')
    parser.add_argument('--mode', type=str, default='composite',
                        choices=['sine', 'random', 'bursty', 'composite', 'scenario'],
                        help='TPS 變化模式（預設 composite）')
    parser.add_argument('--size', type=int, default=0,
                        help='目標每筆事件大小（bytes），0 表示不 padding（預設 0）')
    parser.add_argument('--scenario', type=str, default='double_peak',
                        choices=['double_peak', 'triple_peak', 'step_stress'],
                        help='scenario 模式下的劇本選擇（預設 double_peak）')
    parser.add_argument('--scenario-noise', type=float, default=0.03,
                        help='scenario 模式的隨機抖動幅度，0.0=確定性（預設 0.03）')
    # bursty / composite 細調參數
    parser.add_argument('--burst-prob',     type=float, default=0.002,
                        help='尖峰觸發機率（每次 TPS 查詢，預設 0.002）')
    parser.add_argument('--burst-duration', type=float, default=5.0,
                        help='尖峰持續秒數（預設 5.0）')

    args = parser.parse_args()

    min_tps = args.min
    max_tps = args.max
    offset    = (max_tps + min_tps) // 2
    amplitude = (max_tps - min_tps) // 2

    # 初始化 TPS 模型
    if args.mode == 'sine':
        model = SineModel(offset, amplitude)
        mode_desc = "正弦波"
    elif args.mode == 'random':
        model = RandomWalkModel(offset, amplitude)
        mode_desc = "隨機遊走"
    elif args.mode == 'bursty':
        model = BurstyModel(offset, amplitude,
                            burst_prob=args.burst_prob,
                            burst_duration=args.burst_duration)
        mode_desc = "基線 + 偶發尖峰"
    elif args.mode == 'scenario':
        model = ScenarioModel(
            max_tps=max_tps,
            min_tps=min_tps,
            scenario=args.scenario,
            noise_ratio=args.scenario_noise,
        )
        mode_desc = f"時間軸劇本（{args.scenario}）"
    else:  # composite
        model = CompositeModel(offset, amplitude,
                               burst_prob=args.burst_prob,
                               burst_duration=args.burst_duration)
        mode_desc = "複合真實模式（正弦 + 隨機遊走 + 尖峰）"

    if not os.path.exists(DATA_FILE_PATH):
        print(f"❌ 找不到資料檔: {DATA_FILE_PATH}")
        return

    print(f"🚀 初始化 Kafka Producer...")
    producer = Producer(conf)
    print(f"🌊 TPS 模式：{mode_desc}")
    print(f"📊 TPS 範圍：{min_tps:,} ~ {max_tps:,}")
    if args.size > 0:
        print(f"📦 每筆資料目標大小：{args.size} bytes")
    else:
        print(f"📦 資料大小：原始（不 padding）")
    print()

    start_time    = time.time()
    total_sent    = 0
    BATCH_SIZE    = 5000
    batch_count   = 0
    batch_start_time = time.time()
    sleep_time    = 0.0

    try:
        with open(DATA_FILE_PATH, 'rb') as f:
            while True:
                line = f.readline()
                if not line:
                    f.seek(0)
                    continue

                # 資料大小 padding
                if args.size > 0:
                    line = pad_event(line, args.size)

                try:
                    producer.produce(TOPIC_NAME, line, on_delivery=None)
                except BufferError:
                    producer.poll(0.1)
                    producer.produce(TOPIC_NAME, line, on_delivery=None)

                batch_count += 1
                total_sent  += 1

                if batch_count >= BATCH_SIZE:
                    producer.poll(0)

                    now           = time.time()
                    elapsed_total = now - start_time
                    current_tps   = model.get_tps(elapsed_total)

                    expected_duration = batch_count / max(current_tps, 1)
                    actual_duration   = now - batch_start_time

                    sleep_time = 0.0
                    if actual_duration < expected_duration:
                        sleep_time = expected_duration - actual_duration
                        time.sleep(sleep_time)

                    batch_count      = 0
                    batch_start_time = time.time()

                    if int(elapsed_total) % 3 == 0 and int(elapsed_total * 10) % 10 < 2:
                        real_tps = BATCH_SIZE / max(actual_duration + sleep_time, 1e-9)
                        sys.stdout.write(
                            f"\r⏱️  {int(elapsed_total):4d}s | "
                            f"Mode: {args.mode:<9s} | "
                            f"Target: {current_tps//1000:3d}k | "
                            f"Real: {int(real_tps)//1000:3d}k | "
                            f"Total: {total_sent:,}"
                        )
                        sys.stdout.flush()

    except KeyboardInterrupt:
        print("\n🛑 停止重放。")
    finally:
        print("\n正在將剩餘資料寫入 Kafka...")
        producer.flush()


if __name__ == "__main__":
    main()