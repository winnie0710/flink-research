import json
import time
import numpy as np
from kafka import KafkaConsumer
from collections import defaultdict, deque
from typing import Dict, Any, Deque, Tuple, Union, List

# 配置常量
KAFKA_SERVER: str = '127.0.0.1:9092'
METRICS_TOPIC: str = 'flink-caom-metrics'
MONITOR_INTERVAL_MS: int = 5000  # Flink 報告間隔，5秒
HISTORY_WINDOW_SIZE: int = 5     # 儲存最近 5 個監控間隔的數據 (25秒)
BOTTLENECK_THRESHOLD: float = 1.05 # 實際數據輸入速率超過最大處理能力 5% 則視為瓶頸

# 儲存運行時指標的結構
# 結構: {instance_id: {metric_name: deque of values}}
InstanceMetrics = defaultdict[str, Dict[str, Deque[float]]]

class MetricProcessor:
    """
    處理和分析從 Kafka 接收到的 Flink Metrics。
    """
    def __init__(self):
        self.last_ts: float = time.time()
        # 使用 Type Hint 確保結構清晰
        self.instance_metrics: InstanceMetrics = defaultdict(lambda: defaultdict(lambda: deque(maxlen=HISTORY_WINDOW_SIZE)))

    def update_instance_metrics(self, data: Dict[str, Any]) -> None:
        """從 JSON 數據中提取關鍵指標並存儲。"""
        # 注意: Flink Kafka Reporter 輸出的 JSON 結構複雜，這裡使用常見的鍵名。

        job_name = data.get('jobName')
        task_name = data.get('task_name', data.get('operator_name', 'Unknown'))
        subtask_index = str(data.get('subtask_index'))

        if not job_name or task_name == 'Unknown':
            return

        # 組合唯一的實例ID: [TaskName]-[SubtaskIndex] (例如: WindowMax-0)
        instance_id = f"{task_name}-{subtask_index}"

        # 提取 Metrics:
        # Flink 1.x Metrics 通常以百分比或每毫秒時間計數
        busy_rate = data.get('busyTimeMsPerSecond', data.get('busy_time_pct', 0.0))
        backpressure_rate = data.get('backPressuredTimeMsPerSecond', data.get('backpressure_pct', 0.0))
        idle_rate = data.get('idleTimeMsPerSecond', data.get('idle_pct', 0.0))

        # 數據輸入/輸出速率 (records per second)
        input_rate = data.get('numRecordsInPerSecond', data.get('num_records_in_per_second', 0.0))
        # output_rate = data.get('numRecordsOutPerSecond', data.get('num_records_out_per_second', 0.0))

        # 儲存到 Deque
        try:
            self.instance_metrics[instance_id]['busy_rate'].append(float(busy_rate))
            self.instance_metrics[instance_id]['bp_rate'].append(float(backpressure_rate))
            self.instance_metrics[instance_id]['idle_rate'].append(float(idle_rate))
            self.instance_metrics[instance_id]['input_rate'].append(float(input_rate))

        except (ValueError, TypeError) as e:
            # 忽略無效的數值
            print(f"警告: 無效的 Metric 數值 for {instance_id}. 錯誤: {e}")


    def calculate_capacity(self, instance_id: str) -> Tuple[float, float]:

        #計算實例的實際數據輸入速率 (\hat{\lambda}_{i,j}) 和最大處理能力 (\lambda_{i,j}^a)。
        #基於論文公式 (10) 和 (12)。

        metrics = self.instance_metrics[instance_id]

        # 確保有足夠的歷史數據進行平滑
        if len(metrics['input_rate']) < HISTORY_WINDOW_SIZE:
            return 0.0, 0.0

        # 使用最近 N 個數據的平均值來平滑計算
        try:
            avg_busy = np.mean(metrics['busy_rate'])
            avg_bp = np.mean(metrics['bp_rate'])
            avg_input = np.mean(metrics['input_rate'])

            if avg_busy < 0.001:  # 忙碌率趨近於零，無法計算比率
                return avg_input, float('inf')

                # 1. 實際輸入速率 (\hat{\lambda}_{i,j}) - 論文公式 (11) 核心
            # 這是當背壓消除後，實例能夠處理的數據量
            # 實際速率 = 觀測速率 * (總處理時間 / 實際忙碌時間)
            actual_input_rate = avg_input * (avg_busy + avg_bp) / avg_busy

            # 2. 最大處理能力 (\lambda_{i,j}^a) - 論文公式 (13) 核心
            # 這是當忙碌率為 100% 時能達到的理論速率
            max_processing_capacity = avg_input / avg_busy

            return actual_input_rate, max_processing_capacity

        except Exception:
            # 捕捉所有計算錯誤，返回 0 避免程式崩潰
            return 0.0, 0.0

    def detect_bottlenecks(self) -> Dict[str, Any]:

        # 檢測所有瓶頸操作符。

        bottlenecks: Dict[str, Any] = {}

        # 步驟 1: 計算每個實例的實際速率和最大容量
        instance_capacities: Dict[str, Dict[str, float]] = {}
        for instance_id in self.instance_metrics:
            actual_rate, max_capacity = self.calculate_capacity(instance_id)
            if max_capacity > 0:
                instance_capacities[instance_id] = {'actual_rate': actual_rate, 'max_capacity': max_capacity}

        if not instance_capacities:
            return {}

        # 步驟 2: 進行瓶頸檢測 (\hat{\lambda} > \lambda^a * Threshold)
        for instance_id, caps in instance_capacities.items():
            actual_rate = caps['actual_rate']
            max_capacity = caps['max_capacity']

            # 檢查瓶頸條件: 實際輸入速率超過最大處理能力 5%
            if actual_rate > max_capacity * BOTTLENECK_THRESHOLD:
                # 提取 Operator 名稱 (例如: WindowMax-0 -> WindowMax)
                op_name = instance_id.split('-')[0]

                if op_name not in bottlenecks:
                    bottlenecks[op_name] = {'instances': []}

                bottlenecks[op_name]['instances'].append({
                    'id': instance_id,
                    'actual_rate': actual_rate,
                    'max_capacity': max_capacity
                })

        return bottlenecks

def monitor_metrics():
    #主監控循環，連接 Kafka Consumer。
    print("--- 啟動 CAOM 瓶頸偵測器 ---")
    print(f"連線至 Kafka: {KAFKA_SERVER}, Topic: {METRICS_TOPIC}")

    try:
        consumer = KafkaConsumer(
            METRICS_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"錯誤: 無法連線到 Kafka: {e}. 請確認 Kafka 服務是否啟動。")
        return

    processor = MetricProcessor()

    print("開始接收 Metrics 數據...")

    for message in consumer:
        # Flink Kafka Reporter 可能一次性報告多個 Metrics
        data: Union[Dict[str, Any], List[Dict[str, Any]]] = message.value

        if isinstance(data, list):
            for item in data:
                processor.update_instance_metrics(item)
        elif isinstance(data, dict):
            processor.update_instance_metrics(data)

        current_time: float = time.time()

        # 每隔一個監控間隔進行一次瓶頸分析
        if current_time - processor.last_ts > MONITOR_INTERVAL_MS / 1000:

            bottlenecks = processor.detect_bottlenecks()

            if bottlenecks:
                print(f"\n[{time.strftime('%H:%M:%S')}] 🚨 偵測到瓶頸操作符 (Bottleneck Operators):")
                for op_name, info in bottlenecks.items():
                    print(f"  > 操作符: {op_name} ({len(info['instances'])} 個實例)")
                    for inst in info['instances']:
                        print(f"    - 實例 {inst['id']} 狀況:")
                        print(f"      實際速率 (λ_hat): {inst['actual_rate']:.2f} r/s")
                        print(f"      最大容量 (λ_max): {inst['max_capacity']:.2f} r/s")
                        print(f"      超載比率: {inst['actual_rate'] / inst['max_capacity']:.2f}x")

                # TODO: 接下來將呼叫遷移規劃函數 (OptimizeMigrationTime())

            else:
                print(f"[{time.strftime('%H:%M:%S')}] 狀態正常。")

            processor.last_ts = current_time

if __name__ == "__main__":
    monitor_metrics()