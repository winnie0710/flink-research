#!/bin/bash

echo "========================================="
echo "Resource-ID 驗證腳本"
echo "========================================="
echo ""

# 等待服務啟動
sleep 5

echo "1. 檢查 Prometheus 是否有 resource_id 標籤"
echo "-----------------------------------------"
curl -s 'http://localhost:9090/api/v1/label/resource_id/values' | python3 -c "import sys, json; data=json.load(sys.stdin); print('✅ resource_id 標籤存在' if data['status']=='success' and len(data['data'])>0 else '❌ resource_id 標籤不存在'); print('可用的 resource_id:', ', '.join(data['data']) if data['status']=='success' else 'None')"
echo ""

echo "2. 查詢帶有 resource_id 的 metrics"
echo "-----------------------------------------"
curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond' | python3 -c "
import sys, json
data = json.load(sys.stdin)
if data['status'] == 'success' and len(data['data']['result']) > 0:
    print('✅ 找到 metrics')
    for result in data['data']['result'][:3]:
        metric = result['metric']
        resource_id = metric.get('resource_id', 'N/A')
        tm_id = metric.get('tm_id', 'N/A')
        task_name = metric.get('task_name', 'N/A')
        print(f'  - task: {task_name[:50]}, resource_id: {resource_id}, tm_id: {tm_id[:30]}...')
else:
    print('❌ 未找到 metrics')
"
echo ""

echo "3. 測試 detector.py 的 get_taskmanager_info()"
echo "-----------------------------------------"
python3 << 'PYTHON_SCRIPT'
import sys
sys.path.insert(0, '/home/yenwei/research/structure_setup/caom_core')
from detector import FlinkDetector

detector = FlinkDetector()
tm_info = detector.get_taskmanager_info()

if tm_info:
    print('✅ detector.py 成功獲取 TaskManager 資訊')
    for resource_id, info in list(tm_info.items())[:5]:
        print(f'  - resource_id: {resource_id}, host: {info["host"]}, load: {info["load"]:.1f}')
else:
    print('❌ detector.py 未能獲取 TaskManager 資訊')
PYTHON_SCRIPT

echo ""
echo "4. 測試 detector.py 的 get_subtask_locations()"
echo "-----------------------------------------"
python3 << 'PYTHON_SCRIPT'
import sys
sys.path.insert(0, '/home/yenwei/research/structure_setup/caom_core')
from detector import FlinkDetector

detector = FlinkDetector()
locations = detector.get_subtask_locations()

if locations:
    print('✅ detector.py 成功獲取 Subtask 位置')
    for subtask_id, resource_id in list(locations.items())[:5]:
        print(f'  - {subtask_id[:60]}: {resource_id}')
else:
    print('❌ detector.py 未能獲取 Subtask 位置')
PYTHON_SCRIPT

echo ""
echo "========================================="
echo "驗證完成"
echo "========================================="
