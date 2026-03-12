#!/usr/bin/env python3
"""
測試 Flink REST API 來獲取 Checkpoint 狀態大小
"""

import requests
import json
from propose import format_bytes

FLINK_REST_URL = "http://localhost:8081"

def test_checkpoint_api():
    """測試 Checkpoint API 的完整流程"""

    print("=" * 60)
    print("測試 Flink Checkpoint API")
    print("=" * 60)

    # Step 1: 獲取正在運行的 Job
    print("\n📋 Step 1: 獲取正在運行的 Job")
    try:
        response = requests.get(f"{FLINK_REST_URL}/jobs")
        jobs_data = response.json()

        running_jobs = []
        for job in jobs_data.get('jobs', []):
            if job['status'] == 'RUNNING':
                running_jobs.append(job['id'])
                print(f"   ✅ 找到運行中的 Job: {job['id']}")

        if not running_jobs:
            print("   ❌ 沒有正在運行的 Job")
            return

        job_id = running_jobs[0]

    except Exception as e:
        print(f"   ❌ 錯誤: {e}")
        return

    # Step 2: 獲取 Job 詳情和 Vertex 列表
    print(f"\n📋 Step 2: 獲取 Job {job_id} 的 Vertex 列表")
    try:
        response = requests.get(f"{FLINK_REST_URL}/jobs/{job_id}")
        job_data = response.json()

        print(f"\n   Job Name: {job_data.get('name', 'Unknown')}")
        print(f"   Vertices:")

        vertices = job_data.get('vertices', [])
        for vertex in vertices:
            vertex_id = vertex.get('id')
            vertex_name = vertex.get('name')
            parallelism = vertex.get('parallelism')
            print(f"      - ID: {vertex_id}")
            print(f"        Name: {vertex_name}")
            print(f"        Parallelism: {parallelism}")

    except Exception as e:
        print(f"   ❌ 錯誤: {e}")
        return

    # Step 3: 獲取最新的 Checkpoint ID
    print(f"\n📋 Step 3: 獲取最新的 Checkpoint")
    try:
        response = requests.get(f"{FLINK_REST_URL}/jobs/{job_id}/checkpoints")
        checkpoint_data = response.json()

        # 顯示 checkpoint 統計
        if 'latest' in checkpoint_data:
            latest = checkpoint_data['latest']

            if 'completed' in latest and latest['completed']:
                completed = latest['completed']
                checkpoint_id = completed.get('id')
                print(f"   ✅ 最新完成的 Checkpoint:")
                print(f"      ID: {checkpoint_id}")
                print(f"      Trigger Time: {completed.get('trigger_timestamp', 'N/A')}")
                print(f"      Duration: {completed.get('end_to_end_duration', 'N/A')} ms")

                # 嘗試獲取狀態大小
                if 'state_size' in completed:
                    print(f"      State Size: {format_bytes(completed.get('state_size', 0))}")

            else:
                print("   ⚠️ 沒有完成的 Checkpoint")
                checkpoint_id = None

        if not checkpoint_id:
            print("   ❌ 無法獲取 Checkpoint ID")
            return

    except Exception as e:
        print(f"   ❌ 錯誤: {e}")
        return

    # Step 4: 對每個 Vertex 獲取 Subtask 的詳細信息
    print(f"\n📋 Step 4: 獲取每個 Vertex 的 Subtask 狀態大小")
    for vertex in vertices:
        vertex_id = vertex.get('id')
        vertex_name = vertex.get('name')

        print(f"\n   🔍 {vertex_name}")
        print(f"      Vertex ID: {vertex_id}")

        try:
            url = f"{FLINK_REST_URL}/jobs/{job_id}/checkpoints/details/{checkpoint_id}/subtasks/{vertex_id}"
            response = requests.get(url)

            if response.status_code != 200:
                print(f"      ⚠️ 無法獲取詳情 (HTTP {response.status_code})")
                continue

            subtask_data = response.json()

            # 顯示每個 subtask 的信息
            if 'subtasks' in subtask_data:
                subtasks = subtask_data['subtasks']
                print(f"      Subtasks: {len(subtasks)}")

                total_state_size = 0

                for subtask in subtasks:
                    # 使用 'index' 欄位，不是 'subtask'
                    subtask_index = subtask.get('index', -1)

                    # 狀態大小在 subtask 的頂層，不是在 checkpoint 內
                    state_size = subtask.get('state_size', 0)

                    # 如果沒有 state_size，嘗試 checkpointed_size
                    if state_size == 0:
                        state_size = subtask.get('checkpointed_size', 0)

                    total_state_size += state_size

                    if state_size > 0:
                        print(f"         Subtask {subtask_index}: {format_bytes(state_size)}")

                if total_state_size > 0:
                    print(f"      ✅ 總狀態大小: {format_bytes(total_state_size)}")
                else:
                    print(f"      ⚠️ 無狀態或無法獲取狀態大小")

                # 顯示原始 JSON（用於調試）
                if len(subtasks) > 0:
                    print(f"\n      📄 第一個 Subtask 的原始數據樣例:")
                    print(json.dumps(subtasks[0], indent=8))

        except Exception as e:
            print(f"      ❌ 錯誤: {e}")

    print("\n" + "=" * 60)
    print("測試完成")
    print("=" * 60)


if __name__ == "__main__":
    test_checkpoint_api()
