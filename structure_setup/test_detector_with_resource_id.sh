#!/bin/bash

echo "============================================"
echo "测试 detector.py 与 ResourceID"
echo "============================================"
echo ""

echo "步骤 1: 验证 Prometheus 连接"
echo "--------------------------------------------"
if ! curl -s http://localhost:9090/-/healthy >/dev/null 2>&1; then
    echo "❌ 无法连接到 Prometheus"
    echo "请确保 Prometheus 正在运行: docker ps | grep prometheus"
    exit 1
fi
echo "✅ Prometheus 正常运行"

echo ""
echo "步骤 2: 检查是否有 Flink metrics 数据"
echo "--------------------------------------------"
result=$(curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond' | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('data', {}).get('result', [])))")

if [ "$result" -eq 0 ]; then
    echo "⚠️  还没有 Job metrics 数据"
    echo ""
    echo "需要先提交一个 Flink Job。运行以下命令:"
    echo "  docker exec -it jobmanager bash"
    echo "  cd /opt/nexmark"
    echo "  flink run -d nexmark-flink-0.3-SNAPSHOT.jar ..."
    echo ""
    echo "或者等待已有的 Job 产生数据"
    exit 1
else
    echo "✅ 找到 $result 条 metrics 数据"
fi

echo ""
echo "步骤 3: 查看 tm_id 标签格式"
echo "--------------------------------------------"
curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_job_task_busyTimeMsPerSecond' | python3 << 'EOF'
import sys, json
data = json.load(sys.stdin)
if data['status'] == 'success' and data['data']['result']:
    tm_ids = set()
    for r in data['data']['result'][:5]:
        tm_id = r['metric'].get('tm_id', 'N/A')
        tm_ids.add(tm_id)

    print("tm_id 样本:")
    for tm_id in sorted(tm_ids):
        if tm_id.startswith('tm-'):
            print(f"  ✅ {tm_id} (ResourceID 格式)")
        else:
            print(f"  ❌ {tm_id} (非 ResourceID 格式)")
EOF

echo ""
echo "步骤 4: 测试 detector.py 的 get_taskmanager_info()"
echo "--------------------------------------------"
python3 << 'PYTHON'
import sys
sys.path.insert(0, '/home/yenwei/research/structure_setup/caom_core')

try:
    from detector import FlinkDetector

    detector = FlinkDetector()
    tm_info = detector.get_taskmanager_info()

    if tm_info:
        print(f"✅ 成功获取 {len(tm_info)} 个 TaskManager 信息\n")

        all_resource_id = True
        for resource_id, info in tm_info.items():
            if resource_id.startswith('tm-'):
                print(f"  ✅ {resource_id:20s} host: {info['host']:15s} load: {info['load']:6.1f}")
            else:
                print(f"  ❌ {resource_id:20s} host: {info['host']:15s} load: {info['load']:6.1f} (非 ResourceID)")
                all_resource_id = False

        if all_resource_id:
            print("\n🎉 所有 TaskManager 都使用 ResourceID 格式！")
        else:
            print("\n⚠️  部分 TaskManager 未使用 ResourceID 格式")
    else:
        print("❌ 未能获取 TaskManager 信息")
        sys.exit(1)

except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON

if [ $? -ne 0 ]; then
    echo ""
    echo "detector.py 测试失败"
    exit 1
fi

echo ""
echo "步骤 5: 测试 detector.py 的 get_subtask_locations()"
echo "--------------------------------------------"
python3 << 'PYTHON'
import sys
sys.path.insert(0, '/home/yenwei/research/structure_setup/caom_core')

try:
    from detector import FlinkDetector

    detector = FlinkDetector()
    locations = detector.get_subtask_locations()

    if locations:
        print(f"✅ 成功获取 {len(locations)} 个 Subtask 位置\n")

        sample_count = min(5, len(locations))
        print(f"显示前 {sample_count} 个:")

        all_resource_id = True
        for subtask_id, resource_id in list(locations.items())[:sample_count]:
            short_name = subtask_id[:50] + "..." if len(subtask_id) > 50 else subtask_id
            if resource_id.startswith('tm-'):
                print(f"  ✅ {short_name:55s} -> {resource_id}")
            else:
                print(f"  ❌ {short_name:55s} -> {resource_id} (非 ResourceID)")
                all_resource_id = False

        if all_resource_id:
            print("\n🎉 所有 Subtask 位置都使用 ResourceID 格式！")
        else:
            print("\n⚠️  部分 Subtask 位置未使用 ResourceID 格式")
    else:
        print("❌ 未能获取 Subtask 位置")
        sys.exit(1)

except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON

if [ $? -ne 0 ]; then
    echo ""
    echo "detector.py 测试失败"
    exit 1
fi

echo ""
echo "步骤 6: 测试生成 migration plan (模拟)"
echo "--------------------------------------------"
python3 << 'PYTHON'
import sys
sys.path.insert(0, '/home/yenwei/research/structure_setup/caom_core')

try:
    from detector import FlinkDetector

    detector = FlinkDetector()

    # 检测瓶颈
    reports = detector.detect_bottleneck()

    if not reports:
        print("⚠️  还没有足够的 metrics 数据来检测瓶颈")
        print("这是正常的，Job 刚启动时数据较少")
        sys.exit(0)

    print(f"✅ 检测到 {len(reports)} 个 Task\n")

    # 模拟找出过载的 subtask（降低阈值用于测试）
    overloaded_subtasks = []
    for report in reports:
        if report['max_busy'] > 100:  # 降低阈值
            task_name = report['task_name']
            for idx, busy in enumerate(report['details']):
                if busy > 100:
                    subtask_id = f"{task_name}_{idx}"
                    overloaded_subtasks.append((subtask_id, busy))

    if not overloaded_subtasks:
        print("⚠️  未检测到过载的 Subtask")
        print("可以尝试降低阈值或等待更多数据")
        sys.exit(0)

    print(f"模拟检测到 {len(overloaded_subtasks)} 个过载的 Subtask")

    # 生成 migration plan
    migration_plan = detector.generate_migration_plan(overloaded_subtasks[:3])  # 只取前3个

    if migration_plan:
        print(f"\n✅ 成功生成 migration plan，包含 {len(migration_plan)} 个迁移项目\n")

        all_resource_id = True
        for subtask_id, target_resource_id in migration_plan.items():
            short_name = subtask_id[:50] + "..." if len(subtask_id) > 50 else subtask_id
            if target_resource_id.startswith('tm-'):
                print(f"  ✅ {short_name:55s} -> {target_resource_id}")
            else:
                print(f"  ❌ {short_name:55s} -> {target_resource_id} (非 ResourceID)")
                all_resource_id = False

        if all_resource_id:
            print("\n🎉 Migration plan 使用 ResourceID 格式！")
            print("\n可以写入文件:")
            print("  detector.write_migration_plan(migration_plan)")
        else:
            print("\n⚠️  Migration plan 未使用 ResourceID 格式")
    else:
        print("❌ 无法生成 migration plan")
        sys.exit(1)

except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON

echo ""
echo "============================================"
echo "测试完成"
echo "============================================"
echo ""
echo "✅ 如果所有步骤都通过，说明:"
echo "  1. ResourceID 配置成功"
echo "  2. Prometheus 正确抓取了 tm_id=resource_id"
echo "  3. detector.py 能够正确读取和使用 ResourceID"
echo "  4. Migration plan 会使用 ResourceID 格式"
echo ""
echo "下一步:"
echo "  1. 运行真实的 detector.py 脚本生成 migration plan"
echo "  2. 实现 Flink ResourceManager 层级的 ResourceID 匹配"
echo "  3. 测试完整的迁移流程"
echo ""
