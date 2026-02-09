#!/bin/bash

echo "============================================"
echo "快速验证 ResourceID 配置"
echo "============================================"
echo ""

echo "步骤 1: 重新构建和启动"
echo "--------------------------------------------"
read -p "是否需要重新构建 Docker 镜像? (y/n): " rebuild
if [ "$rebuild" = "y" ]; then
    echo "重新构建..."
    docker compose build
fi

echo "停止现有容器..."
docker compose down

echo "启动服务..."
docker compose up -d

echo "等待服务启动 (30秒)..."
sleep 30

echo ""
echo "步骤 2: 检查 TaskManager 日志"
echo "--------------------------------------------"
for tm in tm-20c-1 tm-20c-2-net tm-10c-3-cpu tm-20c-4; do
    echo "检查 $tm:"
    docker logs $tm 2>&1 | grep -i "resource-id" | head -1
done

echo ""
echo "步骤 3: 检查 Prometheus metrics 中的 tm_id"
echo "--------------------------------------------"
sleep 10  # 再等待一下让 metrics 生成

curl -s 'http://10.28.248.84:9090/api/v1/query?query=flink_taskmanager_Status_JVM_Memory_Heap_Used' 2>/dev/null | \
  python3 << 'EOF'
import sys, json
try:
    data = json.load(sys.stdin)
    if data['status'] == 'success' and data['data']['result']:
        tm_ids = set()
        for result in data['data']['result']:
            tm_id = result['metric'].get('tm_id', 'N/A')
            tm_ids.add(tm_id)

        print("找到的 tm_id:")
        for tm_id in sorted(tm_ids):
            if tm_id.startswith('tm-'):
                print(f"  ✅ {tm_id} (ResourceID format)")
            elif '_' in tm_id and ':' in tm_id:
                print(f"  ❌ {tm_id} (IP format - ResourceID 未生效)")
            else:
                print(f"  ⚠️  {tm_id} (未知格式)")

        if all(tm_id.startswith('tm-') for tm_id in tm_ids if tm_id != 'N/A'):
            print("\n✅ ResourceID 配置成功！所有 TaskManager 都使用了自定义的 resource-id")
        else:
            print("\n❌ ResourceID 配置失败！某些 TaskManager 仍使用 IP 格式的 tm_id")
            print("\n请检查:")
            print("  1. start-taskmanager-with-metrics.sh 是否正确")
            print("  2. TM_RESOURCE_ID 环境变量是否设置")
            print("  3. Docker 镜像是否重新构建")
    else:
        print("❌ 无法获取 metrics 数据")
        print("请确保:")
        print("  1. Flink 集群正在运行")
        print("  2. Prometheus 正在抓取数据")
        print("  3. 至少有一个 Job 在运行")
except Exception as e:
    print(f"❌ 错误: {e}")
    print("请检查 Prometheus 是否正常运行: http://localhost:9090")
EOF

echo ""
echo "步骤 4: 测试 detector.py"
echo "--------------------------------------------"
python3 << 'EOF'
import sys
sys.path.insert(0, '/home/yenwei/research/structure_setup/caom_core')

try:
    from detector import FlinkDetector

    detector = FlinkDetector()
    tm_info = detector.get_taskmanager_info()

    if tm_info:
        print("✅ detector.py 成功获取 TaskManager 信息")
        print(f"找到 {len(tm_info)} 个 TaskManager:")
        for resource_id, info in list(tm_info.items())[:5]:
            if resource_id.startswith('tm-'):
                print(f"  ✅ {resource_id} (host: {info['host']}, load: {info['load']:.1f})")
            else:
                print(f"  ❌ {resource_id} (仍然是 IP 格式)")

        # 检查是否所有都是 resource-id 格式
        all_resource_id = all(rid.startswith('tm-') for rid in tm_info.keys())
        if all_resource_id:
            print("\n✅ 所有 TaskManager 都使用 ResourceID！")
        else:
            print("\n❌ 某些 TaskManager 仍使用 IP 格式的 tm_id")
    else:
        print("⚠️  detector.py 未能获取 TaskManager 信息")
        print("这是正常的，如果还没有 Job 运行的话")

except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
EOF

echo ""
echo "============================================"
echo "验证完成"
echo "============================================"
echo ""
echo "下一步:"
echo "  1. 如果 ResourceID 配置成功 → 提交 Flink Job 并生成 migration plan"
echo "  2. 如果 ResourceID 配置失败 → 检查上述输出的错误信息"
echo ""
echo "提交 Job 后，可以运行以下命令测试 migration plan:"
echo "  python3 /home/yenwei/research/structure_setup/caom_core/test_migration.py"
echo ""
