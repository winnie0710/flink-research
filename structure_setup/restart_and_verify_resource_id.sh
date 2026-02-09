#!/bin/bash

echo "============================================"
echo "重启并验证 ResourceID 配置"
echo "============================================"
echo ""

echo "步骤 1: 停止现有容器"
echo "--------------------------------------------"
docker compose down

echo ""
echo "步骤 2: 启动服务"
echo "--------------------------------------------"
docker compose up -d

echo ""
echo "步骤 3: 等待服务启动 (30秒)"
echo "--------------------------------------------"
for i in {30..1}; do
    echo -ne "剩余 $i 秒...\r"
    sleep 1
done
echo ""

echo ""
echo "步骤 4: 检查 TaskManager 日志中的 ResourceID"
echo "--------------------------------------------"
for tm in tm-20c-1 tm-20c-2-net tm-10c-3-cpu tm-20c-4; do
    echo "检查 $tm:"
    docker logs $tm 2>&1 | grep "Starting TaskManager with ResourceID" | tail -1
done

echo ""
echo "步骤 5: 等待 Prometheus 抓取数据 (20秒)"
echo "--------------------------------------------"
for i in {20..1}; do
    echo -ne "剩余 $i 秒...\r"
    sleep 1
done
echo ""

echo ""
echo "步骤 6: 检查 Prometheus metrics 中的 tm_id"
echo "--------------------------------------------"
curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_Status_JVM_Memory_Heap_Used' 2>/dev/null | \
  python3 << 'EOF'
import sys, json
try:
    data = json.load(sys.stdin)
    if data['status'] == 'success' and data['data']['result']:
        tm_ids = {}
        for result in data['data']['result']:
            tm_id = result['metric'].get('tm_id', 'N/A')
            host = result['metric'].get('host', 'N/A')
            if tm_id not in tm_ids:
                tm_ids[tm_id] = host

        print("\n找到的 TaskManager (tm_id -> host):")
        success_count = 0
        for tm_id, host in sorted(tm_ids.items()):
            if tm_id.startswith('tm-'):
                print(f"  ✅ {tm_id:20s} (host: {host})")
                success_count += 1
            elif '_' in tm_id and ':' in tm_id:
                print(f"  ❌ {tm_id:20s} (host: {host}) <- 仍然是 IP 格式")
            else:
                print(f"  ⚠️  {tm_id:20s} (host: {host})")

        print(f"\n结果: {success_count}/{len(tm_ids)} 个 TaskManager 使用了 ResourceID")

        if success_count == len(tm_ids) and success_count > 0:
            print("\n🎉 成功！所有 TaskManager 都使用了自定义的 resource-id")
            print("\n下一步:")
            print("  1. 提交 Flink Job")
            print("  2. 等待 Job 运行产生 metrics")
            print("  3. 运行 detector.py 生成 migration plan")
        elif success_count > 0:
            print(f"\n⚠️  部分成功：{success_count} 个 TM 使用 ResourceID，{len(tm_ids)-success_count} 个仍使用 IP 格式")
        else:
            print("\n❌ 失败：所有 TaskManager 仍使用 IP 格式的 tm_id")
            print("\n请检查:")
            print("  1. 配置文件是否正确 (config/tm-*-flink-conf.yaml)")
            print("  2. docker-compose.yml 中的 volumes 挂载")
            print("  3. TaskManager 日志中的错误信息")
    else:
        print("❌ 无法获取 metrics 数据")
        print("\n可能的原因:")
        print("  1. Prometheus 还在启动中，请等待片刻后重试")
        print("  2. TaskManager 还没有完全启动")
        print("  3. 还没有 Job 运行")
        print("\n请手动检查:")
        print("  curl http://localhost:9090/api/v1/query?query=up")
except Exception as e:
    print(f"❌ 错误: {e}")
    print("\n请检查:")
    print("  1. Prometheus 是否正常运行: docker ps | grep prometheus")
    print("  2. Prometheus UI: http://localhost:9090")
EOF

echo ""
echo "============================================"
echo "验证完成"
echo "============================================"
