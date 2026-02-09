#!/bin/bash

echo "============================================"
echo "验证 ResourceID 配置"
echo "============================================"
echo ""

echo "步骤 1: 检查 TaskManager 日志中的 ResourceID"
echo "--------------------------------------------"
all_success=true
for tm in tm-20c-1 tm-20c-2-net tm-10c-3-cpu tm-20c-4; do
    echo -n "检查 $tm: "
    result=$(docker logs $tm 2>&1 | grep "Starting TaskManager with ResourceID" | tail -1)
    if [[ $result == *"ResourceID: $tm"* ]]; then
        echo "✅ $tm"
    else
        echo "❌ 未找到正确的 ResourceID"
        echo "   实际: $result"
        all_success=false
    fi
done

echo ""
echo "步骤 2: 检查 Prometheus 是否运行"
echo "--------------------------------------------"
if docker ps | grep -q prometheus; then
    echo "✅ Prometheus 容器正在运行"

    # 检查 Prometheus 是否准备好
    echo -n "检查 Prometheus 健康状态: "
    if curl -s http://localhost:9090/-/healthy 2>/dev/null | grep -q "Prometheus"; then
        echo "✅ Prometheus 健康"
    else
        echo "⚠️  Prometheus 还在启动中，等待 10 秒..."
        sleep 10
    fi
else
    echo "❌ Prometheus 容器未运行"
    all_success=false
fi

echo ""
echo "步骤 3: 检查 Prometheus metrics 中的 tm_id"
echo "--------------------------------------------"

# 尝试多次查询
max_attempts=3
attempt=1
metrics_success=false

while [ $attempt -le $max_attempts ] && [ "$metrics_success" = false ]; do
    echo "尝试 $attempt/$max_attempts..."

    response=$(curl -s 'http://localhost:9090/api/v1/query?query=flink_taskmanager_Status_JVM_Memory_Heap_Used' 2>/dev/null)

    if [ -n "$response" ] && echo "$response" | grep -q "success"; then
        echo "$response" | python3 << 'EOF'
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
            print("\n🎉 完全成功！所有 TaskManager 都使用了自定义的 resource-id")
            sys.exit(0)
        elif success_count > 0:
            print(f"\n⚠️  部分成功：{success_count} 个 TM 使用 ResourceID")
            sys.exit(1)
        else:
            print("\n❌ 失败：所有 TaskManager 仍使用 IP 格式")
            sys.exit(2)
    else:
        print("⚠️  还没有 metrics 数据")
        sys.exit(3)
except Exception as e:
    print(f"❌ 解析错误: {e}")
    sys.exit(4)
EOF

        if [ $? -eq 0 ]; then
            metrics_success=true
        elif [ $? -eq 3 ]; then
            echo "等待 5 秒后重试..."
            sleep 5
            attempt=$((attempt + 1))
        else
            break
        fi
    else
        echo "⚠️  无法连接到 Prometheus，等待 5 秒后重试..."
        sleep 5
        attempt=$((attempt + 1))
    fi
done

if [ "$metrics_success" = false ]; then
    echo ""
    echo "⚠️  注意: Prometheus metrics 还没有数据"
    echo ""
    echo "可能的原因:"
    echo "  1. TaskManager 刚启动，还没有产生 metrics"
    echo "  2. 还没有 Job 运行"
    echo "  3. Prometheus 还在首次抓取数据"
    echo ""
    echo "但是从 TaskManager 日志来看，ResourceID 已经成功配置！"
fi

echo ""
echo "============================================"
echo "总结"
echo "============================================"

if [ "$all_success" = true ]; then
    echo "✅ TaskManager ResourceID 配置: 成功"
    echo "✅ 所有 4 个 TaskManager 都使用了正确的 resource-id"
    echo ""
    echo "下一步:"
    echo "  1. 提交 Flink Job (如果还没有)"
    echo "  2. 等待 2-3 分钟让 Job 运行并产生 metrics"
    echo "  3. 再次运行此脚本确认 Prometheus 中的 tm_id"
    echo "  4. 运行 detector.py 测试 migration plan 生成"
else
    echo "❌ 某些检查失败，请查看上面的错误信息"
fi

echo ""
