# 使用一個官方且較新的穩定 Flink 映像檔作為基礎
FROM flink:1.19.0-java11

# 安裝 iproute2 封裝，其中包含了 tc (Traffic Control) 命令
# Flink 官方映像檔 (如 1.18.1-java17) 通常基於 Debian/Ubuntu
# RUN apt-get update 必須在 RUN apt-get install 之前執行
RUN apt-get update && apt-get install -y iproute2

# (可選) 安裝其他網路測試工具
RUN apt-get install -y iperf3

# 安裝 JDK 開發工具（包含 jps 命令），用於 CPU Metric Sender
RUN apt-get install -y openjdk-11-jdk-headless

# 複製啟動腳本到不同的路徑，避免覆蓋原始 entrypoint
COPY start-taskmanager-with-metrics.sh /usr/local/bin/start-taskmanager-with-metrics.sh
RUN chmod +x /usr/local/bin/start-taskmanager-with-metrics.sh

# 使用新的腳本作為啟動命令
ENTRYPOINT ["/usr/local/bin/start-taskmanager-with-metrics.sh"]
CMD ["taskmanager"]