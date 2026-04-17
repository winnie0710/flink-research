/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink;

// --- 原有的 Imports ---
import org.apache.flink.configuration.Configuration;
import com.github.nexmark.flink.metric.FlinkRestClient;
import com.github.nexmark.flink.metric.JobBenchmarkMetric;
import com.github.nexmark.flink.metric.MetricReporter;
import com.github.nexmark.flink.metric.cpu.CpuMetricReceiver;
import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import com.github.nexmark.flink.workload.Workload;
import com.github.nexmark.flink.workload.WorkloadSuite;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import static com.github.nexmark.flink.metric.BenchmarkMetric.NUMBER_FORMAT;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatDoubleValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValuePerSecond;

// --- 新增: Q7 Isolated 所需的 DataStream API Imports ---
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.common.JobID;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.source.NexmarkEventDeserializationSchema;

/**
 * The entry point to run benchmark for nexmark queries.
 * Revised to include Hardcoded Q7 Isolated Logic.
 */
public class BenchmarkIsoQ7 {

    // TODO: remove this once q6 is supported
    private static final Set<String> UNSUPPORTED_QUERIES = Collections.singleton("q6");
    private static final String Q7_ISOLATED_NAME = "q7-isolated"; // 自定義的查詢名稱

    private static final Option LOCATION = new Option("l", "location", true, "Nexmark directory.");
    private static final Option QUERIES = new Option("q", "queries", true, "Query to run.");
    private static final Option CATEGORY = new Option("c", "category", true, "Query category.");

    public static final String CATEGORY_OA = "oa";

    public static void main(String[] args) throws ParseException {
        if (args == null || args.length == 0) {
            throw new RuntimeException("Usage: --queries q1,q3,q7-isolated --category oa --location /path/to/nexmark");
        }
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
        String category = CATEGORY.getValue(CATEGORY_OA).toLowerCase();
        boolean isQueryOa = CATEGORY_OA.equals(category);
        Path queryLocation = isQueryOa ? location.resolve("queries") : location.resolve("queries-" + category);

        // 獲取查詢列表
        List<String> queries = getQueries(queryLocation, line.getOptionValue(QUERIES.getOpt()), isQueryOa);

        System.out.println("Benchmark Queries: " + queries);
        runQueries(queries, location, category);
    }

    private static void runQueries(List<String> queries, Path location, String category) {
        String flinkHome = System.getenv("FLINK_HOME");
        if (flinkHome == null) {
            throw new IllegalArgumentException("FLINK_HOME environment variable is not set.");
        }
        Path flinkDist = new File(flinkHome).toPath();

        // 載入配置
        Configuration nexmarkConf = NexmarkGlobalConfiguration.loadConfiguration();

        // Config Diagnosis
        System.err.println("--- NEXMARK CONFIGURATION ---");
        System.err.println("kafka.bootstrap.servers: " + nexmarkConf.getString("kafka.bootstrap.servers", "NOT_FOUND"));

        String jmAddress = nexmarkConf.get(FlinkNexmarkOptions.FLINK_REST_ADDRESS);
        int jmPort = nexmarkConf.get(FlinkNexmarkOptions.FLINK_REST_PORT);
        String reporterAddress = nexmarkConf.get(FlinkNexmarkOptions.METRIC_REPORTER_HOST);
        int reporterPort = nexmarkConf.get(FlinkNexmarkOptions.METRIC_REPORTER_PORT);

        FlinkRestClient flinkRestClient = new FlinkRestClient(jmAddress, jmPort);
        CpuMetricReceiver cpuMetricReceiver = new CpuMetricReceiver(reporterAddress, reporterPort);
        cpuMetricReceiver.runServer();

        Duration monitorDelay = nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_DELAY);
        Duration monitorInterval = nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_INTERVAL);
        Duration monitorDuration = nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_DURATION);

        WorkloadSuite workloadSuite = WorkloadSuite.fromConf(nexmarkConf, category);

        LinkedHashMap<String, JobBenchmarkMetric> totalMetrics = new LinkedHashMap<>();

        // 執行查詢
        executeQueries(
                queries,
                workloadSuite,
                flinkRestClient,
                cpuMetricReceiver,
                monitorDelay,
                monitorInterval,
                monitorDuration,
                location,
                flinkDist,
                totalMetrics,
                category,
                nexmarkConf // 傳入配置以便 Q7 使用
        );

        printSummary(totalMetrics);

        flinkRestClient.close();
        cpuMetricReceiver.close();
    }

    private static List<String> getQueries(Path queryLocation, String queries, boolean isQueryOa) {
        List<String> queryList = new ArrayList<>();
        // 特殊處理: 如果使用者輸入 q7-isolated，直接加入列表，不檢查檔案是否存在
        if (!queries.equals("all")) {
            for (String queryName : queries.split(",")) {
                if (queryName.trim().equals(Q7_ISOLATED_NAME)) {
                    queryList.add(Q7_ISOLATED_NAME);
                    continue;
                }
                // ... 原有的檔案檢查邏輯 ...
                if (isQueryOa && UNSUPPORTED_QUERIES.contains(queryName)) continue;
                File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
                if (!queryFile.exists()) {
                    throw new IllegalArgumentException(String.format("The query path \"%s\" does not exist.", queryFile.getAbsolutePath()));
                }
                queryList.add(queryName);
            }
        } else {
            // ... 原有 "all" 的邏輯 ...
            for (int i = 0; i < 100; i++) {
                String queryName = "q" + i;
                if (isQueryOa && UNSUPPORTED_QUERIES.contains(queryName)) continue;
                File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
                if (queryFile.exists()) queryList.add(queryName);
            }
        }
        return queryList;
    }

    private static void executeQueries(
            List<String> queries,
            WorkloadSuite workloadSuite,
            FlinkRestClient flinkRestClient,
            CpuMetricReceiver cpuMetricReceiver,
            Duration monitorDelay,
            Duration monitorInterval,
            Duration monitorDuration,
            Path location,
            Path flinkDist,
            LinkedHashMap<String, JobBenchmarkMetric> totalMetrics,
            String category,
            Configuration nexmarkConf) {

        for (String queryName : queries) {
            // 對於 q7-isolated，我們使用 q7 的 workload 配置 (或者您可以自定義)
            String workloadName = queryName.equals(Q7_ISOLATED_NAME) ? "q7" : queryName;

            Workload workload = workloadSuite.getQueryWorkload(workloadName);
            if (workload == null) {
                throw new IllegalArgumentException(String.format("The workload of query %s is not defined.", workloadName));
            }
            workload.validateWorkload(monitorDuration);

            // 在每次執行查詢前清空舊的 CPU metrics
            System.out.println("Clearing previous CPU metrics before starting query: " + queryName);
            cpuMetricReceiver.clearMetrics();

            MetricReporter reporter = new MetricReporter(
                    flinkRestClient,
                    cpuMetricReceiver,
                    monitorDelay,
                    monitorInterval,
                    monitorDuration);

            JobBenchmarkMetric metric;

            // [修改點] 判斷是否為我們的特殊查詢
            if (queryName.equals(Q7_ISOLATED_NAME)) {
                System.out.println(">>> Starting Custom Logic for: " + Q7_ISOLATED_NAME);
                metric = runIsolatedQ7(nexmarkConf, reporter, workload);
            } else {
                // 原有的 SQL 執行路徑
                QueryRunner runner = new QueryRunner(
                        queryName,
                        workload,
                        location,
                        flinkDist,
                        reporter,
                        flinkRestClient,
                        category);
                metric = runner.run();
            }

            totalMetrics.put(queryName, metric);
        }
    }

    /**
     * [Modified] 執行硬編碼的 Q7 Isolated DataStream 邏輯
     * 整合 QueryRunner 的生命週期管理與 MetricReporter 監控
     */
    private static JobBenchmarkMetric runIsolatedQ7(Configuration conf, MetricReporter reporter, Workload workload) {
        JobClient jobClient = null;
        try {
            // -------------------------------------------------------------------------
            // 1. 環境設定
            // -------------------------------------------------------------------------
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().disableGenericTypes();

            // checkpoint 間隔，不要太頻繁
            env.enableCheckpointing(120000);

            // -------------------------------------------------------------------------
            // 2. Kafka 設定
            // -------------------------------------------------------------------------
            String kafkaBootstrap = conf.getString("kafka.bootstrap.servers", "kafka:9092");
            Properties properties = new Properties();
            // 修改 1： 要求 Kafka 至少湊滿 1MB 資料才回傳，或者等待 500ms
            properties.setProperty("fetch.min.bytes", "1048576"); // 1MB (預設是 1 byte)
            properties.setProperty("fetch.max.wait.ms", "50");   // 最多等 500ms (預設是 500ms)
            properties.setProperty("max.partition.fetch.bytes", "5242880"); // 每個分區最大拉取 5MB
            // 預設是 500，這對於高吞吐 Benchmark 來說太小了
            // 修改 2 ： 把它設為 5000，讓 Flink 一次可以處理更多數據，減少 poll 的次數
            properties.setProperty("max.poll.records", "50000");

            // 額外建議：增加 TCP 接收緩衝區 (針對 Docker 網路優化)
            properties.setProperty("receive.buffer.bytes", "655360"); // 640KB

            KafkaSource<Event> source = KafkaSource.<Event>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setTopics("nexmark-events")
                    .setGroupId("nexmark-q7-isolated-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new NexmarkEventDeserializationSchema())   //解析 json 資訊，要驗證是否正確
                    .setProperties(properties)
                    .build();

            // -------------------------------------------------------------------------
            // 3. 資料流邏輯
            // -------------------------------------------------------------------------

            // [關鍵修正] 修改 Name 以適配 FlinkRestClient 的搜尋規則
            // 很多 FlinkRestClient 實作會尋找開頭為 "Source:" 的 Operator
            DataStream<Event> events = env
                    .fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(200)), "Source: KafkaSource")
                    .name("Source: KafkaSource")
                    .uid("source-uid")
                    .slotSharingGroup("ingest-group");

            DataStream<Bid> bids = events
                    .filter(event -> event.bid != null)
                    .name("Filter-Bids")
                    .uid("filter-uid")
                    .slotSharingGroup("ingest-group") // 共享
                    .map(event -> event.bid)
                    .name("Map-To-Bid")
                    .uid("map-uid")
                    .slotSharingGroup("ingest-group"); // 共享

            // 後續操作使用不同的 SlotSharingGroup 以強制隔離
            // 每 2 秒鐘，結算過去 10 秒鐘的帳
            DataStream<Bid> maxBids = bids
                    .keyBy(bid -> bid.auction)
                    .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(2)))
                    .max("price")
                    .name("Window-Max")
                    .uid("window-max-uid")
                    .slotSharingGroup("window-max-group"); // 獨立 Slot

            DataStream<String> highestBids = ((SingleOutputStreamOperator<String>) bids
                    .join(maxBids)
                    .where(bid -> bid.auction)
                    .equalTo(bid -> bid.auction)
                    .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(2)))
                    .apply(new JoinFunction<Bid, Bid, String>() {
                        @Override
                        public String join(Bid original, Bid max) {
                            return max.auction + "," + max.price + "," + max.bidder + "," + max.dateTime;
                        }
                    }))
                    .name("Window-Join")
                    .uid("window-join-uid")
                    .slotSharingGroup("window-join-group"); // 獨立 Slot

            // -------------------------------------------------------------------------
            // 4. Sink 設定
            // -------------------------------------------------------------------------
            Properties sinkProperties = new Properties();   // 設定批次發送電腦好像無法負荷
            //[關鍵優化 1] Linger ms: 讓 Producer 等待 5~10ms 以湊滿 Batch
            // 這會犧牲一點點延遲(Latency)，換取巨大的吞吐量(Throughput)提升
            sinkProperties.setProperty("linger.ms", "10");
            // [關鍵優化 2] Batch Size: 加大批次大小 (先設 16KB )
            sinkProperties.setProperty("batch.size", "131072");
            // [關鍵優化 3] 壓縮: 減少網路傳輸量 (推薦 lz4 或 snappy，CPU 開銷極低)
            sinkProperties.setProperty("compression.type", "lz4");
            // [選用] ACK 設定: 1 代表 Leader 收到就好，all 代表所有副本都要收到 (較慢但安全)
            // 配合 AT_LEAST_ONCE，Flink 會確保資料不掉，設為 1 通常效能較好
            sinkProperties.setProperty("acks", "1");
            sinkProperties.setProperty("buffer.memory", "67108864"); // 64MB 緩衝

            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("nexmark-results")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setKafkaProducerConfig(sinkProperties)
                    .build();

            highestBids.sinkTo(sink)
                    .name("Sink: KafkaSink")
                    .uid("sink-uid")
                    .slotSharingGroup("sink-group");

            // -------------------------------------------------------------------------
            // 5. 提交 Job (非阻塞) 提交到flink 上跑，java 繼續啟動監控
            // -------------------------------------------------------------------------
            System.out.println("Submitting Q7 Isolated Job...");
            jobClient = env.executeAsync("Nexmark Q7 Isolated (Benchmark Driver)");
            JobID jobID = jobClient.getJobID();
            System.out.println("Job Submitted with ID: " + jobID);

            // -------------------------------------------------------------------------
            // 6. 等待 Job 真正開始運行
            // -------------------------------------------------------------------------
            System.out.println("Waiting for job to start running...");
            int maxWaitSeconds = 30;
            int waitedSeconds = 0;
            while (waitedSeconds < maxWaitSeconds) {
                try {
                    org.apache.flink.api.common.JobStatus status = jobClient.getJobStatus().get();
                    if (status == org.apache.flink.api.common.JobStatus.RUNNING) {
                        System.out.println("Job is now RUNNING");
                        break;
                    }
                    System.out.println("Job status: " + status + ", waiting...");
                    Thread.sleep(1000);
                    waitedSeconds++;
                } catch (Exception e) {
                    System.err.println("Error checking job status: " + e.getMessage());
                    Thread.sleep(1000);
                    waitedSeconds++;
                }
            }

            if (waitedSeconds >= maxWaitSeconds) {
                throw new RuntimeException("Job did not start within " + maxWaitSeconds + " seconds");
            }

            // -------------------------------------------------------------------------
            // 7. 啟動監控 (參考 QueryRunner 邏輯)
            // -------------------------------------------------------------------------

            // 這裡不需要像 QueryRunner 一樣做 Warmup，直接進入監控
            // 使用 MetricReporter 的標準介面，這會阻塞直到 monitorDuration 結束
            JobBenchmarkMetric metric = reporter.reportMetric(jobID.toHexString(), workload.getEventsNum());

            return metric;

        } catch (Exception e) {
            throw new RuntimeException("Failed to run isolated Q7", e);
        } finally {
            // -------------------------------------------------------------------------
            // 8. 停止 Job (參考 QueryRunner 的 cancelJob)
            // -------------------------------------------------------------------------
            if (jobClient != null) {
                System.out.println("Stopping job " + jobClient.getJobID());
                try {
                    // 嘗試取消 Job
                    jobClient.cancel().get();
                } catch (Exception e) {
                    System.err.println("Warning: Failed to cancel job " + jobClient.getJobID());
                    e.printStackTrace();
                }
            }
        }
    }

    // ... 原有的 printSummary, printEventNumSummary, printTPSSummary, printLine, getOptions 方法保持不變 ...
    public static void printSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        if (totalMetrics.isEmpty()) {
            return;
        }
        System.err.println("-------------------------------- Nexmark Results --------------------------------");
        System.err.println();
        if (totalMetrics.values().iterator().next().getEventsNum() != 0) {
            printEventNumSummary(totalMetrics);
        } else {
            printTPSSummary(totalMetrics);
        }
        System.err.println();
    }

    private static void printEventNumSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        int[] itemMaxLength = {7, 18, 9, 11, 18, 15, 18};
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "");
        printLine(' ', "|", itemMaxLength, " Query", " Events Num", " Cores", " Time(s)", " Cores * Time(s)", " Throughput ", " Throughput/Cores");
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "");

        long totalEventsNum = 0;
        double totalCpus = 0;
        double totalTimeSeconds = 0;
        double totalCoresMultiplyTimeSeconds = 0;
        double totalThroughput = 0;
        double totalThroughputPerCore = 0;
        for (Map.Entry<String, JobBenchmarkMetric> entry : totalMetrics.entrySet()) {
            JobBenchmarkMetric metric = entry.getValue();
            double throughput = metric.getEventsNum() / metric.getTimeSeconds();
            double throughputPerCore = metric.getEventsNum() / metric.getCoresMultiplyTimeSeconds();
            printLine(' ', "|", itemMaxLength,
                    entry.getKey(),
                    NUMBER_FORMAT.format(metric.getEventsNum()),
                    NUMBER_FORMAT.format(metric.getCpu()),
                    formatDoubleValue(metric.getTimeSeconds()),
                    formatDoubleValue(metric.getCoresMultiplyTimeSeconds()),
                    formatLongValuePerSecond((long) throughput),
                    formatLongValuePerSecond((long) throughputPerCore));
            totalEventsNum += metric.getEventsNum();
            totalCpus += metric.getCpu();
            totalTimeSeconds += metric.getTimeSeconds();
            totalCoresMultiplyTimeSeconds += metric.getCoresMultiplyTimeSeconds();
            totalThroughput += throughput;
            totalThroughputPerCore += throughputPerCore;
        }
        printLine(' ', "|", itemMaxLength,
                "Total",
                NUMBER_FORMAT.format(totalEventsNum),
                formatDoubleValue(totalCpus),
                formatDoubleValue(totalTimeSeconds),
                formatDoubleValue(totalCoresMultiplyTimeSeconds),
                formatLongValuePerSecond((long) totalThroughput),
                formatLongValuePerSecond((long) totalThroughputPerCore));
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "");
    }

    // (為了完整性，這裡需要保留您原檔中的 helper methods，如 printLine 等)
    private static void printTPSSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        int[] itemMaxLength = {7, 18, 10, 18};
        printLine('-', "+", itemMaxLength, "", "", "", "");
        printLine(' ', "|", itemMaxLength, " Query", " Throughput (r/s)", " Cores", " Throughput/Cores");
        printLine('-', "+", itemMaxLength, "", "", "", "");

        long totalTpsPerCore = 0;
        for (Map.Entry<String, JobBenchmarkMetric> entry : totalMetrics.entrySet()) {
            JobBenchmarkMetric metric = entry.getValue();
            printLine(' ', "|", itemMaxLength,
                    entry.getKey(),
                    metric.getPrettyTps(),
                    metric.getPrettyCpu(),
                    metric.getPrettyTpsPerCore());
            totalTpsPerCore += metric.getTpsPerCore();
        }
        printLine(' ', "|", itemMaxLength,
                "Total",
                "-",
                "-",
                formatLongValue(totalTpsPerCore));
        printLine('-', "+", itemMaxLength, "", "", "", "");
    }

    private static void printLine(
            char charToFill,
            String separator,
            int[] itemMaxLength,
            String... items) {
        StringBuilder builder = new StringBuilder();
        Iterator<Integer> lengthIterator = Arrays.stream(itemMaxLength).iterator();
        int lineLength = 0;
        for (String item : items) {
            if (lengthIterator.hasNext()) {
                lineLength = lengthIterator.next();
            }
            builder.append(separator);
            builder.append(item);
            int left = lineLength - item.length() - separator.length();
            for (int i = 0; i < left; i++) {
                builder.append(charToFill);
            }
        }
        builder.append(separator);
        System.err.println(builder.toString());
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(QUERIES);
        options.addOption(CATEGORY);
        options.addOption(LOCATION);
        return options;
    }
}