package com.github.nexmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;

import com.github.nexmark.flink.metric.FlinkRestClient;
import com.github.nexmark.flink.metric.JobBenchmarkMetric;
import com.github.nexmark.flink.metric.MetricReporter;
import com.github.nexmark.flink.metric.cpu.CpuMetricReceiver;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.source.NexmarkEventDeserializationSchema;
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
import java.util.*;

import static com.github.nexmark.flink.metric.BenchmarkMetric.NUMBER_FORMAT;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatDoubleValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValuePerSecond;

/**
 * Revised to include Hardcoded Q5 (Hot Items) Isolated Logic.
 */
public class BenchmarkIsoQ5 {

    private static final String Q5_ISOLATED_NAME = "q5-isolated";

    private static final Option LOCATION = new Option("l", "location", true, "Nexmark directory.");
    private static final Option QUERIES = new Option("q", "queries", true, "Query to run.");
    private static final Option CATEGORY = new Option("c", "category", true, "Query category.");

    public static final String CATEGORY_OA = "oa";

    public static void main(String[] args) throws ParseException {
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
        String category = CATEGORY.getValue(CATEGORY_OA).toLowerCase();
        boolean isQueryOa = CATEGORY_OA.equals(category);
        Path queryLocation = isQueryOa ? location.resolve("queries") : location.resolve("queries-" + category);

        List<String> queries = getQueries(queryLocation, line.getOptionValue(QUERIES.getOpt()), isQueryOa);
        runQueries(queries, location, category);
    }

    private static void runQueries(List<String> queries, Path location, String category) {
        Configuration nexmarkConf = NexmarkGlobalConfiguration.loadConfiguration();
        FlinkRestClient flinkRestClient = new FlinkRestClient(
                nexmarkConf.get(FlinkNexmarkOptions.FLINK_REST_ADDRESS),
                nexmarkConf.get(FlinkNexmarkOptions.FLINK_REST_PORT));
        CpuMetricReceiver cpuMetricReceiver = new CpuMetricReceiver(
                nexmarkConf.get(FlinkNexmarkOptions.METRIC_REPORTER_HOST),
                nexmarkConf.get(FlinkNexmarkOptions.METRIC_REPORTER_PORT));
        cpuMetricReceiver.runServer();

        WorkloadSuite workloadSuite = WorkloadSuite.fromConf(nexmarkConf, category);
        LinkedHashMap<String, JobBenchmarkMetric> totalMetrics = new LinkedHashMap<>();

        executeQueries(queries, workloadSuite, flinkRestClient, cpuMetricReceiver, location, totalMetrics, nexmarkConf);

        printSummary(totalMetrics);
        flinkRestClient.close();
        cpuMetricReceiver.close();
    }

    private static List<String> getQueries(Path queryLocation, String queries, boolean isQueryOa) {
        List<String> queryList = new ArrayList<>();
        for (String queryName : queries.split(",")) {
            if (queryName.trim().equals(Q5_ISOLATED_NAME)) {
                queryList.add(Q5_ISOLATED_NAME);
            } else {
                queryList.add(queryName);
            }
        }
        return queryList;
    }

    private static void executeQueries(
            List<String> queries,
            WorkloadSuite workloadSuite,
            FlinkRestClient flinkRestClient,
            CpuMetricReceiver cpuMetricReceiver,
            Path location,
            LinkedHashMap<String, JobBenchmarkMetric> totalMetrics,
            Configuration nexmarkConf) {

        for (String queryName : queries) {
            String workloadName = queryName.equals(Q5_ISOLATED_NAME) ? "q5" : queryName;
            Workload workload = workloadSuite.getQueryWorkload(workloadName);
            cpuMetricReceiver.clearMetrics();

            MetricReporter reporter = new MetricReporter(
                    flinkRestClient,
                    cpuMetricReceiver,
                    nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_DELAY),
                    nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_INTERVAL),
                    nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_DURATION));

            JobBenchmarkMetric metric;
            if (queryName.equals(Q5_ISOLATED_NAME)) {
                metric = runIsolatedQ5(nexmarkConf, reporter, workload);
            } else {
                // 原有的 SQL 執行路徑
                metric = new QueryRunner(queryName, workload, location, null, reporter, flinkRestClient, "oa").run();
            }
            totalMetrics.put(queryName, metric);
        }
    }

    /**
     * 執行硬編碼的 Q5 Isolated DataStream 邏輯
     * Q5: Hot Items (Sliding Window + Max Count)
     */
    private static JobBenchmarkMetric runIsolatedQ5(Configuration conf, MetricReporter reporter, Workload workload) {
        JobClient jobClient = null;
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().disableGenericTypes();
            env.enableCheckpointing(120000);

            String kafkaBootstrap = conf.getString("kafka.bootstrap.servers", "kafka:9092");
            Properties properties = new Properties();
            properties.setProperty("max.poll.records", "50000");

            KafkaSource<Event> source = KafkaSource.<Event>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setTopics("nexmark-events")
                    .setGroupId("nexmark-q5-isolated-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new NexmarkEventDeserializationSchema())
                    .setProperties(properties)
                    .build();

            // 1. Source & Ingest
            DataStream<Bid> bids = env
                    .fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(200)), "Source: KafkaSource")
                    .name("Source: KafkaSource")
                    .uid("source-uid")
                    .slotSharingGroup("ingest-group")
                    .filter(event -> event.bid != null)
                    .map(event -> event.bid)
                    .name("Filter-Map-Bids")
                    .slotSharingGroup("ingest-group");

            // 2. 第一層聚合：計算每個 Auction 在視窗內的投標數
            // Tuple3: (AuctionID, Count, WindowEnd)
            DataStream<Tuple3<Long, Long, Long>> windowCounts = bids
                    .keyBy(bid -> bid.auction)
                    .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(2)))
                    .aggregate(new CountAggregate())
                    .name("Window-Auction-Count")
                    .uid("window-count-uid")
                    .slotSharingGroup("count-group");

            // 3. 第二層聚合：找出每個視窗中 Count 最大的 Auction
            DataStream<String> hotItems = windowCounts
                    .keyBy(t -> t.f2) // 按 WindowEnd 分組
                    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(2)))
                    .process(new GetMaxProcessFunction())
                    .name("Window-Max-Filter")
                    .uid("window-max-uid")
                    .slotSharingGroup("max-group");

            // 4. Sink
            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("nexmark-results")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            hotItems.sinkTo(sink).name("Sink: KafkaSink").uid("sink-uid").slotSharingGroup("sink-group");

            System.out.println("Submitting Q5 Isolated Job...");
            jobClient = env.executeAsync("Nexmark Q5 Isolated (Migration Test)");
            JobID jobID = jobClient.getJobID();

            // 等待運行 (簡略原有的等待邏輯)
            Thread.sleep(5000);

            return reporter.reportMetric(jobID.toHexString(), workload.getEventsNum());

        } catch (Exception e) {
            throw new RuntimeException("Failed to run isolated Q5", e);
        } finally {
            if (jobClient != null) {
                try { jobClient.cancel().get(); } catch (Exception e) { /* ignore */ }
            }
        }
    }

    // --- Helper Classes for Q5 Logic ---

    /**
     * 累加器：計算 Auction 出現次數，並記錄視窗結束時間
     */
    public static class CountAggregate implements AggregateFunction<Bid, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {
        @Override public Tuple3<Long, Long, Long> createAccumulator() { return new Tuple3<>(0L, 0L, 0L); }
        @Override public Tuple3<Long, Long, Long> add(Bid value, Tuple3<Long, Long, Long> acc) {
            return new Tuple3<>(value.auction, acc.f1 + 1, 0L);
        }
        @Override public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> a, Tuple3<Long, Long, Long> b) {
            return new Tuple3<>(a.f0, a.f1 + b.f1, 0L);
        }
        @Override public Tuple3<Long, Long, Long> getResult(Tuple3<Long, Long, Long> acc) { return acc; }
    }

    /**
     * 處理視窗結果，帶入視窗結束時間
     */
    public static class WindowResultFunction extends ProcessWindowFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Tuple3<Long, Long, Long>> elements, Collector<Tuple3<Long, Long, Long>> out) {
            Tuple3<Long, Long, Long> next = elements.iterator().next();
            out.collect(new Tuple3<>(next.f0, next.f1, context.window().getEnd()));
        }
    }

    /**
     * 在同一個 WindowEnd 的所有資料中找出 Max 並輸出
     */
    public static class GetMaxProcessFunction extends ProcessWindowFunction<Tuple3<Long, Long, Long>, String, Long, TimeWindow> {
        @Override
        public void process(Long windowEnd, Context context, Iterable<Tuple3<Long, Long, Long>> elements, Collector<String> out) {
            List<Tuple3<Long, Long, Long>> list = new ArrayList<>();
            long maxCount = 0;
            for (Tuple3<Long, Long, Long> e : elements) {
                list.add(e);
                if (e.f1 > maxCount) maxCount = e.f1;
            }
            for (Tuple3<Long, Long, Long> e : list) {
                if (e.f1 == maxCount) {
                    out.collect(e.f0 + "," + e.f1); // 輸出格式：AuctionID, Count
                }
            }
        }
    }

    // --- 保持原有的 printSummary 和 Helper 方法 ---
    // (省略其餘與原檔相同的 printSummary, printLine, getOptions 等方法)
}