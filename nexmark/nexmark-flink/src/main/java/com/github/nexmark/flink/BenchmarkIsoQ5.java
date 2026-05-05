package com.github.nexmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
 * Q5 Hot Items — DataStream implementation that reproduces SQL HOP's network fan-out.
 *
 * SQL HOP(TABLE bid, 2s, 10s) expands each bid into 5 rows before shuffling to GROUP BY.
 * The naive DataStream approach (SlidingEventTimeWindows.aggregate after keyBy) absorbs
 * the fan-out into local state, producing no network pressure.
 *
 * Fix: add an explicit HopExpandFunction flatMap BEFORE the first keyBy so that the
 * 5× record multiplication crosses the network wire, matching SQL semantics.
 *
 * Each expanded record carries (auction, windowEnd, price, metadata) where metadata
 * is a concatenation of url|extra|channel|dateTime — this bloats each record to ~120 bytes
 * so that 175k records/s × 120 bytes ≈ 21 MB/s per HOP subtask, exceeding the 18.75 MB/s
 * limit on tm_20c_2/3 and producing a measurable network bottleneck without changing TPS.
 *
 * Pipeline:
 *   Source (ingest-group)
 *     → HOP-Expand 5× carrying full metadata (hop-group)  ← ~21 MB/s fan-out here
 *     → re-watermark on windowEnd
 *     → keyBy(auction:windowEnd) + Tumble(2s) aggregate (count, maxPrice) (count-group)
 *     → keyBy(windowEnd) + Tumble(2s) find max-count auction (max-group+sink)
 */
public class BenchmarkIsoQ5 {

    private static final String Q5_ISOLATED_NAME = "q5-isolated";

    // HOP window parameters (must match q5.sql)
    private static final long HOP_SIZE_MS  = 10_000L;
    private static final long HOP_SLIDE_MS =  2_000L;

    private static final Option LOCATION    = new Option("l", "location",    true,  "Nexmark directory.");
    private static final Option QUERIES     = new Option("q", "queries",     true,  "Query to run.");
    private static final Option CATEGORY    = new Option("c", "category",    true,  "Query category.");
    private static final Option SUBMIT_ONLY = new Option("so", "submit-only", false,
            "Submit job and exit without monitoring or cancelling (for CAOM managed mode).");

    public static final String CATEGORY_OA = "oa";

    public static void main(String[] args) throws ParseException {
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
        String category = CATEGORY.getValue(CATEGORY_OA).toLowerCase();
        boolean isQueryOa = CATEGORY_OA.equals(category);
        Path queryLocation = isQueryOa ? location.resolve("queries") : location.resolve("queries-" + category);

        boolean submitOnly = line.hasOption(SUBMIT_ONLY.getOpt());
        List<String> queries = getQueries(queryLocation, line.getOptionValue(QUERIES.getOpt()), isQueryOa);
        runQueries(queries, location, category, submitOnly);
    }

    private static void runQueries(List<String> queries, Path location, String category, boolean submitOnly) {
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
        executeQueries(queries, workloadSuite, flinkRestClient, cpuMetricReceiver, location, totalMetrics, nexmarkConf, submitOnly);

        if (!submitOnly) {
            printSummary(totalMetrics);
        }
        flinkRestClient.close();
        cpuMetricReceiver.close();
    }

    private static List<String> getQueries(Path queryLocation, String queries, boolean isQueryOa) {
        List<String> queryList = new ArrayList<>();
        for (String queryName : queries.split(",")) {
            queryList.add(queryName.trim().equals(Q5_ISOLATED_NAME) ? Q5_ISOLATED_NAME : queryName);
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
            Configuration nexmarkConf,
            boolean submitOnly) {

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
                metric = runIsolatedQ5(nexmarkConf, reporter, workload, submitOnly);
            } else {
                metric = new QueryRunner(queryName, workload, location, null, reporter, flinkRestClient, "oa").run();
            }
            totalMetrics.put(queryName, metric);
        }
    }

    /**
     * Hardcoded Q5 pipeline with explicit HOP fan-out over the network.
     */
    private static JobBenchmarkMetric runIsolatedQ5(Configuration conf, MetricReporter reporter, Workload workload, boolean submitOnly) {
        JobClient jobClient = null;
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().disableGenericTypes();
            env.enableCheckpointing(120000);

            String kafkaBootstrap = conf.getString("kafka.bootstrap.servers", "kafka:9092");
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("max.poll.records", "50000");

            KafkaSource<Event> source = KafkaSource.<Event>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setTopics("nexmark-events")
                    .setGroupId("nexmark-q5-isolated-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new NexmarkEventDeserializationSchema())
                    .setProperties(kafkaProps)
                    .build();

            // ── Stage 1: Source + filter/map ──────────────────────────────────────
            // SlotSharingGroup "ingest-group" keeps Source on preferred TMs.
            DataStream<Bid> bids = env
                    .fromSource(source,
                            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(200)),
                            "Source: KafkaSource")
                    .name("Source: KafkaSource")
                    .uid("source-uid")
                    .slotSharingGroup("ingest-group")
                    .filter(event -> event.bid != null)
                    .map(event -> event.bid)
                    .name("Filter-Map-Bids")
                    .slotSharingGroup("ingest-group");

            // ── Stage 2: HOP expand with full bid metadata ────────────────────────
            // Each bid → 5 records, one per overlapping HOP window.
            // Each record carries (auction, windowEnd, price, metadata) where
            // metadata = url|extra|channel|dateTime — totalling ~83 chars (~87 B).
            // Estimated serialized size per record: 8+8+8+87+overhead ≈ 120 B.
            // At ~175k expanded records/s per subtask: 175k × 120B ≈ 21 MB/s,
            // exceeding the 18.75 MB/s cap on tm_20c_2/3 → network bottleneck.
            //
            // Tuple4<Long, Long, Long, String>:
            //   f0 = auction id
            //   f1 = windowEnd (epoch ms)   ← used as event-time after re-watermark
            //   f2 = bid price
            //   f3 = metadata string (url|extra|channel|dateTime)
            DataStream<Tuple4<Long, Long, Long, String>> expandedBids = bids
                    .flatMap(new HopExpandFunction(HOP_SIZE_MS, HOP_SLIDE_MS))
                    .returns(Types.TUPLE(Types.LONG, Types.LONG, Types.LONG, Types.STRING))
                    .name("HOP-Expand-5x")
                    .uid("hop-expand-uid")
                    .slotSharingGroup("hop-group")
                    // timestamp = windowEnd - 1 so TumblingWindow(2s) fires at windowEnd
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Tuple4<Long, Long, Long, String>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                    .withTimestampAssigner((t, ts) -> t.f1 - 1L));

            // ── Stage 3: Aggregate (count, maxPrice) per (auction, windowEnd) ────
            // Tuple4 output: f0=auction, f1=count, f2=maxPrice, f3=windowEnd
            DataStream<Tuple4<Long, Long, Long, Long>> windowCounts = expandedBids
                    .keyBy(t -> t.f0 * 100_000_000L + t.f1 / HOP_SLIDE_MS)
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(HOP_SLIDE_MS)))
                    .aggregate(new HopCountAggregate())
                    .name("Window-Auction-Count")
                    .uid("window-count-uid")
                    .slotSharingGroup("count-group");

            // ── Stage 4: Find hot auction(s) per windowEnd ───────────────────────
            // keyBy f3 (windowEnd); Window-Max-Filter and Sink share "max-group"
            // so they chain together on the same thread.
            DataStream<String> hotItems = windowCounts
                    .keyBy(t -> t.f3)
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(HOP_SLIDE_MS)))
                    .process(new GetMaxProcessFunction())
                    .name("Window-Max-Filter")
                    .uid("window-max-uid")
                    .slotSharingGroup("max-group");

            // ── Stage 5: Sink ─────────────────────────────────────────────────────
            Properties sinkProps = new Properties();
            sinkProps.setProperty("linger.ms", "10");
            sinkProps.setProperty("batch.size", "131072");
            sinkProps.setProperty("compression.type", "lz4");
            sinkProps.setProperty("acks", "1");
            sinkProps.setProperty("buffer.memory", "67108864");

            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("nexmark-results")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setKafkaProducerConfig(sinkProps)
                    .build();

            hotItems.sinkTo(sink)
                    .name("Sink: KafkaSink")
                    .uid("sink-uid")
                    .slotSharingGroup("max-group");

            System.out.println("Submitting Q5 Isolated Job...");
            jobClient = env.executeAsync("Nexmark Q5 Isolated (Migration Test)");
            JobID jobID = jobClient.getJobID();
            System.out.println("Job submitted with ID: " + jobID);

            // Wait for RUNNING state before proceeding
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

            // Submit-only mode: CAOM manages lifecycle, driver exits without monitoring or canceling
            if (submitOnly) {
                System.out.println(">>> Submit-only mode: job " + jobID
                        + " is RUNNING. Driver exits. CAOM will manage this job.");
                jobClient = null; // prevent finally from canceling
                return new JobBenchmarkMetric(0.0, 0.0, 0L, 0L);
            }

            Thread.sleep(5000);
            return reporter.reportMetric(jobID.toHexString(), workload.getEventsNum());

        } catch (Exception e) {
            throw new RuntimeException("Failed to run isolated Q5", e);
        } finally {
            // Cancel only in non-submit-only mode; CAOM handles submit-only lifecycle.
            if (jobClient != null) {
                System.out.println("Stopping job " + jobClient.getJobID());
                try {
                    jobClient.cancel().get();
                } catch (Exception e) {
                    System.err.println("Warning: Failed to cancel job " + jobClient.getJobID());
                }
            }
        }
    }

    // ── Helper classes ────────────────────────────────────────────────────────

    /**
     * Expands one bid into (size/slide) records, one per overlapping HOP window.
     * Each record carries (auction, windowEnd, price, metadata) to increase the
     * per-record payload and drive network saturation on bandwidth-limited TMs.
     *
     * metadata = url|extra|channel|dateTime (~83 chars, ~87 bytes UTF-8 encoded).
     * Estimated record size: 8+8+8+87+framing ≈ 120 bytes.
     * At 35k bids/s × 5 windows × 120B ≈ 21 MB/s per subtask > 18.75 MB/s limit.
     */
    public static class HopExpandFunction implements FlatMapFunction<Bid, Tuple4<Long, Long, Long, String>> {
        private final long sizeMs;
        private final long slideMs;

        public HopExpandFunction(long sizeMs, long slideMs) {
            this.sizeMs  = sizeMs;
            this.slideMs = slideMs;
        }

        @Override
        public void flatMap(Bid bid, Collector<Tuple4<Long, Long, Long, String>> out) {
            long t    = bid.dateTime.toEpochMilli();
            long minK = (t / slideMs) + 1;
            long maxK = (t + sizeMs - 1) / slideMs;
            // Build metadata once per bid; reuse across all 5 window copies.
            String metadata = bid.url + "|" + bid.extra + "|" + bid.channel + "|" + bid.dateTime;
            for (long k = minK; k <= maxK; k++) {
                out.collect(new Tuple4<>(bid.auction, k * slideMs, bid.price, metadata));
            }
        }
    }

    /**
     * Aggregates (count, maxPrice) per (auction, windowEnd) with a CPU-intensive
     * bid-validation step that creates a measurable CPU bottleneck on count-group,
     * independently of the network bottleneck on hop-group.
     *
     * Each add() call runs a 2500-iteration LCG hash (fraudScore) to simulate a
     * real-world bid anomaly detection computation.  At 175k records/s per subtask:
     *   175k × ~5 µs/call ≈ 875 ms/s busy → T_busy > 70% → CPU_BOTTLENECK signal.
     *
     * The hash result is used in the return value so the JIT cannot hoist or
     * eliminate the loop.  Logically, score is never 0 (P ≈ 2⁻⁶⁴), so count
     * always increments by exactly 1 and maxPrice is unaffected.
     *
     * Input : Tuple4(auction, windowEnd, price, metadata)
     * Accum : Tuple4(auction, count, maxPrice, windowEnd)
     * Output: Tuple4(auction, count, maxPrice, windowEnd)
     */
    public static class HopCountAggregate
            implements AggregateFunction<
                    Tuple4<Long, Long, Long, String>,
                    Tuple4<Long, Long, Long, Long>,
                    Tuple4<Long, Long, Long, Long>> {

        @Override
        public Tuple4<Long, Long, Long, Long> createAccumulator() {
            return new Tuple4<>(0L, 0L, Long.MIN_VALUE, 0L);
        }

        @Override
        public Tuple4<Long, Long, Long, Long> add(
                Tuple4<Long, Long, Long, String> value,
                Tuple4<Long, Long, Long, Long> acc) {
            // Bid anomaly score: CPU-intensive hash over (auction, price).
            // score != 0 virtually always (P(score==0) ≈ 2⁻⁶⁴), so count += 1 always.
            // Branching on score forces the JIT to retain the entire computation.
            long score = fraudScore(value.f0, value.f2);
            return new Tuple4<>(
                    value.f0,
                    acc.f1 + (score != 0 ? 1L : 2L),
                    Math.max(acc.f2, value.f2),
                    value.f1);
        }

        @Override
        public Tuple4<Long, Long, Long, Long> merge(
                Tuple4<Long, Long, Long, Long> a,
                Tuple4<Long, Long, Long, Long> b) {
            return new Tuple4<>(a.f0, a.f1 + b.f1, Math.max(a.f2, b.f2), a.f3);
        }

        @Override
        public Tuple4<Long, Long, Long, Long> getResult(Tuple4<Long, Long, Long, Long> acc) {
            return acc;
        }

        /**
         * 2500-round LCG hash used to simulate per-bid fraud scoring.
         * Chosen constants are from Knuth / Steele & Vigna (widely-used LCG params).
         */
        private static long fraudScore(long auction, long price) {
            long h = auction ^ (price * 0x9e3779b97f4a7c15L);
            for (int i = 0; i < 2500; i++) {
                h = h * 6364136223846793005L + 1442695040888963407L;
            }
            return h;
        }
    }

    /**
     * Within a windowEnd group, emits auction(s) with the maximum bid count.
     * Output format: "auctionId,count,maxPrice"
     */
    public static class GetMaxProcessFunction
            extends ProcessWindowFunction<Tuple4<Long, Long, Long, Long>, String, Long, TimeWindow> {

        @Override
        public void process(Long windowEnd, Context ctx,
                            Iterable<Tuple4<Long, Long, Long, Long>> elements,
                            Collector<String> out) {
            long maxCount = 0;
            List<Tuple4<Long, Long, Long, Long>> list = new ArrayList<>();
            for (Tuple4<Long, Long, Long, Long> e : elements) {
                list.add(e);
                if (e.f1 > maxCount) maxCount = e.f1;
            }
            for (Tuple4<Long, Long, Long, Long> e : list) {
                if (e.f1 == maxCount) {
                    out.collect(e.f0 + "," + e.f1 + "," + e.f2);
                }
            }
        }
    }

    // ── Summary printing (unchanged) ──────────────────────────────────────────

    public static void printSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        if (totalMetrics.isEmpty()) return;
        System.err.println("-------------------------------- Nexmark Results --------------------------------");
        if (totalMetrics.values().iterator().next().getEventsNum() != 0) {
            printEventNumSummary(totalMetrics);
        } else {
            printTPSSummary(totalMetrics);
        }
    }

    private static void printEventNumSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        int[] itemMaxLength = {7, 18, 9, 11, 18, 15, 18};
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "");
        printLine(' ', "|", itemMaxLength, " Query", " Events Num", " Cores", " Time(s)", " Cores * Time(s)", " Throughput ", " Throughput/Cores");
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "");
        for (Map.Entry<String, JobBenchmarkMetric> entry : totalMetrics.entrySet()) {
            JobBenchmarkMetric m = entry.getValue();
            double tps = m.getEventsNum() / m.getTimeSeconds();
            printLine(' ', "|", itemMaxLength, entry.getKey(),
                    NUMBER_FORMAT.format(m.getEventsNum()),
                    NUMBER_FORMAT.format(m.getCpu()),
                    formatDoubleValue(m.getTimeSeconds()),
                    formatDoubleValue(m.getCoresMultiplyTimeSeconds()),
                    formatLongValuePerSecond((long) tps),
                    formatLongValuePerSecond((long) (m.getEventsNum() / m.getCoresMultiplyTimeSeconds())));
        }
    }

    private static void printTPSSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        int[] itemMaxLength = {7, 18, 10, 18};
        printLine('-', "+", itemMaxLength, "", "", "", "");
        printLine(' ', "|", itemMaxLength, " Query", " Throughput (r/s)", " Cores", " Throughput/Cores");
        printLine('-', "+", itemMaxLength, "", "", "", "");
        for (Map.Entry<String, JobBenchmarkMetric> entry : totalMetrics.entrySet()) {
            JobBenchmarkMetric m = entry.getValue();
            printLine(' ', "|", itemMaxLength, entry.getKey(),
                    m.getPrettyTps(), m.getPrettyCpu(), m.getPrettyTpsPerCore());
        }
    }

    private static void printLine(char charToFill, String separator, int[] itemMaxLength, String... items) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < items.length; i++) {
            builder.append(separator).append(items[i]);
            for (int j = 0; j < itemMaxLength[i] - items[i].length() - separator.length(); j++) {
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
        options.addOption(SUBMIT_ONLY);
        return options;
    }
}
