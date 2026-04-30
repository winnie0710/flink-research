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
import java.io.Serializable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import static com.github.nexmark.flink.metric.BenchmarkMetric.NUMBER_FORMAT;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatDoubleValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValuePerSecond;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.Collector;
import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.source.NexmarkEventDeserializationSchema;

/**
 * Isolated Q4 Benchmark: Average Price for a Category.
 *
 * SQL equivalent:
 *   SELECT Q.category, AVG(Q.final)
 *   FROM (
 *     SELECT MAX(B.price) AS final, A.category
 *     FROM auction A, bid B
 *     WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
 *     GROUP BY A.id, A.category
 *   ) Q
 *   GROUP BY Q.category;
 *
 * DataStream pipeline:
 *   Source → [Filter/Map Auction, Filter/Map Bid]
 *     → IntervalJoin (auction.id = bid.auction, bid.time ∈ [auction.start, auction.expires])
 *     → MaxPricePerAuctionFunction (timer-based, fires at auction.expires)
 *     → AvgPricePerCategoryFunction (running average keyed by category)
 *     → KafkaSink
 */
public class BenchmarkIsoQ4 {

    private static final Set<String> UNSUPPORTED_QUERIES = Collections.singleton("q6");
    private static final String Q4_ISOLATED_NAME = "q4-isolated";

    // IntervalJoin upper bound (ingestion-time based).
    // Replay data arrives within seconds; 10 minutes is a safe upper bound
    // without inflating watermark holdback too much (old: 4 hours → 10 min).
    private static final long INTERVAL_JOIN_UPPER_BOUND_MINUTES = 10;

    // How long after the first matching bid arrives (ingestion time) to collect
    // bids before emitting the auction's max price.
    private static final long AUCTION_COLLECTION_WINDOW_MS = 60_000;

    private static final Option LOCATION = new Option("l", "location", true, "Nexmark directory.");
    private static final Option QUERIES = new Option("q", "queries", true, "Query to run.");
    private static final Option CATEGORY = new Option("c", "category", true, "Query category.");
    private static final Option SUBMIT_ONLY = new Option("so", "submit-only", false,
            "Submit job and exit without monitoring or cancelling (for CAOM managed mode).");

    public static final String CATEGORY_OA = "oa";

    // -------------------------------------------------------------------------
    // Inner POJOs for intermediate pipeline results
    // -------------------------------------------------------------------------

    /** Output of IntervalJoin: one record per matching (auction, bid) pair. */
    public static class AuctionBidResult implements Serializable {
        public long auctionId;
        public long category;
        public long bidPrice;
        public long auctionExpiresMs; // epoch millis, used as event-time timer

        public AuctionBidResult() {}

        public AuctionBidResult(long auctionId, long category, long bidPrice, long auctionExpiresMs) {
            this.auctionId = auctionId;
            this.category = category;
            this.bidPrice = bidPrice;
            this.auctionExpiresMs = auctionExpiresMs;
        }
    }

    /** Output of MaxPricePerAuctionFunction: winning price for one closed auction. */
    public static class CategoryMaxPrice implements Serializable {
        public long category;
        public long maxPrice;

        public CategoryMaxPrice() {}

        public CategoryMaxPrice(long category, long maxPrice) {
            this.category = category;
            this.maxPrice = maxPrice;
        }
    }

    // -------------------------------------------------------------------------
    // ProcessJoinFunction: filters bids within auction window and emits result
    // -------------------------------------------------------------------------

    public static class AuctionBidJoinFunction
            extends ProcessJoinFunction<Auction, Bid, AuctionBidResult> {
        @Override
        public void processElement(Auction auction, Bid bid, Context ctx,
                Collector<AuctionBidResult> out) {
            // Ingestion-time mode: dateTime comparison is no longer meaningful.
            // Timer fires at: auction's Kafka ingestion timestamp + collection window.
            long timerTarget = ctx.getLeftTimestamp() + AUCTION_COLLECTION_WINDOW_MS;
            out.collect(new AuctionBidResult(
                    auction.id,
                    auction.category,
                    bid.price,
                    timerTarget));
        }
    }

    // -------------------------------------------------------------------------
    // MaxPricePerAuctionFunction: tracks max bid price per auction.
    // Emits one CategoryMaxPrice when event-time reaches auction.expires.
    // -------------------------------------------------------------------------

    public static class MaxPricePerAuctionFunction
            extends KeyedProcessFunction<Long, AuctionBidResult, CategoryMaxPrice> {

        private transient ValueState<Long> maxPriceState;
        private transient ValueState<Long> categoryState;
        private transient ValueState<Boolean> timerRegistered;

        @Override
        public void open(Configuration parameters) {
            maxPriceState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("max-price", Long.class));
            categoryState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auction-category", Long.class));
            timerRegistered = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer-registered", Boolean.class));
        }

        @Override
        public void processElement(AuctionBidResult value, Context ctx,
                Collector<CategoryMaxPrice> out) throws Exception {
            Long currentMax = maxPriceState.value();
            if (currentMax == null || value.bidPrice > currentMax) {
                maxPriceState.update(value.bidPrice);
            }
            categoryState.update(value.category);

            // Register timer only once per auction key.
            Boolean registered = timerRegistered.value();
            if (registered == null || !registered) {
                ctx.timerService().registerEventTimeTimer(value.auctionExpiresMs);
                timerRegistered.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                Collector<CategoryMaxPrice> out) throws Exception {
            Long maxPrice = maxPriceState.value();
            Long category = categoryState.value();
            if (maxPrice != null && category != null) {
                out.collect(new CategoryMaxPrice(category, maxPrice));
            }
            // Clean up state after emission.
            maxPriceState.clear();
            categoryState.clear();
            timerRegistered.clear();
        }
    }

    // -------------------------------------------------------------------------
    // AvgPricePerCategoryFunction: running average per category.
    // Emits an updated average on every new winning price received.
    // -------------------------------------------------------------------------

    public static class AvgPricePerCategoryFunction
            extends KeyedProcessFunction<Long, CategoryMaxPrice, String> {

        private transient ValueState<Long> sumState;
        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) {
            sumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("price-sum", Long.class));
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("price-count", Long.class));
        }

        @Override
        public void processElement(CategoryMaxPrice value, Context ctx,
                Collector<String> out) throws Exception {
            Long sum = sumState.value();
            Long count = countState.value();
            if (sum == null) {
                sum = 0L;
                count = 0L;
            }
            sum += value.maxPrice;
            count++;
            sumState.update(sum);
            countState.update(count);
            out.collect(value.category + "," + (sum / count));
        }
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws ParseException {
        if (args == null || args.length == 0) {
            throw new RuntimeException(
                    "Usage: --queries q4-isolated --category oa --location /path/to/nexmark");
        }
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
        String category = CATEGORY.getValue(CATEGORY_OA).toLowerCase();
        boolean isQueryOa = CATEGORY_OA.equals(category);
        boolean submitOnly = line.hasOption(SUBMIT_ONLY.getOpt());
        Path queryLocation = isQueryOa ? location.resolve("queries") : location.resolve("queries-" + category);

        List<String> queries = getQueries(queryLocation, line.getOptionValue(QUERIES.getOpt()), isQueryOa);
        System.out.println("Benchmark Queries: " + queries);
        if (submitOnly) {
            System.out.println(">>> Submit-only mode: job will be managed externally by CAOM.");
        }
        runQueries(queries, location, category, submitOnly);
    }

    private static void runQueries(List<String> queries, Path location, String category, boolean submitOnly) {
        String flinkHome = System.getenv("FLINK_HOME");
        if (flinkHome == null) {
            throw new IllegalArgumentException("FLINK_HOME environment variable is not set.");
        }
        Path flinkDist = new File(flinkHome).toPath();

        Configuration nexmarkConf = NexmarkGlobalConfiguration.loadConfiguration();

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

        executeQueries(queries, workloadSuite, flinkRestClient, cpuMetricReceiver,
                monitorDelay, monitorInterval, monitorDuration,
                location, flinkDist, totalMetrics, category, nexmarkConf, submitOnly);

        if (!submitOnly) {
            printSummary(totalMetrics);
        }

        flinkRestClient.close();
        cpuMetricReceiver.close();
    }

    private static List<String> getQueries(Path queryLocation, String queries, boolean isQueryOa) {
        List<String> queryList = new ArrayList<>();
        if (!queries.equals("all")) {
            for (String queryName : queries.split(",")) {
                if (queryName.trim().equals(Q4_ISOLATED_NAME)) {
                    queryList.add(Q4_ISOLATED_NAME);
                    continue;
                }
                if (isQueryOa && UNSUPPORTED_QUERIES.contains(queryName)) continue;
                File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
                if (!queryFile.exists()) {
                    throw new IllegalArgumentException(
                            String.format("The query path \"%s\" does not exist.", queryFile.getAbsolutePath()));
                }
                queryList.add(queryName);
            }
        } else {
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
            Configuration nexmarkConf,
            boolean submitOnly) {

        for (String queryName : queries) {
            String workloadName = queryName.equals(Q4_ISOLATED_NAME) ? "q4" : queryName;
            Workload workload = workloadSuite.getQueryWorkload(workloadName);
            if (workload == null) {
                throw new IllegalArgumentException(
                        String.format("The workload of query %s is not defined.", workloadName));
            }
            workload.validateWorkload(monitorDuration);

            System.out.println("Clearing previous CPU metrics before starting query: " + queryName);
            cpuMetricReceiver.clearMetrics();

            MetricReporter reporter = new MetricReporter(
                    flinkRestClient, cpuMetricReceiver, monitorDelay, monitorInterval, monitorDuration);

            JobBenchmarkMetric metric;

            if (queryName.equals(Q4_ISOLATED_NAME)) {
                System.out.println(">>> Starting Custom Logic for: " + Q4_ISOLATED_NAME);
                metric = runIsolatedQ4(nexmarkConf, reporter, workload, submitOnly);
            } else {
                QueryRunner runner = new QueryRunner(
                        queryName, workload, location, flinkDist, reporter, flinkRestClient, category);
                metric = runner.run();
            }

            totalMetrics.put(queryName, metric);
        }
    }

    // -------------------------------------------------------------------------
    // Core Q4 isolated pipeline
    // -------------------------------------------------------------------------

    private static JobBenchmarkMetric runIsolatedQ4(
            Configuration conf, MetricReporter reporter, Workload workload, boolean submitOnly) {

        JobClient jobClient = null;
        try {
            // ----------------------------------------------------------------
            // 1. Environment
            // ----------------------------------------------------------------
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().disableGenericTypes();
            env.enableCheckpointing(120000);

            // ----------------------------------------------------------------
            // 2. Kafka Source (single source for all event types)
            // ----------------------------------------------------------------
            String kafkaBootstrap = conf.getString("kafka.bootstrap.servers", "kafka:9092");
            Properties sourceProps = new Properties();
            sourceProps.setProperty("fetch.min.bytes", "1048576");
            sourceProps.setProperty("fetch.max.wait.ms", "50");
            sourceProps.setProperty("max.partition.fetch.bytes", "5242880");
            sourceProps.setProperty("max.poll.records", "50000");
            sourceProps.setProperty("receive.buffer.bytes", "655360");

            KafkaSource<Event> source = KafkaSource.<Event>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setTopics("nexmark-events")
                    .setGroupId("nexmark-q4-isolated-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new NexmarkEventDeserializationSchema())
                    .setProperties(sourceProps)
                    .build();

            // Use Kafka record timestamp (ingestion time) as the event timestamp.
            // recordTimestamp is the Kafka message timestamp (set when producer writes to Kafka),
            // which is ≈ now regardless of the Nexmark dateTime field inside the payload.
            // This makes currentEmitEventTimeLag reflect real processing lag, not data age.
            WatermarkStrategy<Event> watermarkStrategy =
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner((event, recordTimestamp) -> recordTimestamp);

            DataStream<Event> events = env
                    .fromSource(source, watermarkStrategy, "Source: KafkaSource")
                    .name("Source: KafkaSource")
                    .uid("source-uid")
                    .slotSharingGroup("source-group");

            // ----------------------------------------------------------------
            // 3. Split into Auction and Bid streams (timestamps inherit from source)
            // ----------------------------------------------------------------
            DataStream<Auction> auctions = events
                    .filter(e -> e.newAuction != null)
                    .name("Filter-Auctions")
                    .uid("filter-auctions-uid")
                    .slotSharingGroup("source-group")
                    .map((MapFunction<Event, Auction>) e -> e.newAuction)
                    .returns(Auction.class)
                    .name("Map-To-Auction")
                    .uid("map-auction-uid")
                    .slotSharingGroup("source-group");

            DataStream<Bid> bids = events
                    .filter(e -> e.bid != null)
                    .name("Filter-Bids")
                    .uid("filter-bids-uid")
                    .slotSharingGroup("source-group")
                    .map((MapFunction<Event, Bid>) e -> e.bid)
                    .returns(Bid.class)
                    .name("Map-To-Bid")
                    .uid("map-bid-uid")
                    .slotSharingGroup("source-group");

            // ----------------------------------------------------------------
            // 4. Interval Join
            //    Left (auction) at time t: match right (bid) in [t+0, t+MAX_AUCTION_LIFETIME]
            //    Exact upper bound (bid.dateTime <= auction.expires) enforced in process function.
            // ----------------------------------------------------------------
            DataStream<AuctionBidResult> joined = auctions
                    .keyBy(a -> a.id)
                    .intervalJoin(bids.keyBy(b -> b.auction))
                    .between(Time.milliseconds(0), Time.minutes(INTERVAL_JOIN_UPPER_BOUND_MINUTES))
                    .process(new AuctionBidJoinFunction())
                    .name("Interval-Join")
                    .uid("interval-join-uid")
                    .slotSharingGroup("join-group");

            // ----------------------------------------------------------------
            // 5. First aggregation: max bid price per auction (timer fires at auction.expires)
            // ----------------------------------------------------------------
            DataStream<CategoryMaxPrice> maxPerAuction = joined
                    .keyBy(r -> r.auctionId)
                    .process(new MaxPricePerAuctionFunction())
                    .name("Max-Per-Auction")
                    .uid("max-per-auction-uid")
                    .slotSharingGroup("max-group");

            // ----------------------------------------------------------------
            // 6. Second aggregation: running average price per category
            // ----------------------------------------------------------------
            DataStream<String> avgPerCategory = maxPerAuction
                    .keyBy(r -> r.category)
                    .process(new AvgPricePerCategoryFunction())
                    .name("Avg-Per-Category")
                    .uid("avg-per-category-uid")
                    .slotSharingGroup("avg-group");

            // ----------------------------------------------------------------
            // 7. Kafka Sink (with throughput-optimized producer config)
            // ----------------------------------------------------------------
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

            avgPerCategory.sinkTo(sink)
                    .name("Sink: KafkaSink")
                    .uid("sink-uid")
                    .slotSharingGroup("sink-group");

            // ----------------------------------------------------------------
            // 8. Submit (non-blocking)
            // ----------------------------------------------------------------
            System.out.println("Submitting Q4 Isolated Job...");
            jobClient = env.executeAsync("Nexmark Q4 Isolated (Benchmark Driver)");
            JobID jobID = jobClient.getJobID();
            System.out.println("Job Submitted with ID: " + jobID);

            // ----------------------------------------------------------------
            // 9. Wait for RUNNING
            // ----------------------------------------------------------------
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

            // ----------------------------------------------------------------
            // 10. Submit-only mode: CAOM manages lifecycle, driver exits
            // ----------------------------------------------------------------
            if (submitOnly) {
                System.out.println(">>> Submit-only mode: job " + jobID
                        + " is RUNNING. Driver exits. CAOM will manage this job.");
                return new JobBenchmarkMetric(0.0, 0.0, 0L, 0L);
            }

            // ----------------------------------------------------------------
            // 11. Monitor (blocks until monitorDuration elapses)
            // ----------------------------------------------------------------
            return reporter.reportMetric(jobID.toHexString(), workload.getEventsNum());

        } catch (Exception e) {
            throw new RuntimeException("Failed to run isolated Q4", e);
        } finally {
            // Cancel only in non-submit-only mode; CAOM handles submit-only lifecycle.
            if (!submitOnly && jobClient != null) {
                System.out.println("Stopping job " + jobClient.getJobID());
                try {
                    jobClient.cancel().get();
                } catch (Exception e) {
                    System.err.println("Warning: Failed to cancel job " + jobClient.getJobID());
                    e.printStackTrace();
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Summary printing (identical to BenchmarkIsoQ7)
    // -------------------------------------------------------------------------

    public static void printSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        if (totalMetrics.isEmpty()) return;
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
        printLine(' ', "|", itemMaxLength, " Query", " Events Num", " Cores", " Time(s)",
                " Cores * Time(s)", " Throughput ", " Throughput/Cores");
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "");

        long totalEventsNum = 0;
        double totalCpus = 0, totalTimeSeconds = 0, totalCoresMultiplyTimeSeconds = 0;
        double totalThroughput = 0, totalThroughputPerCore = 0;

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
        printLine(' ', "|", itemMaxLength, "Total", "-", "-", formatLongValue(totalTpsPerCore));
        printLine('-', "+", itemMaxLength, "", "", "", "");
    }

    private static void printLine(char charToFill, String separator, int[] itemMaxLength, String... items) {
        StringBuilder builder = new StringBuilder();
        Iterator<Integer> lengthIterator = Arrays.stream(itemMaxLength).iterator();
        int lineLength = 0;
        for (String item : items) {
            if (lengthIterator.hasNext()) lineLength = lengthIterator.next();
            builder.append(separator);
            builder.append(item);
            int left = lineLength - item.length() - separator.length();
            for (int i = 0; i < left; i++) builder.append(charToFill);
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
