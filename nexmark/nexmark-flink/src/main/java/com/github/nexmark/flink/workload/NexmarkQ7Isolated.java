/*
*  為了實現細粒度控制 (Fine-grained control) 並禁止 Operator Chaining 與 Slot Sharing，您必須使用 Flink DataStream API 撰寫。
*  env.disableOperatorChaining(): 強制切斷 Operator Chain，使每個 Operator 成為獨立 Task。
*  .slotSharingGroup("unique_name"): 這是禁止 Slot Sharing 最有效的程式化手段。
*  給每個 Operator 一個獨立的 group name，強制它們即使在同一個 TaskManager 也不能共享 Slot。
*
*
*
*
*
*/

package com.github.nexmark.flink.workload;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//  Flink 1.19 後建議使用 Java 原生 Duration
import java.time.Duration;

import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.source.NexmarkEventDeserializationSchema;

public class NexmarkQ7Isolated {

    public static void main(String[] args) throws Exception {

        // -------------------------------------------------------------------------
        // 1. 環境設定
        // -------------------------------------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如果發現任何不能當作 POJO 的資料型態，就直接拋出異常讓程式崩潰
        env.getConfig().disableGenericTypes();

        // [關鍵] 禁用 Operator Chaining，確保每個 Operator 都是獨立 Task，方便遷移
        env.disableOperatorChaining();

        // [Flink 2.1] 建議設定 Checkpoint，這對於實現「狀態遷移 (State Migration)」至關重要
        env.enableCheckpointing(10000); // 10秒一次
        // 在 Flink 2.x，建議在 flink-conf.yaml (或 config.yaml) 中設定預設 Checkpoint Storage
        // 若要程式碼內指定，可使用: env.getCheckpointConfig().setCheckpointStorage("file:///shared_data/checkpoints");

        // -------------------------------------------------------------------------
        // 2. Kafka Source 設定
        // -------------------------------------------------------------------------
        String kafkaBootstrap = "kafka:9092";

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("nexmark-events")
                .setGroupId("nexmark-q7-caom-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new NexmarkEventDeserializationSchema())
                .build();

        // -------------------------------------------------------------------------
        // 3. 定義資料流邏輯 (Q7: Highest Bid)
        // -------------------------------------------------------------------------

        // Step A: Source
        DataStream<Event> events = env
                .fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "Kafka Source")
                .name("Source")
                .uid("source-uid")
                .slotSharingGroup("source-group");

        // Step B: Filter & Map
        DataStream<Bid> bids = events
                .filter(event -> event.bid != null)
                .name("Filter-Bids")
                .uid("filter-uid")
                .slotSharingGroup("filter-group")
                .map(event -> event.bid)
                .name("Map-To-Bid")
                .uid("map-uid")
                .slotSharingGroup("map-group");

        // Step C: Window-Max (找出視窗內最高價)
        // [改進] 使用 Duration 取代 Time
        DataStream<Bid> maxBids = bids
                .keyBy(bid -> bid.auction)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(2)))
                .max("price")
                .name("Window-Max")
                .uid("window-max-uid")
                .slotSharingGroup("window-max-group");

        // Step D: Window-Join
        // 將 apply(...) 的結果強制轉型為 SingleOutputStreamOperator
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
                })) // 注意這裡的括號，轉型結束
                .name("Window-Join")       // 現在可以用了
                .uid("window-join-uid")
                .slotSharingGroup("window-join-group");

        // -------------------------------------------------------------------------
        // 4. Kafka Sink 設定
        // -------------------------------------------------------------------------
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("nexmark-results")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        highestBids
                .sinkTo(sink)
                .name("Sink")
                .uid("sink-uid")
                .slotSharingGroup("sink-group");

        env.execute("Nexmark Q7 Isolated (CAOM Experiment - Flink 1.9)");
    }
}
