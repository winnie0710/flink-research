-- ------------------------------------------------------------------------
-- 1. 定義 Kafka Sink 表 (目標: nexmark-events)
--    這是資料將被寫入的地方，供後續 Q7 Job 讀取
-- ------------------------------------------------------------------------
CREATE TABLE nexmark_kafka_sink (
    `type` INT,  -- 0:Person, 1:Auction, 2:Bid (Nexmark 標準)
    `bid` ROW<
        auction BIGINT,
        bidder BIGINT,
        price BIGINT,
        channel STRING,
        url STRING,
        dateTime STRING, -- 改為 STRING 以便控制格式
        extra STRING
    >
) WITH (
    'connector' = 'kafka',
    'topic' = 'nexmark-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- ------------------------------------------------------------------------
-- 2. 定義 Datagen Source 表 (來源: 隨機生成器)
--    使用 Flink 內建的 datagen 連接器模擬 Nexmark Bid 事件
-- ------------------------------------------------------------------------
CREATE TABLE nexmark_datagen_source (
    auction  BIGINT,
    bidder   BIGINT,
    price    BIGINT,
    channel  STRING,
    url      STRING,
    event_time  TIMESTAMP(3),
    extra    STRING
) WITH (
    'connector' = 'datagen',

    -- 控制數據生成速率 (配合您的實驗需求，例如 1000 萬/秒可能太快，可先設低一點測試)
    'rows-per-second' = '500',

    -- 欄位生成規則 (模擬真實數據分佈)
    'fields.auction.min' = '1',
    'fields.auction.max' = '1000',
    'fields.bidder.min' = '1',
    'fields.bidder.max' = '10000',
    'fields.price.min' = '10',
    'fields.price.max' = '100000',
    'fields.channel.length' = '10',
    'fields.url.length' = '20',
    'fields.extra.length' = '100'
);

-- ------------------------------------------------------------------------
-- 3. 執行寫入操作
--    將 Datagen 生成的數據連續寫入 Kafka
--    使用 DATE_FORMAT 將 TIMESTAMP 轉換為 ISO-8601 格式字串
-- ------------------------------------------------------------------------
INSERT INTO nexmark_kafka_sink
SELECT
    2, -- 固定 type 為 2 (代表這是 Bid 事件)
    ROW(
        auction,
        bidder,
        price,
        channel,
        url,
        DATE_FORMAT(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''), -- 轉換為 ISO-8601 格式
        extra
    )
FROM nexmark_datagen_source;

