SET 'parallelism.default' = '1';
-- ------------------------------------------------------------------------
-- 1. 定義 File System Sink 表 (目標: 本地 JSON 檔案)
-- ------------------------------------------------------------------------
CREATE TABLE nexmark_file_sink (
    `type` INT,
    `bid` ROW<
        auction BIGINT,
        bidder BIGINT,
        price BIGINT,
        channel STRING,
        url STRING,
        dateTime STRING,
        extra STRING
    >
) WITH (
    'connector' = 'filesystem',

    -- 設定輸出的路徑 (這是 Flink Container 內部的路徑)
    -- 注意：Flink 會產生一個資料夾，裡面包含 part-xxx 檔案
    'path' = 'file:///opt/flink/data/nexmark_data',

    'format' = 'json'
);

-- ------------------------------------------------------------------------
-- 2. 定義 Datagen Source 表
--
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

    -- 【重要】設定總共要產生多少筆資料 (例如 5000 萬筆)
    -- 產生完畢後 Job 會自動結束 (FINISHED)
    'number-of-rows' = '50000000',

    -- 這裡速率可以設快一點，反正只是為了生成檔案，越快寫完越好
    'rows-per-second' = '100000',

    -- 以下欄位規則維持不變 auction 10000種品項
    'fields.auction.min' = '1',
    'fields.auction.max' = '10000',
    'fields.bidder.min' = '1',
    'fields.bidder.max' = '100000',
    'fields.price.min' = '10',
    'fields.price.max' = '100000',
    'fields.channel.length' = '10',
    'fields.url.length' = '10',
    'fields.extra.length' = '10'
);

-- ------------------------------------------------------------------------
-- 3. 執行寫入
-- ------------------------------------------------------------------------
INSERT INTO nexmark_file_sink
SELECT
    2,
    ROW(
        auction,
        bidder,
        price,
        channel,
        url,
        DATE_FORMAT(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),
        extra
    )
FROM nexmark_datagen_source;