-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

--把operator拆成不同task
--SET 'pipeline.operator-chaining' = 'false';
-- -------------------------------------------------------------------------------------------------
-- 0. 清除舊的表定義 (這一步最重要！解決 Unknown identifier 錯誤)
-- -------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS bid;
DROP TABLE IF EXISTS nexmark_q7;

-- -------------------------------------------------------------------------------------------------
-- 1. 定義 Source 表 (讀取 Kafka)
--    這就是您缺失的部分。它負責從 nexmark-events 讀取 JSON 並定義 Watermark。
-- -------------------------------------------------------------------------------------------------
CREATE TABLE bid (
    auction  BIGINT,
    bidder   BIGINT,
    price    BIGINT,
    channel  STRING,
    url      STRING,
    event_time TIMESTAMP(3),  -- 對應 JSON 中的 event_time
    extra    STRING,

    -- !!! 這裡就是 Watermark 的定義 !!!
    -- 意義：告訴 Flink 使用 event_time 欄位作為事件時間，
    -- 並且容許最多 5 秒的數據延遲 (Out-of-orderness)。
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'nexmark-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'nexmark-q7-group',
    'scan.startup.mode' = 'earliest-offset', -- 從最早的數據開始讀，方便測試
    'format' = 'json'
    --'json.timestamp-format.standard' = 'SQL'
);

-- -------------------------------------------------------------------------------------------------
-- 2. 定義 Sink 表 (輸出結果)
--    目前使用 blackhole (黑洞)，數據寫入後會直接消失 (通常用於測速)。
--    如果您想在螢幕看結果，可以改用 'connector' = 'print'。
-- -------------------------------------------------------------------------------------------------
CREATE TABLE nexmark_q7 (
  auction  BIGINT,
  bidder   BIGINT,
  price    BIGINT,
  `dateTime` TIMESTAMP(3),
  extra    VARCHAR
) WITH (
  --'connector' = 'blackhole'
  'connector' = 'print'  -- 如果想看輸出，請把上面那行註解掉，換成這行
);

-- -------------------------------------------------------------------------------------------------
-- 3. 執行查詢 (Query 7: Highest Bid)
--    邏輯：找出由 window_start 到 window_end 這段時間內出價最高的紀錄
-- -------------------------------------------------------------------------------------------------
INSERT INTO nexmark_q7
SELECT
    B.auction,
    B.price,
    B.bidder,
    B.event_time,  -- 注意：這裡改用 event_time 以匹配 Source 定義
    B.extra
FROM bid B
JOIN (
  -- 子查詢：算出每個視窗內的最高價格 (MAX price)
  SELECT
    MAX(price) AS maxprice,
    window_end
  FROM TABLE(
      -- 定義 10 秒的滾動視窗 (Tumble Window)
      TUMBLE(TABLE bid, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
  )
  GROUP BY window_start, window_end
) B1
-- 條件：將原始資料流與最高價結果 Join
ON B.price = B1.maxprice
-- 確保 Join 的時間範圍正確 (只 Join 該視窗內的數據)
WHERE B.event_time BETWEEN B1.window_end - INTERVAL '10' SECOND AND B1.window_end;