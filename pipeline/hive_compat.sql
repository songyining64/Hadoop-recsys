-- Executed as Spark SQL (HiveQL-compatible dialect).

CREATE OR REPLACE TEMP VIEW hive_user_rollups AS
SELECT user_id,
       COUNT(*) AS event_cnt,
       MAX(user_level) AS max_level,
       AVG(purchase_freq) AS avg_purchase_freq
FROM raw_events
GROUP BY user_id;

CREATE OR REPLACE TEMP VIEW hive_item_rollups AS
SELECT item_id,
       first(category) AS category,
       AVG(price) AS avg_price,
       AVG(like_num) AS avg_like_num
FROM raw_events
GROUP BY item_id;
