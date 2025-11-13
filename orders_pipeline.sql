CREATE OR REFRESH STREAMING TABLE orders_1_bronze
TBLPROPERTIES (
  "quality" = "bronze",
  "pipeline.reset.allowed" = false
)
AS
  SELECT
  *,
  current_timestamp() as proccessing_time,
  _metadata.file_name as source_file
  FROM STREAM read_files(
    "${source}/orders",
    format => "JSON"
  );

CREATE OR REFRESH STREAMING TABLE orders_2_silver
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
TBLPROPERTIES (
  "quality" = "silver"
)
AS
  SELECT
  to_timestamp(concat(Date, ' ', Time), "yyyy-MM-dd HH:mm:ss") AS order_timestamp,
  customer_id,
  order_id
  FROM STREAM orders_1_bronze;

CREATE OR REPLACE MATERIALIZED VIEW orders_3_gold
TBLPROPERTIES (
  "quality" = "gold"
)
AS
  SELECT
    date(order_timestamp) as order_date,
    count(*) as total_daily_orders
  FROM orders_2_silver
  GROUP BY date(order_timestamp);

