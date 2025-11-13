CREATE OR REFRESH STREAMING TABLE status_1_bronze
TBLPROPERTIES (
  "quality" = "bronze",
  "pipeline.reset.allowed" = false
)
AS
  SELECT
    *,
    current_timestamp() as proccessing_time,
    _metadata.file_name as source_file
  FROM
    STREAM read_files(
    "${source}/status",
    format => "JSON"
  );

CREATE OR REFRESH STREAMING TABLE status_2_silver
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_status EXPECT (status IN ('placed', 'preparing', 'on the way', 'delivered', 'cancelled'))
)
TBLPROPERTIES (
  "quality" = "silver"
)
AS
  SELECT
    to_timestamp(concat(date, ' ', time), "yyyy-MM-dd HH:mm:ss") AS timestamp,
    order_id,
    status
  FROM STREAM status_1_bronze;


CREATE OR REFRESH MATERIALIZED VIEW full_order_info_gold
TBLPROPERTIES (
  "quality" = "gold"
)
AS
  SELECT
    orders.order_id,
    orders.order_timestamp,
    status.status,
    status.timestamp as status_timestamp

FROM orders_2_silver orders
LEFT JOIN status_2_silver status
ON orders.order_id = status.order_id;

CREATE OR REFRESH MATERIALIZED VIEW current_order_status_gold
TBLPROPERTIES (
  "quality" = "gold"
)
AS
SELECT
    order_id,
    order_timestamp,
    status,
    status_timestamp
FROM
  (SELECT
        order_id,
        order_timestamp,
        status,
        status_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY status_timestamp DESC
        ) AS row_number
    FROM full_order_info_gold
  )
WHERE row_number = 1;

CREATE OR REFRESH MATERIALIZED VIEW cancelled_orders_gold
TBLPROPERTIES (
  "quality" = "gold"
)
AS
SELECT
  order_id,
  order_timestamp,
  status,
  status_timestamp,
  datediff(DAY, order_timestamp, status_timestamp) AS days_to_cancel
FROM full_order_info_gold
WHERE status = 'cancelled';

CREATE OR REFRESH MATERIALIZED VIEW delivered_orders_gold
TBLPROPERTIES (
  "quality" = "gold"
)
AS
SELECT
  order_id,
  order_timestamp,
  status,
  status_timestamp,
  datediff(DAY, order_timestamp, status_timestamp) AS days_to_delivery
FROM full_order_info_gold
WHERE status = 'delivered';

