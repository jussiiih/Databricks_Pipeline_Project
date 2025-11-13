CREATE OR REFRESH STREAMING TABLE customers_1_bronze
TBLPROPERTIES (
  "quality" = "bronze",
  "pipeline.reset.allowed" = false
)
AS
  SELECT
    *,
    current_timestamp() as proccessing_time,
    _metadata.file_name as source_file
  FROM STREAM
    read_files(
      "${source}/customers",
      format => "JSON"
    );

CREATE OR REFRESH STREAMING TABLE customers_2_silver;

CREATE FLOW customers_flow
AS
  AUTO CDC INTO customers_2_silver
  FROM
    STREAM customers_1_bronze
    KEYS (id)
    APPLY AS DELETE WHEN operation = "DELETE"
    SEQUENCE BY operation_timestamp
    COLUMNS * EXCEPT (operation, operation_timestamp, proccessing_time, source_file, _rescued_data)
    STORED AS SCD TYPE 2;

CREATE MATERIALIZED VIEW current_customers_gold
AS
  SELECT
    id as customer_id,
    name,
    address,
    city,
    email,
    phone
  FROM
    pipeline.pipeline_schema.customers_2_silver
  WHERE
    `__END_AT` IS NULL;
