# Databricks Pipeline Project

This pipeline was created for a Databricks Academy course using SQL. It processes data stored in the internal `raw_data` volume, where .json files are added. The raw data includes information about customers, orders, and order statuses. All data is mock data generated with ChatGPT based on a schema defined by the developer. The pipeline follows the Medallion Architecture.

## Pipeline Architecture
![pipeline skeema](https://github.com/user-attachments/assets/b50a0521-1b24-450a-a608-1b067a3bb8cc)


## Pipeline Outputs

### **Bronze Tables**
- customers_1_bronze – streams data from raw_data/customers
- status_1_bronze – streams data from raw_data/status
- orders_1_bronze – streams data from raw_data/orders

### **Silver Tables**
- customers_2_silver – streaming table creted by using Auto CDC type 2, table captures all changes in customer data
- status_2_silver – streaming table that cleans the bronze status table and applies several data quality constraints
- orders_2_silver – streaming table that cleans the bronze orders table and applies several data quality constraints

### **Gold Tables**
- current_customers_gold – materialized view showing only current customers,
- full_order_info_gold – materialized view joining orders with their status information
- current_order_status_gold – materialized view created from full_order_info_gold showing the current status of all orders
- cancelled_orders_gold – materialized view showing only cancelled orders
- delivered_orders_gold – materialized view showing only delivered orders
- orders_by_date_gold – materialized view that aggregates the daily order count from orders_2_silver



