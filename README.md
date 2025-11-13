# Databricks Pipeline Project

Pipeline created for Databricks Academy course using SQL. Pipeline uses internal raw_data volum where .json files are added. Raw data contains data about customers, orders and status of the orders. Pipeline uses medallion architecture.

##Pipeline creates
**Bronze tables**
- customers_1_bronze (streams data from raw_data/customers)
- status_1_bronze (streams data from raw_data/status)
- orders_1_bronze (streams data from raw_data/orders)
- 
**Silver tables**
- customers_1_silver
- status_1_silver
- orders_1_silver
**Gold tables**
- 

**Pipeline architecture**
<img width="1620" height="613" alt="image" src="https://github.com/user-attachments/assets/9fb5cc01-ae2f-468e-b5e6-3103461ea1d1" />

(To be updated...)
