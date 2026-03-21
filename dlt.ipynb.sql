-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Broaze Layer

-- COMMAND ----------

create or refresh streaming live table orders_raw_live
as select * from cloud_files('${dataset_path}/orders-raw/', 'parquet', map("schema","order_id STRING, order_timestamp LONG, customer_id STRING, quantity long"))

-- COMMAND ----------

create or refresh live table customers_live 
comment "customers"
as select * from json.`/Volumes/hilton_catalog/bronze/dataset/customers-json/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver Layer

-- COMMAND ----------

create or refresh streaming live table orders_cleaned(
  constraint order_id_not_null expect (order_id is not null) on violation drop row
)
as select order_id, quantity, o.customer_id, c.profile:first_name as f_name,
c.profile:last_name as l_name, c.profile:address:country as country,
cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp from stream(live.orders_raw_live) o
left join live.customers_live c on o.customer_id = c.customer_id


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Gold Layer

-- COMMAND ----------

create or replace materialized view cn_daily_customer_books
comment "Chinese people daily books"
as
select customer_id, l_name, f_name, date_trunc('DD', order_timestamp) order_date, sum(quantity) book_count
from live.orders_cleaned
where country = 'China'
group by customer_id, l_name, f_name, date_trunc('DD', order_timestamp)

