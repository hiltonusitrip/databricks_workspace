# Databricks notebook source
# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------



from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

#w.lakehouse_monitors.delete(
#    table_name="catalog.schema.table_name"
#)
for m in w.lakehouse_monitors.list():
    print(m.table_name)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for m in w.lakehouse_monitors.list():
    print(m.table_name)

w.lakehouse_monitors.delete(
    table_name="catalog.schema.table_name"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  distinct concat('drop table if exists ',
# MAGIC   catalog_name,'.',
# MAGIC   schema_name,'.',
# MAGIC   table_name,';')
# MAGIC FROM system.data_quality_monitoring.table_results
# MAGIC where event_time>'2026-01-05'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists hilton_hotmail.default.masked_vendor_demo;
# MAGIC drop table if exists hilton_hotmail.default.masked_vendor_demo_bronze;
# MAGIC drop table if exists hilton_hotmail.default.customers_live;
# MAGIC drop table if exists hilton_hotmail.default.customers;
# MAGIC drop table if exists hilton_hotmail.default.books_stream;
# MAGIC drop table if exists hilton_hotmail.default.sample_aggregation_dec_27_1159;
# MAGIC drop table if exists hilton_hotmail.default.dlt_vendor_demo;
# MAGIC drop table if exists hilton_hotmail.default.orders_stream;
# MAGIC drop table if exists hilton_hotmail.default.customer_silver;
# MAGIC drop table if exists hilton_hotmail.default.books_csv;
# MAGIC drop table if exists hilton_hotmail.default.process_bronze;
# MAGIC drop table if exists hilton_hotmail.default.orders;
# MAGIC drop table if exists hilton_hotmail.default.orders_raw_live;
# MAGIC drop table if exists hilton_hotmail.default.orders_cleaned;
# MAGIC drop table if exists hilton_hotmail.default.bronze;
# MAGIC drop table if exists hilton_hotmail.default.employees;
# MAGIC drop table if exists hilton_hotmail.default.masked_vendor_demo_silver;
# MAGIC drop table if exists hilton_hotmail.default.books_silver;
# MAGIC drop table if exists hilton_hotmail.default.books_csv1;
# MAGIC drop table if exists hilton_hotmail.default.author_count_state;
# MAGIC drop table if exists hilton_hotmail.default.orders_microbatch;
# MAGIC drop table if exists hilton_hotmail.default.sample_users_dec_27_1159;
# MAGIC drop table if exists hilton_hotmail.default.cn_daily_customer_books;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.books_raw;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.author_stats;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.country_stats_profile_metrics;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.country_stats_drift_metrics;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.current_books;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.country_stats;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.orders_silver;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.bronze;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.order_quarantine;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.customers_silver;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.books_sales;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.event_log_dp_pro;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.country_lookup;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.books_silver;
# MAGIC drop table if exists hilton_hotmail.default.books_bronze_drift_metrics;
# MAGIC drop table if exists hilton_hotmail.default.books_bronze_profile_metrics;
# MAGIC drop table if exists hilton_hotmail.bookstore_eng_pro.country_lookup1;
# MAGIC drop table if exists hilton_hotmail.default.device_gold;
# MAGIC drop table if exists hilton_hotmail.default.device_silver;
# MAGIC drop table if exists hilton_hotmail.default.device_bronze;
# MAGIC drop table if exists hilton_hotmail.default.bronze_customers;
# MAGIC drop table if exists hilton_hotmail.default.silver_customers;
# MAGIC drop table if exists hilton_hotmail.default.customer_dim;
# MAGIC drop table if exists hilton_hotmail.default.vendor_demo_bronze;
# MAGIC drop table if exists hilton_hotmail.default.raw_documentation;
# MAGIC drop table if exists hilton_hotmail.default.databrocks_documentation;