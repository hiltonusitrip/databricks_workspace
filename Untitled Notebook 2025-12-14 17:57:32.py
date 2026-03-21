# Databricks notebook source
# MAGIC %sql
# MAGIC with q as (
# MAGIC   select
# MAGIC     statement_id,
# MAGIC     user_name,
# MAGIC     warehouse_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     total_duration_ms
# MAGIC   from system.query.history
# MAGIC ),
# MAGIC
# MAGIC qc as (
# MAGIC   select statement_id, compute_id
# MAGIC   from system.query.history.compute
# MAGIC ),
# MAGIC
# MAGIC u as (
# MAGIC   select
# MAGIC     compute_id,
# MAGIC     usage_start_time,
# MAGIC     usage_end_time,
# MAGIC     usage_quantity as dbus
# MAGIC   from system.billing.usage
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   q.user_name,
# MAGIC   q.statement_id,
# MAGIC   q.start_time,
# MAGIC   q.total_duration_ms,
# MAGIC   u.dbus
# MAGIC from q
# MAGIC join qc on q.statement_id = qc.statement_id
# MAGIC join u
# MAGIC   on qc.compute_id = u.compute_id
# MAGIC  and q.start_time between u.usage_start_time and u.usage_end_time

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM system.information_schema.metastore_privileges;

# COMMAND ----------

# MAGIC %sql describe detail vendor_demo_bronze

# COMMAND ----------

dbutils.fs.ls('/Volumes/hilton_catalog/bronze/data_files')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists employees
# MAGIC (employee_id int, name string);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employees values(1, 'Hilton');
# MAGIC
# MAGIC insert into employees values(2, 'Hotmail');
# MAGIC     
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %sql select * from employees version as of 2

# COMMAND ----------

# MAGIC %sql describe detail employees

# COMMAND ----------

# MAGIC %sql describe extended employees

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY view tmp_books_csv
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   header = 'true',
# MAGIC   delimiter = ';',
# MAGIC   path = '/Volumes/hilton_catalog/bronze/dataset/books-csv'
# MAGIC );
# MAGIC
# MAGIC select * from tmp_books_csv;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE books_csv
# MAGIC as select * from tmp_books_csv;
# MAGIC
# MAGIC select * from books_csv;
# MAGIC
# MAGIC describe extended books_csv;
# MAGIC

# COMMAND ----------


(  spark.read.table('books_csv').write
  .format('csv')
  .mode('append')
  .option('header', 'true')
  .option('delimiter',';')
  .save('/Volumes/hilton_catalog/bronze/dataset/books-csv')
)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/hilton_catalog/bronze/dataset/books-csv
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files('/Volumes/hilton_catalog/bronze/dataset/books-csv',format=>'csv',delimiter=>';', header=>true)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders from parquet.`/Volumes/hilton_catalog/bronze/dataset/orders-new`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers from read_files ('/Volumes/hilton_catalog/bronze/dataset/customers-json', format=>'json')

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view customers_update as select * from read_files('/Volumes/hilton_catalog/bronze/dataset/customers-json-new',format=>'json');
# MAGIC     
# MAGIC select * from customers_update

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into customers 
# MAGIC using customers_update
# MAGIC on customers.customer_id = customers_update.customer_id 
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC /*select schema_of_json(profile) from customers;*/
# MAGIC
# MAGIC
# MAGIC select from_json(profile, "STRUCT<address: STRUCT<city: STRING, country: STRING, street: STRING>, first_name: STRING, gender: STRING, last_name: STRING>").first_name from customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, array_distinct(flatten(collect_set(books.book_id))) FROM ORDERS GROUP BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;
# MAGIC select * from (select order_id, customer_id,quantity from orders)
# MAGIC pivot (sum(quantity) for order_id in ('000000000003559','000000000004243'))
# MAGIC order by customer_id
# MAGIC     
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, transform(books, x -> cast(x.subtotal * 0.7 as int))  as discounted_subtotal from orders)
# MAGIC

# COMMAND ----------



# COMMAND ----------

def get_url(email: str) -> str:
    return 'http://www.' + email.split('@')[1]

print(get_url('hma68@hotmail.com'))


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function get_url(email string)
# MAGIC returns string
# MAGIC return concat("http://www.", split(email,'@')[1]);
# MAGIC
# MAGIC select get_url('hma68@hotmail.com')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe function extended get_url

# COMMAND ----------

(spark.readStream.table('books_csv')
 .writeStream
 .format('delta')
 .outputMode('append')
 .trigger(availableNow=True)
 .option('checkpointLocation', '/Volumes/hilton_catalog/bronze/checkpoints/books')
 .table('books_stream')
 .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from books_stream order by book_id

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into books_csv select * from books_csv1;

# COMMAND ----------



(
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.schemaLocation', '/Volumes/hilton_catalog/bronze/checkpoints/orders/')
    .option('cloudfile.schemaEvolutionMode', 'addNewColumns')
    .load('/Volumes/hilton_catalog/bronze/dataset/orders-raw')
    .writeStream
    .format('delta')
    .outputMode('append')
    .trigger(availableNow=True)
    .option('checkpointLocation', '/Volumes/hilton_catalog/bronze/checkpoints/orders_checkpoint/')
    .table('orders_stream')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_stream

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.table('orders_stream')
subset = df.select('order_id','customer_id','quantity')

final = (subset.groupBy('customer_id').agg(F.sum(F.col('quantity').cast('int')).alias('total_quantity')))

final.show()



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cn_daily_customer_books

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_silver

# COMMAND ----------

try:
    spark.sql("SELECT 1").show()
    print("✓ Spark session is working")
except Exception as e:
    print(f"✗ Session error: {e}")
    print("Please restart the session")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hilton_hotmail.bookstore_eng_pro.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct quarantined from bookstore_eng_pro.orders_silver  

# COMMAND ----------

rules = {
    'valid_quantity': 'quantity>0',
    'valid_total': 'total>0'
}

print(" AND ".join(rules.values()))

quarantine_rules = "NOT {0}".format(' AND '.join(rules.values()))
print(quarantine_rules)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.bookstore_eng_pro.event_log