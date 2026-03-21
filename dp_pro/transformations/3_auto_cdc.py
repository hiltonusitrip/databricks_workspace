import pyspark.pipelines as dp

from pyspark.sql.functions import *

dataset_store = '/Volumes/hilton_hotmail/bookstore_eng_pro/dataset'

@dp.table

def country_lookup():
    return(spark.read.json(f'{dataset_store}/country_lookup')
    )

@dp.temporary_view
def customer_bronze_cdc():
    schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, city STRING, country_code STRING, row_status STRING, row_time TIMESTAMP"

    country_lookup_df = spark.read.table('country_lookup')
    
    bronze_df = (spark.readStream.table('bronze')
            .filter("topic='customers'")
            .select(from_json(col("value").cast('string'), schema).alias("v"))
            .select("v.*")
            .filter("row_status in ('insert','update')")
    )

    country_df = (bronze_df.alias('bronze')
            .join(broadcast(country_lookup_df.alias('c')), 
                  col('bronze.country_code') == col('c.code'), 
                  'inner')           
            .select(
                col('bronze.customer_id'),
                col('bronze.email'),
                col('bronze.last_name'),
                col('bronze.first_name'),
                col('bronze.gender'),
                col('c.country'),
                col('bronze.row_status'),
                col('bronze.row_time')
            )
    )

    return country_df

@dp.table 
def customers_silver():
    return spark.readStream.table('customer_bronze_cdc')

# Apply CDC changes with full DELETE support
'''
dp.apply_changes(
    source='customer_bronze_cdc',
    target='customers_silver',
    keys=['customer_id'],
    sequence_by='row_time',
    apply_as_deletes="col('row_status') == 'DELETE'",
    except_column_list=['row_status', 'row_time']
)
'''

@dp.materialized_view
def country_stats():
    customers_df=spark.read.table('customers_silver')
    orders_df = spark.read.table('orders_silver')

    return (customers_df.alias('c')
            .join(orders_df.alias('o'), 
                  col('c.customer_id') == col('o.customer_id'), 
                  'inner')           
            .groupBy('c.country')             
            .agg(count('o.order_id').alias('total_orders'),
                 sum('o.quantity').alias('total_quantity'))
    )


