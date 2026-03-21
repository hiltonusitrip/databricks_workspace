from pyspark import pipelines as dp
from pyspark.sql.functions import *


rules = {
    'valid_quantity': 'quantity>0',
    'valid_total': 'total>0'
}

quarantine_rules = "NOT ({0})".format(' AND '.join(rules.values()))

def process_orders():
    schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT< book_id:STRING, quantity:BIGINT, subtotal:BIGINT >>"

    return(spark.readStream.table('bronze')
            .filter("topic='orders'")
            .select(from_json(col('value').cast('string'),schema).alias("v"))
            .select("v.*")
            .withColumn('quarantined', expr(quarantine_rules))     
    )

@dp.table
def orders_silver():
    return process_orders()



@dp.table
@dp.expect_or_drop('invalid_quantity','quantity<=0 or quantity is null')
def order_quarantine():
    return process_orders()

book_schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
@dp.table
def books_raw():
    return (spark.readStream.table('bronze')
           .filter('topic="books"')
           .select(from_json(col('value').cast('string'),book_schema).alias('v'))
           .select ("v.*")
    )
                
            