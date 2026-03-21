import pyspark.sql.functions as F
import pyspark.pipelines as dp

dataset_path = spark.conf.get('dataset_path')


@dp.table(
    name='bronze',
    partition_cols=['year_month'],
    table_properties={
                      'pipeline.reset.allowed':'false'}
)
@dp.expect_or_drop('positive_value','offset>0')
def process_bronze():
    schema = 'key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG'
    
    bronze_df = (spark.readStream.format('cloudFiles')
                 .option('cloudFiles.format', 'json')
                 .schema(schema)
                 .load(f'{dataset_path}/kafka-raw-etl')
                 .withColumn('timestamp', (F.col('timestamp')/1000).cast('timestamp'))
                 .withColumn('year_month', F.date_format(F.col('timestamp'),'yyyy-MM'))
    )
    return bronze_df



