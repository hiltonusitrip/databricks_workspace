from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode,avg, max, min,count

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

@dp.table
def device_bronze():
    static_df = spark.read.format("json").load("/Volumes/hilton_catalog/bronze/dataset/device/")
    _schema = static_df.schema
    return (
        spark
        .readStream
        .format('json')
        .schema(_schema)
        .load('/Volumes/hilton_catalog/bronze/dataset/device/')
    )

@dp.table
def device_silver():
    df_exploded=(spark.readStream.table('device_bronze')
       .withColumn('device', explode(col('data.devices')))
       .select(
           col('device.deviceId').alias('device_id'), 
           col('device.measure').alias('device_measure'),
           col('device.status').alias('device_status'), 
           col('device.temperature').alias('device_temperature') 
        )
    )
    return ( 
        df_exploded
    )

@dp.table
def device_gold():
    return (
        spark.readStream.table('device_silver')
        .groupBy('device_id')
        .agg(
            avg('device_temperature').alias('avg_temperature'),
            max('device_temperature').alias('max_temperature'),
            min('device_temperature').alias('min_temperature'),
            count('*').alias('count')
        )
    )
