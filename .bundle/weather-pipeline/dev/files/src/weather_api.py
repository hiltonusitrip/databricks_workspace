dbutils.widgets.text("latitude", "37.77")
dbutils.widgets.text("longitude", "-122.41")

latitude = dbutils.widgets.get("latitude")
longitude = dbutils.widgets.get("longitude")

import argparse
import requests
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

catalog = args.catalog

bronze_table = f"{catalog}.bronze.weather_current"
silver_table = f"{catalog}.silver.weather_current"

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": latitude,
    "longitude": longitude,
    "current": "temperature_2m,wind_speed_10m",
}

import requests
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

r = requests.get(url, params=params, timeout=30)
r.raise_for_status()
payload = r.json()

df = (
    spark.createDataFrame([payload])
    .withColumn("ingested_at", F.current_timestamp())
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

(
    df.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable(bronze_table)
)

bronze_df = spark.table(bronze_table)

silver_source = (
    bronze_df
    .select(
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("timezone").alias("timezone"),
        F.col("timezone_abbreviation").alias("timezone_abbreviation"),
        F.col("current.time").cast("timestamp").alias("weather_time"),
        F.col("current.interval").cast("int").alias("weather_interval"),
        F.col("current.temperature_2m").cast("double").alias("temperature_2m"),
        F.col("current.wind_speed_10m").cast("double").alias("wind_speed_10m"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table} (
  latitude DOUBLE,
  longitude DOUBLE,
  timezone STRING,
  timezone_abbreviation STRING,
  weather_time TIMESTAMP,
  weather_interval INT,
  temperature_2m DOUBLE,
  wind_speed_10m DOUBLE,
  ingested_at TIMESTAMP,
  ingest_date DATE
) USING DELTA
""")

target_df = spark.table(silver_table)
max_ingested = target_df.select(F.max("ingested_at")).collect()[0][0]

if max_ingested is not None:
    silver_source = silver_source.filter(F.col("ingested_at") > F.lit(max_ingested))

silver_source = silver_source.withColumn("ingest_date", F.to_date("ingested_at"))

w = Window.partitionBy("latitude", "longitude", "weather_time").orderBy(F.col("ingested_at").desc())

silver_source = (
    silver_source
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

delta_target = DeltaTable.forName(spark, silver_table)

(
    delta_target.alias("t")
    .merge(
        silver_source.alias("s"),
        """
        t.latitude = s.latitude
        AND t.longitude = s.longitude
        AND t.weather_time = s.weather_time
        """
    )
    .whenMatchedUpdate(set={
        "timezone": "s.timezone",
        "timezone_abbreviation": "s.timezone_abbreviation",
        "weather_interval": "s.weather_interval",
        "temperature_2m": "s.temperature_2m",
        "wind_speed_10m": "s.wind_speed_10m",
        "ingested_at": "s.ingested_at",
        "ingest_date": "s.ingest_date",
    })
    .whenNotMatchedInsert(values={
        "latitude": "s.latitude",
        "longitude": "s.longitude",
        "timezone": "s.timezone",
        "timezone_abbreviation": "s.timezone_abbreviation",
        "weather_time": "s.weather_time",
        "weather_interval": "s.weather_interval",
        "temperature_2m": "s.temperature_2m",
        "wind_speed_10m": "s.wind_speed_10m",
        "ingested_at": "s.ingested_at",
        "ingest_date": "s.ingest_date",
    })
    .execute()
)

