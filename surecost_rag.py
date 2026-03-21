# Databricks notebook source
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("SureCostRag").getOrCreate()
df=spark.read.text("/Volumes/hilton_catalog/bronze/dataset/genai/")
df.show()
df.write.format("delta" ).mode("overwrite").saveAsTable(
    "hilton_catalog.bronze.genai"
)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col, lit, length, exp, explode, split

docs = spark.table('hilton_catalog.bronze.genai')

docs_clean = docs.filter(length(col('value')) > 50)
docs_clean.show()

# COMMAND ----------

chunks = docs_clean.withColumn('chunk', explode(split(col('value'), ' '))).withColumn('chunk_id', monotonically_increasing_id())
chunks=chunks.limit(30)
chunks.write.format('delta').mode('overwrite').saveAsTable('hilton_catalog.silver.genai')


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from hilton_catalog.silver.genai

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd
from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

@pandas_udf("array<float>")
def embed_udf(texts: pd.Series) -> pd.Series:
    batch = texts.tolist()
    resp = client.predict(
        endpoint="databricks-bge-large-en",
        inputs={"input": batch}
    )
    return pd.Series([x["embedding"] for x in resp["data"]])


spark_df = spark.table("hilton_catalog.silver.genai")

spark_df = spark_df.withColumn(
    "embeddings",
    embed_udf("chunk")
)

spark_df.write.format("delta").mode("overwrite").saveAsTable("hilton_catalog.gold.genai_embedded")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from hilton_catalog.gold.genai_embedded

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

vsc.create_delta_sync_index(
    endpoint_name="rag-endpoint",
    source_table_name="genai.rag.gold_embeddings",
    index_name="genai.rag.docs_index",
    primary_key="chunk_id",
    embedding_column="embedding"
)

