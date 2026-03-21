# Databricks notebook source
doc_articles = spark.read.text('/Volumes/hilton_hotmail/default/dataset/databricks_how_to_download/')
doc_articles.write.mode('overwrite').saveAsTable('raw_documentation')

display(doc_articles)


# COMMAND ----------

# TEST10
%pip install databricks-vectorsearch

dbutils.library.restartPython()
from transformers import AutoTokenizer, OpenAIGPTTokenizer
from langchain.text_splitter import RecursiveCharacterTextSplitter, HTMLHeaderTextSplitter


# COMMAND ----------

from langchain.text_splitter import RecursiveCharacterTextSplitter, HTMLHeaderTextSplitter
from transformers import AutoTokenizer, OpenAIGPTTokenizer
max_chunk_size=500

tokenizer = OpenAIGPTTokenizer.from_pretrained("openai-gpt")
text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(tokenizer, chunk_size=max_chunk_size, chunk_overlap=0)
html_splitter = HTMLHeaderTextSplitter(headers_to_split_on=['h2','header2'])


# COMMAND ----------

from pyspark.sql.functions import udf, explode, col
from pyspark.sql.types import ArrayType, StringType

df = spark.table('raw_documentation')
def split_text_udf(text):
    if text is None:
        return []
    return text_splitter.split_text(text)

split_udf = udf(split_text_udf, ArrayType(StringType()))

df_chunks = (
    df
    .withColumn("chunks", split_udf(col("value")))
    .select(explode(col("chunks")).alias("chunk"))
)
df_chunks.display()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, lit
df_chunks = (df_chunks.withColumn('id', monotonically_increasing_id())
            .withColumn('url', lit('https://www.databrocks.com'))
            .withColumnRenamed('chunk', 'content')
            .select('id', 'url', 'content')
)
spark.sql("DROP TABLE IF EXISTS databrocks_documentation")
df_chunks.write.mode('overwrite').saveAsTable('databrocks_documentation')

# COMMAND ----------

display(spark.sql('select * from databrocks_documentation'))

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import time

VECTOR_SEARCH_ENDPOINT_NAME = "vs_endpoint"

# Do NOT pass a URL here
vsc = VectorSearchClient()

# Create endpoint
try:
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME)
except Exception as e:
    print("Endpoint may already exist:", e)

# Wait until ONLINE
while True:
    eps = vsc.list_endpoints().get("endpoints", [])
    ep = next((e for e in eps if e["name"] == VECTOR_SEARCH_ENDPOINT_NAME), None)

    if ep and ep.get("state") == "ONLINE":
        print("Vector Search endpoint is ONLINE")
        break

    print("Waiting for endpoint...")
    time.sleep(20)
    

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
print(vsc.list_endpoints())

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

source_table_fullname = f'hilton_hotmail.default.databrocks_documentation'
vs_index_fullname = f'hilton_hotmail.default.databrocks_documentation_vs'
vs_endpoint_name = VECTOR_SEARCH_ENDPOINT_NAME

w = WorkspaceClient()

vsc.create_delta_sync_index(
    index_name=vs_index_fullname,
    endpoint_name=vs_endpoint_name,
    source_table_name=source_table_fullname,
    primary_key="id",
    pipeline_type='TRIGGERED',
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-bge-large-en"
)

# COMMAND ----------

import mlflow.deployments
deployment_client = mlflow.deployments.get_deploy_client('databricks')
VECTOR_SEARCH_ENDPOINT_NAME = "vs_endpoint"
source_table_fullname = f'hilton_hotmail.default.databrocks_documentation'
vs_index_fullname = f'hilton_hotmail.default.databrocks_documentation_vs'
vs_endpoint_name = VECTOR_SEARCH_ENDPOINT_NAME

question = "How to download file for databrocks?"

result = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
    query_text = question,
    columns = ['url','content'],
    num_results = 2
)
print(result)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

index = vsc.get_index(index_name="hilton_hotmail.default.databrocks_documentation_vs")

def query_vector_index(question, top_k=5):
    results = index.similarity_search(
        query_text=question,
        columns=["url","content"],
        num_results=2
    )
    return  " ".join([r[0] for r in results["result"]["data_array"]]), " ".join([r[1] for r in results["result"]["data_array"]])

print(query_vector_index("How to download file for databrocks"))

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from openai import OpenAI
import os


os.environ["OPENAI_API_KEY"] = dbutils.secrets.get(
    scope="openai-secrets",
    key="OPEN_API_KEY"
)

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

def ask_rag(question):
    context = query_vector_index(question)

    prompt = f"""
You are a helpful assistant. Answer the question ONLY using the context below.
If the answer is not in the context, say you don't know.

Context:
{context}

Question:
{question}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You answer using provided documentation context."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1
    )

    return response.choices[0].message.content

print(ask_rag("How to download file for databrocks?"))

# COMMAND ----------

import requests
import json
import os

DATABRICKS_TOKEN = dbutils.secrets.get(scope="db-secrets", key="DATABRICKS_TOKEN")

WORKSPACE_URL = "https://dbc-3a9c6033-1f4e.cloud.databricks.com"  # e.g. https://dbc-xxxx.cloud.databricks.com
ENDPOINT = "databricks-gpt-5-1"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

def ask_dbrx_rag(question):
    context = query_vector_index(question)

    prompt = f"""
You are a helpful assistant. Answer ONLY using the context below.
If the answer is not in the context, say you don't know.

Context:
{context}

Question:
{question}
"""

    payload = {
        "messages": [
            {"role": "system", "content": "You answer using provided documentation context."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2,
        "max_tokens": 500
    }


    response = requests.post(
        f"{WORKSPACE_URL}/serving-endpoints/{ENDPOINT}/invocations",
        headers=headers,
        data=json.dumps(payload)
    )

    return response.json()


print(ask_dbrx_rag("How to download file for databricks"))