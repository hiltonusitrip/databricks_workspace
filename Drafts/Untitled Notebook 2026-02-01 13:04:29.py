# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

# Use Application ID, not display name
SP_APPLICATION_ID = "c7955551-40bf-46e0-a51b-00f405eae945"

w.grants.update(
    securable_type=catalog.SecurableType.TABLE,
    full_name="hilton_hotmail.default.databrocks_documentation_vs",
    changes=[
        catalog.PermissionsChange(
            add=[catalog.Privilege.SELECT],
            principal=SP_APPLICATION_ID,  # Application ID, not "hilton_hotmail_sp"
        )
    ],
)

# COMMAND ----------

import os
VECTOR_SEARCH_ENDPOINT_NAME = 'vs_endpoint'
index_name = "hilton_hotmail.default.databrocks_documentation_vs"
host = "https://dbc-3a9c6033-1f4e.cloud.databricks.com/"

os.environ['DATABRICKS_HOST'] = host
os.environ['DATABRICKS_TOKEN'] = dbutils.secrets.get('dbdemos','rag_sp_token')
os.environ['VECTOR_SEARCH_ENDPOINT_NAME'] = VECTOR_SEARCH_ENDPOINT_NAME
os.environ['VECTOR_SEARCH_INDEX_NAME'] = index_name




# COMMAND ----------



# COMMAND ----------

# MAGIC %pip install openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
from openai import OpenAI

# Set the Databricks workspace URL and your personal access token (PAT or SP token)
os.environ['DATABRICKS_HOST'] = "https://dbc-3a9c6033-1f4e.cloud.databricks.com"
os.environ['DATABRICKS_TOKEN'] = dbutils.secrets.get("dbdemos", "rag_sp_token")

# Initialize the OpenAI-compatible client for Databricks GPT
client = OpenAI(
    api_key=os.environ['DATABRICKS_TOKEN'],
    base_url=os.environ['DATABRICKS_HOST'] + "/openai/"
)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Initialize the vector search client
vsc = VectorSearchClient(
    workspace_url=os.environ["DATABRICKS_HOST"],
    service_principal_client_id=os.environ["DATABRICKS_CLIENT_ID"],
    service_principal_client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
)

# Get your index
index = vsc.get_index(index_name=os.environ['VECTOR_SEARCH_INDEX_NAME'])

def query_vector_index(question, top_k=5):
    results = index.similarity_search(
        query_text=question,
        columns=["text"],  # adjust if your index uses a different column
        num_results=top_k
    )
    # DB-VS returns a list of strings or dicts; handle both
    return [r if isinstance(r, str) else r.get("text") for r in results if r]

# COMMAND ----------

query_vector_index('How to download document in Databrocks?')

# COMMAND ----------

from openai import OpenAI
client = OpenAI(api_key="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")

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a helpful AI assistant."},
        {"role": "user", "content": "Explain how to update documentation."}
    ],
    max_tokens=500
)

answer = response.choices[0].message.content
print(answer)

# COMMAND ----------

