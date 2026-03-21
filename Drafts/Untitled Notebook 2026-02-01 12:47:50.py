# Databricks notebook source
#%pip install --upgrade databricks-sdk==0.82.0

%pip show databricks-sdk


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

# Initialize client
w = WorkspaceClient()

# Grant SELECT to service principal on the table
w.grants.update(
    securable_type=SecurableType.TABLE,
    full_name="hilton_hotmail.default.databricks_documentation_vs",
    changes=[
        {
            "add": {
                "principal": "hilton_hotmail_sp",      # service principal name
                "principal_type": "SERVICE_PRINCIPAL", # type of principal
                "privilege": "SELECT"                  # privilege to grant
            }
        }
    ]
)

print("✅ Table-level SELECT granted to service principal")

