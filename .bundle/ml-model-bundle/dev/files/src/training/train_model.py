# Databricks notebook source
# MAGIC %md
# MAGIC # Train Model and Register to Unity Catalog
# MAGIC Placeholder: replace with your actual training logic and MLflow registration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get job parameters (from bundle job base_parameters)

# COMMAND ----------

dbutils.widgets.text("catalog", "hilton_hotmail", "Unity Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("model_name", "wine_quality_model", "Model name")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
model_name = dbutils.widgets.get("model_name")

print(f"Registering model to: {catalog}.{schema}.{model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Placeholder: add your training and MLflow registration here
# MAGIC Example: train, then `mlflow.register_model(..., f"{catalog}.{schema}.{model_name}")`

# COMMAND ----------

# Add your training code and mlflow.log_model / mlflow.register_model here.
# Model will be registered to: ${catalog}.${schema}.${model_name}
