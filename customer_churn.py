# Databricks notebook source
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix



# COMMAND ----------

df = (spark.read.options(header=True, inferSchema=True).csv('/Volumes/hilton_catalog/bronze/dataset/customer_churn/synthetic_customer_churn_100k.csv').toPandas())

df = df.drop('CustomerID',axis=1)
df["Churn"]=df['Churn'].map({'Yes':1,'No':0})
df["Gender"]=df["Gender"].map({'Male':1,'Female':0})
df['Age']= df['Age'].fillna(df['Age'].mean())
df['TotalCharges']= df['TotalCharges'].fillna(df['TotalCharges'].mean)
le = LabelEncoder()
df["Contract"]=le.fit_transform(df["Contract"])
df["PaymentMethod"]=le.fit_transform(df["PaymentMethod"])
scaler = StandardScaler()
df[['Age','Tenure','MonthlyCharges','TotalCharges']] = scaler.fit_transform(df[['Age','Tenure','MonthlyCharges','TotalCharges']])
df.head()

# COMMAND ----------

X = df.drop('Churn',axis=1)
y = df['Churn']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
with mlflow.start_run() as run:
    model = RandomForestClassifier(n_estimators=100, max_depth=7, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    mlflow.sklearn.log_model(model, "model", signature=infer_signature(X_train, model.predict(X_train)))
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))

    run_id = run.info.run_id
print("run_id:", run_id)
# DBTITLE 1,Get the model URI

model_name = "hilton_catalog.default.customer_churn_model"
model_uri = f"runs:/{run_id}/model"
mlflow.register_model(model_uri, model_name)


# COMMAND ----------

model_uri = f"models:/hilton_catalog.default.customer_churn_model/1"
loaded_model = mlflow.sklearn.load_model(model_uri)
y_pred=loaded_model.predict(X_test)
print(confusion_matrix(y_test,y_pred))
print(accuracy_score(y_test,y_pred))

# COMMAND ----------



# COMMAND ----------

