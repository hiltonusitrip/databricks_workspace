# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

red_wine = pd.read_csv('/Volumes/hilton_hotmail/default/dataset/winqt/winequality-red.csv', sep=',', header=0)
white_wine = pd.read_csv('/Volumes/hilton_hotmail/default/dataset/winqt/winequality-white.csv', sep=';', header=0)

red_wine['is_red']=1
white_wine['is_red']=0

data = pd.concat([white_wine, red_wine], axis=0)
#wine

#for column in data.columns:
#    data = data.rename(columns={column: column.replace(' ', '_')})

data.rename(columns={column: column.replace(' ', '_') for column in data.columns}, inplace=True)

display(data)
'''
white_wine_spark = spark.createDataFrame(white_wine)
red_wine_spark = spark.createDataFrame(red_wine)
data = white_wine_spark.union(red_wine_spark)
display(data)
for column in data.columns:
    data = data.withColumnRenamed(column, column.replace(' ', '_'))

display(data)
'''

# COMMAND ----------

from matplotlib import pyplot as plt
import seaborn as sns
sns.heatmap(data.corr())


# COMMAND ----------

sns.histplot(data['quality'])

# COMMAND ----------

data['is_quality_high'] = (data['quality'] >= 7).astype(int)
data

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

dims = [4,3]

fig, ax = plt.subplots(dims[0], dims[1], figsize = (15,15))
for i in range(dims[0]):
    for j in range(dims[1]):
        if data.columns[i*dims[1]+j] == 'is_quality_high' or data.columns[i*dims[1]+j] == 'quality' :
            continue
        sns.boxplot(x=data["is_quality_high"], y = data.columns[i*dims[1]+j],data=data, ax=ax[i,j])
        ax[i,j].set_xlabel("is_high_quality")
        ax[i,j].set_ylabel(data.columns[i*dims[1]+j])
        

# COMMAND ----------

data.isnull().sum()

# COMMAND ----------

X = data.drop(['is_quality_high','quality'],axis=1)
y = data['quality']

from sklearn.model_selection import train_test_split, GridSearchCV
X_train, X_rem, y_train, y_rem = train_test_split(X, y, test_size=0.6, random_state=42)
X_test, X_valid, y_test, y_valid = train_test_split(X_rem, y_rem, test_size=0.5, random_state=42)

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
import mlflow
import mlflow.sklearn
import numpy as np

# Define search space
search_space = {
    'n_estimators': hp.quniform('n_estimators', 100,300,100),
    'max_depth': hp.choice('max_depth', [None] + list(range(1, 8))),
    'min_samples_split': hp.quniform('min_samples_split', 2, 5, 1)
}

def objective(params):
    with mlflow.start_run(nested=True):
        # Build pipeline with sampled params
        pipe = Pipeline([
            ('scaler', StandardScaler()),
            ('model', RandomForestRegressor(
                random_state=42,
                n_estimators=int(params['n_estimators']),
                max_depth=(
                    None if params['max_depth'] is None
                    else int(params['max_depth'])
                ),
                min_samples_split=int(params['min_samples_split'])
            ))
        ])

        pipe.fit(X_train, y_train)
        preds = pipe.predict(X_valid)

        mape = mean_absolute_percentage_error(y_valid, preds)

        mlflow.log_params(params)
        mlflow.log_metric("mape", mape)

        return {'loss': mape, 'status': STATUS_OK}

trials = Trials()
mlflow.autolog()
with mlflow.start_run(run_name="rf_hyperopt"):
    best_params = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,
        max_evals=20,
        trials=trials
    )

print("Best params:", best_params)

# COMMAND ----------

lowest_mape = trials.best_trial['result']['loss']
print(lowest_mape)

print(trials.best_trial)

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor


best_trial = trials.best_trial

best_params = best_trial['misc']['vals']
best_params = {
    k: int(v[0]) for k, v in best_params.items() if k in ('n_estimators', 'min_samples_split')
}

best_pipe = Pipeline([
    ('scaler', StandardScaler()),
    ('model', RandomForestRegressor(
        random_state=42,
        **best_params
    ))
])

best_pipe.fit(X_train, y_train)
preds = best_pipe.predict(X_test)
mape = mean_absolute_percentage_error(y_test, preds)
print(mape)

# COMMAND ----------

'''
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error, mean_absolute_percentage_error
import mlflow
import mlflow.sklearn

pipe = Pipeline([
    ('scaler', StandardScaler()),
    ('model', RandomForestRegressor(random_state=42))
])

param_grid = {
    'model__n_estimators': [100, 200, 300],
    'model__max_depth': [None, 5, 7],
    'model__min_samples_split': [2, 5]
}

gs = GridSearchCV(pipe, param_grid, cv=5, scoring= 'neg_mean_absolute_percentage_error')

with mlflow.start_run() as run:
    mlflow.autolog()
    gs.fit(X_train, y_train)
'''


# COMMAND ----------



'''
rf = gs.best_estimator_
y_pred = rf.predict(X_test)
print("RMSE: ", root_mean_squared_error(y_test, y_pred))
print("MAPE: ", mean_absolute_percentage_error(y_test, y_pred))
'''

# COMMAND ----------

model_name = 'wine_quality_model'
run_id = '5aca25b2875244f9bd206d4b5e73f34c'

model_version = mlflow.register_model(f"runs:/{run_id}/model", model_name)
print(model_version)


# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()

client.set_registered_model_alias(
    name="hilton_hotmail.default.wine_quality_model",
    version="3",
    alias="prod"
)

# COMMAND ----------

from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error, r2_score
model = mlflow.pyfunc.load_model(f'models:/hilton_hotmail.default.wine_quality_model@prod')
mean_absolute_percentage_error(y_test, model.predict(X_test))
input_dict = X_test.iloc[[0]].to_dict()
print(model.predict(input_dict))


# COMMAND ----------

import requests
import json

DATABRICKS_TOKEN = "dapi31600c02565364a9f9f9fbba4b6e3e06"
ENDPOINT_URL = "https://dbc-3a9c6033-1f4e.cloud.databricks.com/serving-endpoints/wine_quality_endpoint/invocations"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

data = {
    "dataframe_records": [
        {
            "fixed_acidity": 7.4,
            "volatile_acidity": 0.7,
            "citric_acid": 0.0,
            "residual_sugar": 1.9,
            "chlorides": 0.076,
            "free_sulfur_dioxide": 11,
            "total_sulfur_dioxide": 34,
            "density": 0.9978,
            "pH": 3.51,
            "sulphates": 0.56,
            "alcohol": 9.4,
            "is_red": 1
        }
    ]
}

response = requests.post(
    ENDPOINT_URL,
    headers=headers,
    data=json.dumps(data),
    timeout=30
)

# ---- DEBUG OUTPUT ----
print("Status code:", response.status_code)
print("Response text:", response.text)

# If JSON, pretty print
try:
    print("JSON:", response.json())
except Exception:
    pass