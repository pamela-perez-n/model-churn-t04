# Databricks notebook source
# MAGIC %pip install feature-engine pandas==2.2.2

# COMMAND ----------

import mlflow
model = mlflow.sklearn.load_model("models:/olist-churn-t04/staging")

df = spark.table("analytics.asn.abt_churn_2023").toPandas()
df = df[df['dtSafra']==df['dtSafra'].max()]

df['proba_churn'] = model.predict_proba(df[model.feature_names_in_])[:,1]
df[['dtSafra', 'idVendedor', 'proba_churn']].sort_values(by='proba_churn', ascending=False)
