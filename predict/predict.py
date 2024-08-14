# Databricks notebook source
import mlflow

date = dbutils.widgets.get("date")

with open("etl.sql") as open_file:
    query = open_file.read()

query = query.format(date=date)
df = spark.sql(query).toPandas()

model = mlflow.sklearn.load_model("models:/olist-churn-t04/production")

# COMMAND ----------

proba = model.predict_proba(df[model.feature_names_in_])
df['proba_churn'] = proba[:,1]
df_score = df[['dtSafra', 'idVendedor', 'proba_churn']].copy()
df_score = df_score.sort_values(by='proba_churn', ascending=True).reset_index(drop=True)
df_score['rank'] = list(range(1,df_score.shape[0]+1))
df_score['rank'] = df_score['rank'] / df_score.shape[0]
df_score = df_score.sort_values(by='rank', ascending=False)

df_score['proba_norm'] = (df_score['proba_churn']-df_score['proba_churn'].min()) / (df_score['proba_churn'].max() - df_score['proba_churn'].min())

df_spark = spark.createDataFrame(df_score)

spark.sql(f"DELETE FROM analytics.asn.model_churn_t04 WHERE dtSafra = '{date}'")

(df_spark.write
         .format("delta")
         .mode("append")
         .saveAsTable("analytics.asn.model_churn_t04"))
