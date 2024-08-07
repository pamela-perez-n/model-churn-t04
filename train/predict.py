# Databricks notebook source
# MAGIC %pip install feature-engine pandas==2.2.2

# COMMAND ----------

import pandas as pd

modelo = pd.read_pickle("model_decision_tree.pkl")
