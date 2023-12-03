# Databricks notebook source
df = spark.table("analytics.asn.abt_churn_2023").toPandas()
df.info(memory_usage='deep')
