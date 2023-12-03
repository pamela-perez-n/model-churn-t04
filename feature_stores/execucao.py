# Databricks notebook source
import datetime

# COMMAND ----------

def read_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()


def table_exists(table, database):
    tables = (spark.sql(f"SHOW TABLES FROM {database}")
                   .toPandas()['tableName']
                   .unique()
                   .tolist())
    return table in tables


def make_ingestion(query, date, database, table):

    df = spark.sql(query.format(date=date))

    if table_exists(table, database):
        
        spark.sql(f"DELETE FROM {database}.{table} WHERE dtSafra = '{date}';")
        print("Carga incremental...")
        (df.write
            .format("delta")
            .mode("append")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{database}.{table}"))
        
    else:        
        print("Criando nova tabela...")
        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{database}.{table}"))

# COMMAND ----------


inicio = '2017-01-01'
fim = '2017-12-01'

dates = []
while inicio <= fim :
    dates.append(inicio)
    dt_inicio = datetime.datetime.strptime(inicio, '%Y-%m-%d')
    dt_next = dt_inicio + datetime.timedelta(days=1)
    inicio = dt_next.strftime("%Y-%m-%d")

dates_month = []
for d in dates:
    if d.endswith("01"):
        dates_month.append(d)

dates_month

# COMMAND ----------

table_name = "seller_produto"
query = read_query(f"{table_name}.sql")

for date in dates_month:
    make_ingestion(query, date, "analytics.asn", table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics.asn.seller_pagamento
# MAGIC WHERE idVendedor = '7aa4334be125fcdd2ba64b3180029f14'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE analytics.asn.seller_avaliacoes;
