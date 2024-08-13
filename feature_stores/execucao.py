# Databricks notebook source
import datetime

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

## parametrizado para execução de tabelas de feature store e safras
inicio = dbutils.widgets.get("inicio")
fim = dbutils.widgets.get("fim")
table_name = dbutils.widgets.get("table_name")

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

# COMMAND ----------

query = read_query(f"{table_name}.sql")

for date in dates_month:
    make_ingestion(query, date, "analytics.asn", table_name)
