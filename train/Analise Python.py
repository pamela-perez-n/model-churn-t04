# Databricks notebook source
# MAGIC %pip install feature-engine

# COMMAND ----------

# DBTITLE 1,SAMPLE
# Isso é um dataframe
df = spark.table("analytics.asn.abt_churn_2023").toPandas()

# Isso é uma string
target = 'flChurnMes'

# ISSO É UMA LISTA
colunas = df.columns.tolist()
features = colunas[5:]

X = df[features] # isso é uma matriz (dataframe)
y = df[target]   # isso é um vetor (series)

## SEPARANDO TREINO DE TESTE
from sklearn import model_selection

X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y,
                                                                    test_size=0.2,
                                                                    random_state=42,
                                                                    stratify=y)

print("Taxa de resposta na base de treino:", y_train.mean())
print("Taxa de resposta na base de teste:", y_test.mean())

print("Tamanho da base treino:", y_train.shape)
print("Tamanho da base teste:", y_test.shape)

cat_features = X.dtypes[X.dtypes == 'object'].index.tolist()
num_features = X.dtypes[X.dtypes != 'object'].index.tolist()

# COMMAND ----------

# DBTITLE 1,EXPLORE
import pandas as pd
pd.set_option('display.max_rows', 1000)

X_train[num_features].isna().sum().sort_values(ascending=False) # / X_train.shape[0]

# medianTempoTespostaAvaliacao -> media, mediana, modelo, -1000
# avgNotaAvaliacao             -> media, mediana, modelo, -1000
# avgTempoTespostaAvaliacao    -> media, mediana, modelo, -1000

# nrAvgIntervaloVenda -> média, mediana, modelo, 1000
# nrMinIntervaloVenda -> média, mediana, modelo, 1000
# nrMaxIntervaloVenda -> média, mediana, modelo, 1000

# nrTempoMedioEntregaPromessa -> 1000
# nrTempoMedioEntrega         -> 1000

# avgCubagem     -> média
# nrFretePeso    -> média
# nrFreteCubagem -> média
# avgPeso        -> média

# COMMAND ----------

# DBTITLE 1,EXPLORE
X_train[cat_features].isna().sum().sort_values(ascending=False) / X_train.shape[0]
# topCategoriaVenda   -> "sem_categoria"
# topCategoriaReceita -> "sem_categoria"

# COMMAND ----------

# DBTITLE 1,EXPLORE
df_train = X.copy()
df_train[target] = y.copy()

describe = df_train.groupby(target)[num_features].mean().T
describe['ratio'] = describe[1] / describe[0]
describe.sort_values('ratio', ascending=False)

# COMMAND ----------

# DBTITLE 1,MODIFY
from feature_engine import imputation

features_zeros = [
    'descAvaliacaoNeutraPedidoAvaliado',
    'descAvaliacaoAltaPedidoAvaliado',
    'descAvaliacaoBaixaPedidoAvaliado',
    'pctRespostaAvaliacao',
    'descAvaliacaoAltaPedido',
    'descAvaliacaoNeutraPedido',
    'descAvaliacaoBaixaPedido',
    'pctPedidoAvaliados',
]

imputer_zeros = imputation.ArbitraryNumberImputer(variables=features_zeros, arbitrary_number=0)
imputer_zeros.fit(X_train, y_train)

X_train_transform = imputer_zeros.transform(X_train)
X_test_transform = imputer_zeros.transform(X_test)


# COMMAND ----------

X_test[features_zeros].isna().sum()
