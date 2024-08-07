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

# (df_train.groupby(cat_features[0])[target]
#          .agg(['count', 'mean'])
#          .sort_values(['mean', 'count'], ascending=[False, True])
#          )

X_train[cat_features].isna().sum()

# COMMAND ----------

# DBTITLE 1,MODIFY
from feature_engine import imputation
from feature_engine import encoding
from feature_engine import discretisation

# Imputação com zeros
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

## Imputação com média
features_avg = [
    'avgCubagem',
    'nrFretePeso',
    'nrFreteCubagem',
    'avgPeso',
]

imputer_avg = imputation.MeanMedianImputer(variables=features_avg, imputation_method='mean')
imputer_avg.fit(X_train_transform, y_train)          # aprende a imputação que vai ser feita a partir dos dados de X_train

X_train_transform = imputer_avg.transform(X_train_transform)
X_test_transform = imputer_avg.transform(X_test_transform)

## Imputação com 1000
features_1000 = [
    'nrAvgIntervaloVenda',
    'nrMinIntervaloVenda',
    'nrMaxIntervaloVenda',
    'nrTempoMedioEntregaPromessa',
    'nrTempoMedioEntrega',
]

imputer_1000 = imputation.ArbitraryNumberImputer(variables=features_1000, arbitrary_number=1000)
imputer_1000.fit(X_train_transform, y_train)

X_train_transform = imputer_1000.transform(X_train_transform)
X_test_transform = imputer_1000.transform(X_test_transform)

## Imputação com -1000
features_minus_1000 = [
    'medianTempoTespostaAvaliacao',
    'avgNotaAvaliacao',
    'avgTempoTespostaAvaliacao',
]

imputer_minus_1000 = imputation.ArbitraryNumberImputer(variables=features_minus_1000, arbitrary_number=-1000)
imputer_minus_1000.fit(X_train_transform, y_train)

X_train_transform = imputer_minus_1000.transform(X_train_transform)
X_test_transform = imputer_minus_1000.transform(X_test_transform)


## Imputação com sem_informacao
imputer_missing_cat = imputation.CategoricalImputer(variables=cat_features, fill_value="sem_informacao")
imputer_missing_cat.fit(X_train_transform, y_train)

X_train_transform = imputer_missing_cat.transform(X_train_transform)
X_test_transform = imputer_missing_cat.transform(X_test_transform)

## Encoding
mean_encoder = encoding.MeanEncoder(variables=cat_features, unseen='encode')
mean_encoder.fit(X_train_transform, y_train)

X_train_transform = mean_encoder.transform(X_train_transform)
X_test_transform = mean_encoder.transform(X_test_transform)

## Discretiser
discretiser = discretisation.EqualWidthDiscretiser(variables=cat_features, bins=10)
discretiser.fit(X_train_transform, y_train)

X_train_transform = discretiser.transform(X_train_transform)
X_test_transform = discretiser.transform(X_test_transform)

# COMMAND ----------

# DBTITLE 1,Média (valéria)
from sklearn import tree

clf = tree.DecisionTreeClassifier(min_samples_leaf=50, random_state=42)

clf.fit(X_train_transform, y_train)

# COMMAND ----------

from sklearn import metrics

y_predict = clf.predict(X_train_transform)
y_predict_test = clf.predict( X_test_transform)

acc_train = metrics.accuracy_score(y_train, y_predict)
acc_test = metrics.accuracy_score(y_test, y_predict_test)

print("ACC train:", acc_train)
print("ACC test:", acc_test)
