# Databricks notebook source
# MAGIC %pip install feature-engine pandas==2.2.2

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

# DBTITLE 1,EXPLORE
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

from sklearn import preprocessing

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

## Imputação com média
features_avg = [
    'avgCubagem',
    'nrFretePeso',
    'nrFreteCubagem',
    'avgPeso',
]

imputer_avg = imputation.MeanMedianImputer(variables=features_avg, imputation_method='mean')

## Imputação com 1000
features_1000 = [
    'nrAvgIntervaloVenda',
    'nrMinIntervaloVenda',
    'nrMaxIntervaloVenda',
    'nrTempoMedioEntregaPromessa',
    'nrTempoMedioEntrega',
]

imputer_1000 = imputation.ArbitraryNumberImputer(variables=features_1000, arbitrary_number=1000)

## Imputação com -1000
features_minus_1000 = [
    'medianTempoTespostaAvaliacao',
    'avgNotaAvaliacao',
    'avgTempoTespostaAvaliacao',
]

imputer_minus_1000 = imputation.ArbitraryNumberImputer(variables=features_minus_1000, arbitrary_number=-1000)

## Imputação com sem_informacao
imputer_missing_cat = imputation.CategoricalImputer(variables=cat_features, fill_value="sem_informacao")

## Encoding
mean_encoder = encoding.MeanEncoder(variables=cat_features, unseen='encode')

## Discretiser
discretiser = discretisation.EqualWidthDiscretiser(variables=cat_features, bins=10)

## Scaler
scaler = preprocessing.StandardScaler()

# COMMAND ----------

# DBTITLE 1,MODEL
from sklearn import tree
from sklearn import pipeline
from sklearn import linear_model
from sklearn import ensemble
from sklearn import metrics
from sklearn import model_selection
from sklearn import neural_network

import mlflow

mlflow.set_experiment(experiment_id=2922960558567943)

with mlflow.start_run() as run:

    mlflow.sklearn.autolog()

    # clf = tree.DecisionTreeClassifier(min_samples_leaf=50, random_state=42)
    # clf = linear_model.LogisticRegression(random_state=42)
    # clf = ensemble.RandomForestClassifier(n_estimators=100, min_samples_leaf=50, random_state=42)
    # clf = ensemble.AdaBoostClassifier(n_estimators=500, random_state=42, learning_rate=0.01, algorithm="SAMME")
    # clf = ensemble.RandomForestClassifier(random_state=42, max_features=50, n_jobs=-1)

    clf = neural_network.MLPClassifier(random_state=42, warm_start=True, max_iter=1000)

    params = {
        "activation": ['relu'],
        "solver": ['lbfgs'],
        "hidden_layer_sizes": [(5,5,5,5), (10,5,2), (100,50,10), (50,10,5), (2,2,2), (10, 20, 20, 30, 40, 30, 20, 20, 10)],
        "early_stopping": [True],
    }

    grid = model_selection.GridSearchCV(clf, params,  scoring='roc_auc', refit=True, verbose=4, cv=3, n_jobs=-1)

    meu_pipeline = pipeline.Pipeline(steps=[
        ("Imput 1000", imputer_1000),
        ("Imput AVG", imputer_avg),
        ("Imput -1000", imputer_minus_1000),
        ("Imput 0", imputer_zeros),
        ("Imput Categorias", imputer_missing_cat ),
        ("Encoding", mean_encoder),
        # ("Discretiser", discretiser),
        ("Scaler", scaler),
        ("Modelo", grid),
    ])


    meu_pipeline.fit(X_train, y_train)

    y_predict = meu_pipeline.predict(X_train)
    y_predict_test = meu_pipeline.predict(X_test)

    acc_train = metrics.accuracy_score(y_train, y_predict)
    acc_test = metrics.accuracy_score(y_test, y_predict_test)

    y_proba = meu_pipeline.predict_proba(X_train)
    y_proba_test = meu_pipeline.predict_proba(X_test)

    auc_train = metrics.roc_auc_score(y_train, y_proba[:,1])
    auc_test = metrics.roc_auc_score(y_test, y_proba_test[:,1])

    metricas = {
        "auc_train": auc_train,
        "auc_test": auc_test,
        "acc_train": acc_train,
        "acc_test": acc_test,
        "auc_diff": auc_train - auc_test,
    }

    mlflow.log_metrics(metricas)
    mlflow.log_dict(grid.best_params_, artifact_file="best_params.txt")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM feature_store.upsell.models
# MAGIC where idCliente = '7163ce76-587a-4045-aa6a-cbf9ee98f6f6'
# MAGIC ORDER BY dtRef
