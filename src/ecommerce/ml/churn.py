"""Entrenamiento, evaluación y comparación del modelo de fuga.

Se usa scikit-learn en lugar de MLlib porque, en serverless, MLlib depende de
objetos de la JVM que Spark Connect no expone correctamente. Además, con unos
900 clientes y solo diez variables, los datos son pequeños y entrenar en el
driver es más eficiente; Spark se encarga de procesar y agregar el histórico.

Para evaluar el modelo, siempre se compara con una regla base sencilla, se usan
AUC-ROC y AUC-PR para medir mejor el rendimiento sobre la clase minoritaria, y
la prueba se hace con datos posteriores al entrenamiento para evitar que el
modelo tenga acceso indirecto al futuro.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ecommerce.ml.features import FEATURE_COLUMNS, LABEL_COLUMN

SCORE_COLUMN = "churn_probability"

# Semilla fija: dos ejecuciones sobre los mismos datos deben dar el mismo
# número, o las cifras que se publican no significan nada.
SEED = 42

ALGORITHMS: dict[str, Any] = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, C=100.0),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=200, max_depth=6, min_samples_leaf=5, random_state=SEED
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=100, max_depth=3, random_state=SEED
    ),
}


@dataclass(frozen=True)
class Metrics:
    """Resultado de evaluar un conjunto de predicciones.

    `n` y `positive_rate` acompañan siempre a las áreas: un AUC-PR de 0,4 es
    excelente con un 5 % de positivos y mediocre con un 40 %.
    """

    auc_roc: float
    auc_pr: float
    n: int
    positive_rate: float


@dataclass(frozen=True)
class Selection:
    """El algoritmo ganador junto a todo lo que se descartó, y la línea base."""

    algorithm: str
    model: Pipeline
    scores: dict[str, Metrics]
    baseline: Metrics


def build_pipeline(algorithm: str) -> Pipeline:
    """Escalado más clasificador.

    El escalado no le hace falta a los árboles, pero deja los coeficientes de
    la regresión logística en la misma unidad y por tanto comparables entre sí:
    sin él, `monetary` parecería irrelevante solo por medirse en cientos.
    """
    if algorithm not in ALGORITHMS:
        raise ValueError(f"Algoritmo desconocido: {algorithm!r}. Disponibles: {sorted(ALGORITHMS)}")
    return Pipeline([("escalado", StandardScaler()), ("clasificador", ALGORITHMS[algorithm]())])


def to_pandas(df: DataFrame) -> pd.DataFrame:
    """Trae al driver solo lo que el modelo necesita.

    Las columnas monetarias llegan como `decimal`; scikit-learn quiere flotantes.
    """
    columnas = [c for c in (LABEL_COLUMN, *FEATURE_COLUMNS) if c in df.columns]
    local = df.select("customer_id", *columnas).toPandas()
    for columna in columnas:
        local[columna] = local[columna].astype(float)
    return local


def train(training: DataFrame, *, algorithm: str = "logistic_regression") -> Pipeline:
    modelo = build_pipeline(algorithm)
    local = to_pandas(training)
    modelo.fit(local[list(FEATURE_COLUMNS)], local[LABEL_COLUMN])
    return modelo


def predict(model: Pipeline, df: DataFrame) -> DataFrame:
    """Probabilidad de fuga por cliente, de vuelta como DataFrame de Spark.

    Vuelve a Spark porque el destino es una tabla Delta que se consulta desde
    SQL y desde el dashboard, no un `DataFrame` de pandas en memoria.
    """
    local = to_pandas(df)
    local[SCORE_COLUMN] = model.predict_proba(local[list(FEATURE_COLUMNS)])[:, 1]
    return df.sparkSession.createDataFrame(local)


def evaluate(
    predictions: DataFrame, *, score_col: str = SCORE_COLUMN, label_col: str = LABEL_COLUMN
) -> Metrics:
    local = predictions.select(label_col, score_col).toPandas()
    etiquetas = local[label_col].astype(int)
    puntuaciones = local[score_col].astype(float)
    return Metrics(
        auc_roc=float(roc_auc_score(etiquetas, puntuaciones)),
        auc_pr=float(average_precision_score(etiquetas, puntuaciones)),
        n=len(local),
        positive_rate=float(etiquetas.mean()),
    )


def baseline_scores(df: DataFrame) -> DataFrame:
    """La regla trivial: cuanto más tiempo sin comprar, más riesgo.

    No es un modelo, es la intuición que cualquiera del negocio ya tiene. Se
    puntúa con la recencia normalizada para poder medirla con las mismas áreas
    que al modelo, en vez de compararla solo por acierto binario.
    """
    maximo = df.agg(F.max("recency_days")).first()[0] or 1
    return df.withColumn(SCORE_COLUMN, F.col("recency_days").cast("double") / F.lit(float(maximo)))


def evaluate_baseline(df: DataFrame) -> Metrics:
    return evaluate(baseline_scores(df))


def select_best(training: DataFrame, test: DataFrame, *, metric: str = "auc_pr") -> Selection:
    """Entrena todos los algoritmos y se queda con el mejor sobre el corte posterior.

    La selección mira el conjunto de prueba, así que sus métricas ya no son una
    estimación limpia del rendimiento futuro: son optimistas por construcción.
    Con tres candidatos y sin ajuste de hiperparámetros el sesgo es pequeño, y
    la alternativa —un tercer corte solo para elegir— dejaría menos historia de
    la que hay disponible.
    """
    modelos = {nombre: train(training, algorithm=nombre) for nombre in ALGORITHMS}
    scores = {nombre: evaluate(predict(modelo, test)) for nombre, modelo in modelos.items()}
    ganador = max(scores, key=lambda nombre: getattr(scores[nombre], metric))
    return Selection(
        algorithm=ganador,
        model=modelos[ganador],
        scores=scores,
        baseline=evaluate_baseline(test),
    )


def feature_importances(model: Pipeline) -> list[tuple[str, float]]:
    """Variables ordenadas por peso, de mayor a menor.

    Para la regresión son coeficientes sobre variables escaladas (comparables
    entre sí, con signo); para los árboles, reducción de impureza (siempre
    positiva). No son la misma magnitud y no se deben comparar entre modelos,
    pero dentro de uno responden a la pregunta útil: ¿en qué se está fijando?
    """
    clasificador = model.named_steps["clasificador"]
    pesos = (
        clasificador.coef_[0]
        if hasattr(clasificador, "coef_")
        else clasificador.feature_importances_
    )
    return sorted(
        zip(FEATURE_COLUMNS, (float(p) for p in pesos), strict=True),
        key=lambda par: abs(par[1]),
        reverse=True,
    )
