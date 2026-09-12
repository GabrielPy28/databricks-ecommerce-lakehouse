"""Entrenamiento y evaluación del modelo de fuga.

Un AUC alto no demuestra nada por sí solo. Lo que se comprueba aquí es que el
modelo se compare contra la regla trivial que cualquiera aplicaría sin modelo
("lleva mucho sin comprar, se va a ir") y que la evaluación sea honesta: sobre
un corte posterior al del entrenamiento.
"""

from __future__ import annotations

import random

import pytest

from ecommerce.ml import churn, features

pytestmark = pytest.mark.spark

ESQUEMA = ", ".join(
    [f"{c} double" for c in features.FEATURE_COLUMNS] + ["customer_id string", "churned int"]
)


def cliente(idx: int, *, fuga: bool) -> tuple:
    """Cliente sintético con señal aprendible pero no determinista.

    Los que se van compran menos y hace más tiempo, con solapamiento entre
    ambos grupos: si fueran separables por un umbral, cualquier modelo daría
    AUC 1.0 y el test no distinguiría un modelo bueno de uno tautológico.
    """
    rng = random.Random(idx)
    if fuga:
        recencia = rng.uniform(40, 300)
        frecuencia = rng.uniform(1, 5)
    else:
        recencia = rng.uniform(1, 120)
        frecuencia = rng.uniform(3, 20)
    gasto = frecuencia * rng.uniform(50, 150)
    return (
        recencia,
        frecuencia,
        gasto,
        gasto / frecuencia,
        rng.uniform(30, 900),
        float(rng.randint(0, 3)),
        float(rng.randint(0, 6)),
        gasto / 2,
        rng.uniform(5, 200),
        rng.uniform(0, 20),
        f"CUS-{idx:04d}",
        int(fuga),
    )


@pytest.fixture(scope="module")
def poblacion(spark):
    filas = [cliente(i, fuga=i % 5 < 2) for i in range(400)]
    return spark.createDataFrame(filas, ESQUEMA).cache()


@pytest.fixture(scope="module")
def entrenamiento(poblacion):
    return poblacion.filter("customer_id < 'CUS-0300'")


@pytest.fixture(scope="module")
def prueba(poblacion):
    return poblacion.filter("customer_id >= 'CUS-0300'")


@pytest.fixture(scope="module")
def modelo(entrenamiento):
    return churn.train(entrenamiento, algorithm="logistic_regression")


class TestEntrenamiento:
    def test_los_algoritmos_declarados_se_pueden_entrenar(self, entrenamiento) -> None:
        for nombre in churn.ALGORITHMS:
            assert churn.train(entrenamiento, algorithm=nombre) is not None

    def test_un_algoritmo_desconocido_falla_al_pedirlo(self, entrenamiento) -> None:
        """Antes de gastar minutos entrenando, no después."""
        with pytest.raises(ValueError, match="inexistente"):
            churn.train(entrenamiento, algorithm="inexistente")

    def test_predice_una_probabilidad_por_cliente(self, modelo, prueba) -> None:
        predicciones = churn.predict(modelo, prueba)
        assert predicciones.count() == prueba.count()
        for fila in predicciones.select(churn.SCORE_COLUMN).collect():
            assert 0.0 <= fila[0] <= 1.0

    def test_conserva_el_identificador_para_poder_actuar(self, modelo, prueba) -> None:
        """Una probabilidad sin cliente al lado no sirve para llamar a nadie."""
        assert "customer_id" in churn.predict(modelo, prueba).columns


class TestEvaluacion:
    def test_mide_ambas_areas(self, modelo, prueba) -> None:
        metricas = churn.evaluate(churn.predict(modelo, prueba))
        assert 0.0 <= metricas.auc_roc <= 1.0
        assert 0.0 <= metricas.auc_pr <= 1.0

    def test_informa_el_tamano_y_la_prevalencia(self, modelo, prueba) -> None:
        """Un AUC-PR de 0.4 es excelente con 5 % de positivos y mediocre con
        40 %. Sin la prevalencia al lado, la cifra no se puede interpretar."""
        metricas = churn.evaluate(churn.predict(modelo, prueba))
        assert metricas.n == prueba.count()
        assert metricas.positive_rate == pytest.approx(0.4, abs=0.1)

    def test_el_modelo_supera_a_la_regla_de_recencia(self, modelo, entrenamiento, prueba) -> None:
        """La pregunta que importa: ¿aporta algo sobre lo que ya se sabía?"""
        del entrenamiento
        modelo_metricas = churn.evaluate(churn.predict(modelo, prueba))
        base = churn.evaluate_baseline(prueba)
        assert modelo_metricas.auc_roc > base.auc_roc


class TestSeleccion:
    def test_elige_por_la_metrica_declarada_y_devuelve_todas(self, entrenamiento, prueba) -> None:
        seleccion = churn.select_best(entrenamiento, prueba)
        assert set(seleccion.scores) == set(churn.ALGORITHMS)
        assert seleccion.scores[seleccion.algorithm].auc_pr == max(
            m.auc_pr for m in seleccion.scores.values()
        )

    def test_incluye_la_linea_base_en_la_comparativa(self, entrenamiento, prueba) -> None:
        """Publicar el AUC del modelo sin el de la regla trivial al lado es
        marketing, no evaluación."""
        seleccion = churn.select_best(entrenamiento, prueba)
        assert seleccion.baseline.auc_roc > 0.0


class TestImportancias:
    def test_ordena_las_variables_por_peso(self, modelo) -> None:
        importancias = churn.feature_importances(modelo)
        assert {nombre for nombre, _ in importancias} == set(features.FEATURE_COLUMNS)
        pesos = [abs(peso) for _, peso in importancias]
        assert pesos == sorted(pesos, reverse=True)

    def test_la_recencia_pesa_en_un_modelo_entrenado_con_ella(self, modelo) -> None:
        nombre, _ = churn.feature_importances(modelo)[0]
        assert nombre in {"recency_days", "frequency"}
