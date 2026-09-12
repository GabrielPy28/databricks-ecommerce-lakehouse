"""Predicción de fuga de clientes.

Fase opcional del proyecto. El eje sigue siendo la ingeniería de datos: este
módulo consume `gold` y no altera el pipeline.

**El riesgo de un modelo de churn no es predecir mal, sino predecir demasiado
bien.** Definir la fuga como "sin compras en 90 días" y alimentar el modelo con
`days_since_last_order` produce un clasificador casi perfecto y del todo
inútil: la etiqueta se coló entre las variables.

Aquí las variables se calculan **solo con datos hasta un corte** y la etiqueta
mira **estrictamente después**. Y se usan dos cortes, uno para entrenar y otro
posterior para evaluar, de modo que la validación sea fuera de tiempo y no un
reparto aleatorio del mismo periodo.
"""
