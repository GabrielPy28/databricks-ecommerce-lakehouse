# Databricks E-Commerce Lakehouse

Lakehouse end-to-end para una plataforma de e-commerce, construido sobre
Databricks Free Edition con arquitectura Medallion, procesamiento incremental,
calidad de datos como código y orquestación versionada.

No es un notebook que transforma un CSV. Es un pipeline que se ejecuta solo,
se detiene cuando los datos no cumplen lo mínimo, y cuyas cifras se pueden
reconciliar hasta el céntimo con su capa de origen.

```
Generador (contenedor local, sin Spark)
        │  Parquet por lotes
        ▼
UC Volume  ecommerce.landing.raw/<entidad>/batch_NNN/
        │  Auto Loader · trigger availableNow
        ▼
BRONZE   append-only · esquema preservado · metadatos de linaje
        │  try_cast · reglas de calidad · MERGE · SCD2
        ▼
SILVER   claves de negocio · deduplicado · historial · cuarentena
        │  agregación · recarga por ventana
        ▼
GOLD     siete productos de datos orientados a negocio
        │
        ├──▶ Databricks SQL · dashboard AI/BI
        └──▶ Modelo de fuga (job aparte, fuera del camino crítico)
```

---

## Qué problema resuelve

Una plataforma de e-commerce recibe a diario clientes, productos, órdenes,
líneas de pedido, pagos, reseñas, devoluciones y eventos de navegación. Los
datos llegan **sucios, duplicados y fuera de orden**: el sistema de origen
reemite registros, una orden muta de estado varias veces, y algunos registros
aparecen días después de haber ocurrido.

El pipeline tiene que producir cifras en las que el negocio confíe, y tiene que
seguir haciéndolo cuando se reejecuta, cuando llega un dato tardío y cuando un
extracto viene corrupto.

---

## Cómo ejecutarlo

Todo el desarrollo ocurre dentro de un contenedor. No hace falta instalar
Python, Java ni el CLI de Databricks en la máquina.

```bash
cp .env.example .env          # y rellenar DATABRICKS_HOST y DATABRICKS_TOKEN
docker compose build

docker compose run --rm dev pytest                        # 294 tests, sin workspace
docker compose run --rm dev python scripts/run_sql.py sql/00_bootstrap.sql

# Generar y publicar un lote
docker compose run --rm dev python -m ecommerce.generator.cli \
    --profile dev --seed 42 --out data/batches --batch 0
docker compose run --rm dev databricks fs cp -r --overwrite \
    data/batches dbfs:/Volumes/ecommerce/landing/raw

# Desplegar y ejecutar el pipeline
docker compose run --rm dev databricks bundle deploy --target dev
docker compose run --rm dev databricks bundle run ecommerce_pipeline --target dev

# Fases opcionales, cada una en su propio job y a mano
docker compose run --rm dev databricks bundle run churn_model --target dev
docker compose run --rm dev databricks bundle run time_travel_demo --target dev
```

No hace falta indicar el lote: Auto Loader ingiere todos los ficheros nuevos que
encuentre, y cada fila se etiqueta con el lote **de su propia ruta**.

---

## Decisiones que definen el proyecto

### El dato aterriza como texto, no tipado

Los ficheros crudos tienen **todas las columnas como `string`**. No es un
descuido: una columna Parquet tipada como `timestamp` no admite una fecha
inválida, así que un generador que emitiera datos ya tipados haría imposible
reproducir el escenario que Silver debe resolver. `schemas/` define el contrato
lógico tipado; el trabajo de Silver es reconstruirlo.

### `try_cast`, no `cast`

Spark 4 activa el modo ANSI por defecto: un `cast` inválido lanza excepción y
**aborta el job entero**. Un solo registro corrupto entre millones tumbaría el
pipeline. `try_cast` lo convierte en nulo y la cuarentena lo recoge.

### Cuarentena, no descarte

Una fila que falla la conversión o una regla no se descarta —perderla en
silencio es peor que no procesarla— sino que se aparta en
`silver.<entidad>_quarantine` junto con el motivo exacto, y es reprocesable.

### Idempotencia por construcción

Reejecutar el pipeline sobre los mismos datos no cambia nada:

| Mecanismo | Dónde |
|---|---|
| Checkpoint de Auto Loader | Bronze no reingiere ficheros ya procesados |
| `MERGE` con `s.updated_at > t.updated_at` | Silver no duplica ni revierte |
| Reescritura completa de la cuarentena | No se acumulan filas rechazadas |
| Borrado en Silver de lo que hoy se rechaza | Una regla nueva retira lo que ya estaba publicado |
| `replaceWhere` por ventana | Gold reescribe solo lo afectado |

Esa misma condición del `MERGE` resuelve los **datos tardíos**: un registro que
pertenece al pasado pero llega ahora no revierte el estado actual.

### Las reglas de calidad son configuración, no código

51 reglas declaradas en [`rules.yml`](src/ecommerce/quality/rules.yml). Se puede
responder «¿qué comprueba este pipeline?» leyendo un fichero, y añadir una
comprobación sin tocar Python. Tres severidades:

- `warn` — registra y deja pasar
- `quarantine` — aparta la fila con su motivo
- `fail` — **detiene el pipeline**

Una regla puede acotarse a un subconjunto con `where`, y hay casos que no se
pueden expresar de otra forma. `web_events.product_id` admite nulos —ver la
portada es un evento sin producto—, pero los otros tres pasos del embudo no
pueden carecer de él: una compra sin producto es un dato imposible que
contaminaría `product_performance`. La regla comprueba **43.601 eventos** y
deja fuera las 58.399 vistas de portada.

`row_count_delta` es la única regla que mira la tabla entera, y detecta lo que
ninguna regla por fila puede ver: que **falten** datos. Una fila ausente no
incumple nada —simplemente no está—, así que un pipeline que un día ingiere una
fracción de lo habitual pasa todas las validaciones y publica cifras
silenciosamente incompletas. La línea base sale del propio histórico de
`ops.quality_results`:

```
10,200 filas, sin línea base con la que comparar      ← primera ejecución
10,200 -> 10,200 filas (+0.0%, umbral ±30%)           ← la siguiente
```

La puerta de calidad es una tarea aparte del DAG, y `build_gold` depende de
ella y no de Silver. Si la calidad falla, Gold no llega a publicar cifras
construidas sobre datos que no cumplen lo mínimo.

### SCD Tipo 2 donde cambia el significado

`customers` y `products` guardan historial. No es adorno: `order_items.unit_price`
ya preserva el precio de venta, pero el **coste** y el **país** no. Sin
historial, un cliente que se muda reescribe retroactivamente los ingresos de dos
países, y un cambio de precio de compra inventa margen en todo el pasado.

Gold une cada hecho con la versión de la dimensión **vigente en la fecha del
hecho**, no con la actual.

### El pipeline y el dashboard son código

El DAG, sus parámetros, su entorno y su planificación viven en
[`databricks.yml`](databricks.yml). El dashboard, en
[`dashboards/`](dashboards/). Ambos se revisan en un pull request y se
reconstruyen si alguien los borra.

El código de `src/` viaja a los jobs como **wheel** que el bundle construye e
instala; `src/` se excluye del sincronizado al workspace para que ningún
notebook pueda importar por accidente una copia sin versionar.

### El modelo se evalúa contra la alternativa de no tenerlo

Un modelo de churn es fácil de hacer y fácil de hacer mal: si las variables se
calculan sobre todo el histórico, contienen la respuesta y el AUC sube a 0,99
sin que el modelo prediga nada. Aquí las variables solo miran datos hasta un
corte, la etiqueta solo lo posterior, y la evaluación ocurre en un corte
distinto del de entrenamiento.

Y cada métrica se publica junto a la de la **regla trivial** que el negocio ya
aplica sin modelo. La cifra que importa no es 0,789: es la diferencia con
0,683.

---

## Resultados medidos

Todas las cifras provienen de ejecuciones reales sobre Databricks Free Edition,
perfil `dev`, dos lotes (carga inicial más un incremental).

### Trazabilidad completa de un lote

```
Bronze crudas        10.530   ← lo que llegó, preservado tal cual
Bronze distintas     10.200   ← 30 duplicados y 300 mutaciones de estado
Silver válidas       10.180
Silver cuarentena        20   ← 10 fechas no parseables + 10 clientes inexistentes
```

**10.180 + 20 = 10.200.** La suma cuadra exactamente y ninguna orden aparece a
la vez como válida y en cuarentena. Cada fila está contabilizada; ninguna se
perdió en silencio.

### Reconciliación Gold ↔ Silver

| Gold | Silver | Diferencia |
|---|---|---|
| 1.233.217,10 | 1.233.217,10 | **0,00** |

El agregado se puede recalcular desde su origen y da lo mismo. Sin eso, un
dashboard es una afirmación sin respaldo.

### Tiempos del pipeline orquestado

| Tarea | Duración |
|---|---|
| `ingest_bronze` | 151 s |
| `build_silver` | 189 s |
| `quality_gate` | 22 s |
| `build_gold` | 122 s |
| **Total** | **484 s** |

Carga completa de los dos lotes sobre cómputo serverless. Los tiempos varían
entre ejecuciones según el estado del almacén.

### Generación de datos

| Perfil | Filas | Tiempo |
|---|---|---|
| `dev` | 144.000 | < 2 s |
| `demo` | 7.180.000 | 27 s |

Faker construye repertorios de nombres y marcas; numpy vectorizado produce los
hechos. Faker fila a fila sobre millones de órdenes costaría horas.

### Calidad de datos

51 reglas sobre 8 tablas. En la última ejecución, **69 incumplimientos**, todos
trazables a una anomalía inyectada a propósito:

| Regla | Filas | Origen |
|---|---|---|
| `orders.customer_id` foreign_key | 10 | Huérfanos inyectados |
| `orders.order_date` not_null | 10 | Fechas no parseables |
| `order_items.quantity` min_value | 10 | Cantidades no positivas |
| `order_items.unit_price` min_value | 4 | Precios negativos |
| `customers.email` matches (aviso) | 5 | Correos malformados |
| `order_items.order_id` foreign_key | 17 | **Cascada** |
| `payments.order_id` foreign_key | 10 | **Cascada** |
| `reviews.order_id` foreign_key | 3 | **Cascada** |

Las tres últimas son un efecto emergente: al apartar 20 órdenes, sus líneas,
pagos y reseñas quedaron huérfanos. Es lo que pasa de verdad al rechazar un
registro padre, y ahora está visible en vez de oculto.

### Historial de dimensiones

```
customers   1.018 versiones   1.000 vigentes
products      206 versiones     200 vigentes
```

Intervalos sin huecos ni solapes: el `valid_to` de una versión es exactamente el
`valid_from` de la siguiente.

### Rendimiento: un error de diseño, medido y corregido

La primera versión particionaba las tablas Gold por su columna de fecha. Medir
el layout físico con `DESCRIBE DETAIL` dejó claro que era un error:

| Tabla | Ficheros antes | Después | Tamaño antes | Después |
|---|---|---|---|---|
| `daily_sales` | **698** | **1** | 1,60 MB | 10,8 KB |
| `marketing_funnel` | **721** | **1** | ~2,4 MB | 34,3 KB |
| `category_performance` | 25 | 1 | ~60 KB | 4,5 KB |
| **Total** | **1.444** | **3** | | |

`daily_sales` tenía **una fila por fichero**: 698 días, 698 particiones, ficheros
de 2,2 KB. Una partición por día sobre una tabla de agregados diarios crea una
partición por fila.

**`OPTIMIZE` no lo arreglaba**, y ese fue el hallazgo más útil: compacta *dentro*
de cada partición, y con un solo fichero por partición no tiene nada que hacer.
Ni siquiera dejaba entrada en el historial de la tabla. El problema no era falta
de compactación sino sobre-particionado.

La corrección es **liquid clustering**: agrupa por la misma columna sin trocear
el almacenamiento, así que el salto de datos sigue funcionando y los ficheros
tienen tamaño razonable. `replaceWhere` sigue siendo válido porque opera sobre
columnas de datos, no solo de partición.

La caída de tamaño —de 1,60 MB a 10,8 KB en `daily_sales`— no es magia: con una
fila por fichero, el pie de página de Parquet, el esquema y las estadísticas por
columna pesan más que los datos. Con 698 filas juntas, la compresión columnar
por fin tiene material con el que trabajar.

#### Por qué se mide el layout y no el tiempo

Dos intentos anteriores de medir con `read_bytes` fracasaron, y vale la pena
contar por qué:

1. La **caché de disco** del warehouse devolvía 0 bytes leídos tras `OPTIMIZE`
   —que deja calientes los ficheros que acaba de escribir— y declaraba una
   mejora del 100 % que no existía.
2. Reiniciando el warehouse para vaciarla, las columnas `read_bytes` y
   `read_files` de `system.query.history` seguían a cero: se enriquecen con un
   retraso mayor del que el script podía esperar.

El layout físico no depende de cachés ni de latencias. Es la medida que se
publica. Los tiempos de reloj se omiten a propósito: en serverless miden sobre
todo el estado del almacén.

### Recuperación con time travel

[`notebooks/99_time_travel_demo.py`](notebooks/99_time_travel_demo.py) no narra el
escenario: lo ejecuta. Rompe `gold.daily_sales` con un error plausible —un factor
aplicado a los ingresos de un rango de fechas—, comprueba que **el job termina
con éxito** publicando la cifra inflada, cuantifica el daño y recupera.

| Versión | Estado | Ingreso |
|---|---|---|
| 11 | Correcta | 1.233.217,10 |
| 12 | Rota por el `UPDATE` | **1.326.096,40** |
| 14 | Restaurada | 1.233.217,10 |

**244 días afectados**, y ninguna excepción: solo una cifra que no cuadra. El
`assert` final verifica la recuperación, y la reconciliación con Silver vuelve a
dar 0,00.

`RESTORE` **añade** una versión en lugar de borrar el historial, así que la v12
sigue ahí: se puede auditar qué se publicó y durante cuánto tiempo.

Dos detalles que la demo aprendió por las malas:

- En Free Edition con Default Storage, `DESCRIBE DETAIL` devuelve `location`
  vacío. El time travel por ruta falla; hay que usar `VERSION AS OF` por nombre.
- La primera versión repartía romper y restaurar en celdas distintas. Una falló
  en medio y **dejó la tabla inflada**; la siguiente ejecución tomó ese estado
  como «el bueno». Ahora la parte destructiva está en un `try`/`finally` y el
  notebook arranca reconciliando contra Silver: se niega a ejecutarse sobre una
  tabla ya corrupta.

### Embudo de marketing

```
page_view        58.399   100,00 %
add_to_cart      26.461    45,31 %
checkout_start   12.413    21,25 %
purchase          4.727     8,09 %
```

### Predicción de fuga

Fase opcional, en un job aparte: **consume** Gold y no está en el camino
crítico. Si el modelo falla, las tablas de negocio se siguen publicando.

El riesgo de un modelo de churn no es predecir mal, sino predecir *demasiado
bien*. Definir la fuga como «sin compras en 90 días» y alimentar el modelo con
`days_since_last_order` calculado sobre todo el histórico da un AUC de 0,99 y
un modelo inútil: se le ha dado la respuesta. Aquí las variables se calculan
**solo con datos hasta un corte** y la etiqueta mira **estrictamente después**,
con dos cortes para que la validación sea **fuera de tiempo**:

```
variables (≤ 2026-03-05) → etiqueta (2026-03-05 .. 2026-06-03]   entrenamiento
                           variables (≤ 2026-06-03) → etiqueta (.. 2026-09-01]   prueba
```

El test que sostiene todo esto no comprueba una cifra: añade actividad
posterior al corte y exige que **no cambie ni una variable**.

Resultado sobre 896 clientes de prueba, 36,8 % en fuga:

| Modelo | AUC-ROC | AUC-PR |
|---|---:|---:|
| **Regresión logística** (elegido) | **0,789** | **0,638** |
| Random forest | 0,789 | 0,629 |
| Gradient boosting | 0,779 | 0,620 |
| *Línea base: regla de recencia* | *0,683* | *0,544* |

La línea base no es decorativa: es lo que el negocio ya sabe hacer sin modelo
(«lleva mucho sin comprar, se va a ir»). Publicar 0,789 sin ese 0,683 al lado
es marketing, no evaluación. La mejora real es **+0,106 de AUC-ROC**.

Las 914 predicciones accionables se calculan en el último corte disponible
—clientes cuyos 90 días aún no han pasado, y que por tanto **no tienen
etiqueta**— y se agrupan por decil de riesgo:

| Tramo | Clientes | Prob. media | Recencia media | Órdenes | Gasto medio |
|---|---:|---:|---:|---:|---:|
| alto (decil superior) | 92 | 0,656 | 323 d | 1,0 | 166 |
| medio (20 % siguiente) | 182 | 0,496 | 145 d | 2,2 | 302 |
| bajo | 640 | 0,139 | 53 d | 12,6 | 1.817 |

Dos cosas que la tabla deja ver y conviene no maquillar:

- **Los tramos son por decil, no por umbral absoluto.** La primera versión
  cortaba en 0,7 y dejó 3 clientes en «alto» de 914: con una prevalencia del
  37 % el modelo casi nunca supera esa cifra. Un equipo de retención tiene
  capacidad fija y llama a los N más probables que pueda atender.
- **Riesgo y valor van en direcciones opuestas.** El decil de mayor riesgo
  concentra 15.275 de gasto histórico sobre un total de 1.233.217: un 1,2 %.
  Son compradores de una sola orden. «Llamar a los más probables» y «proteger
  más ingreso» no son la misma lista, y una campaña que ignore eso gastará su
  presupuesto en los clientes más baratos de perder.

Las métricas se guardan en `ops.model_metrics` con sus cortes y su prevalencia.
Un número que solo vive en la salida de un cuaderno se pierde en cuanto alguien
vuelve a ejecutarlo.

---

## Tests

294 tests que corren **sin conexión a Databricks**, en el mismo contenedor que
CI usa.

| Suite | Qué protege |
|---|---|
| `test_environment` | Que la imagen tenga Python 3.12, Java, Spark, Delta y el CLI moderno |
| `test_config`, `test_schemas` | Contrato de datos, perfiles de escala |
| `test_generator`, `test_incremental` | Determinismo, integridad referencial, mutaciones, datos tardíos |
| `test_ingestion` | Opciones de Auto Loader y metadatos de linaje |
| `test_transformations`, `test_scd2` | Tipado, cuarentena, MERGE idempotente, historial |
| `test_quality` | Cada tipo de regla acierta y falla cuando debe |
| `test_analytics`, `test_gold_tables` | Las cifras de negocio, calculadas a mano |
| `test_bundle` | El DAG, el dashboard y sus referencias |
| `test_ml_features`, `test_ml_churn` | Ausencia de fuga de etiqueta, y que el modelo supere a la regla trivial |

Las transformaciones viven en `src/` como funciones puras `DataFrame → DataFrame`
y los notebooks son **drivers finos** que solo las encadenan. Sin esa
separación, `tests/` sería decorativo.

---

## Estructura

```
src/ecommerce/
├── config.py              perfiles de escala, namespace
├── schemas/               contrato de datos (fuente de verdad)
├── generator/             Faker + numpy, mutaciones, suciedad controlada
├── ingestion/             Auto Loader → Bronze
├── transformations/       tipado, MERGE, SCD2, cuarentena
├── quality/               motor de reglas + rules.yml
├── analytics/             constructores de Gold
└── ml/                    variables y modelo de fuga (fase opcional)

notebooks/                 drivers finos (00..05, 99 time travel)
dashboards/                dashboard AI/BI como código
sql/                       bootstrap del namespace y consultas analíticas
scripts/                   utilidades: SQL, resumen de ejecución, medición
tests/                     294 tests
docs/                      arquitectura, modelo de datos, decisiones
```

---

## Tecnologías

| Pieza | Uso |
|---|---|
| Databricks Free Edition | Plataforma, cómputo serverless |
| PySpark 4.1.1 | Procesamiento distribuido |
| Delta Lake 4.3.1 | ACID, time travel, `MERGE`, `replaceWhere` |
| Unity Catalog | Catálogo, esquemas y Volumes |
| Auto Loader | Ingesta incremental idempotente |
| Databricks Workflows | Orquestación del DAG |
| Databricks Asset Bundles | Infraestructura como código |
| Databricks SQL | Warehouse y dashboard AI/BI |
| scikit-learn | Modelo de fuga (fase opcional) |
| Faker + numpy | Generación de datos sintéticos |
| Docker | Entorno reproducible |
| pytest + ruff + GitHub Actions | Tests y CI |

---

## Alcance y limitaciones

Decisiones deliberadas, no omisiones:

- **Divisa única (USD).** `country` se conserva para segmentación; no hay
  conversión FX.
- **Sin streaming continuo.** Auto Loader con `availableNow` da la semántica de
  streaming sin consumo permanente de cuota.
- **`lifetime_value` es gasto histórico, no una predicción.** Un CLV predictivo
  necesita un modelo; llamar «valor de vida» a un modelo inexistente produce
  cifras que nadie puede defender. Lo que sí es una predicción está en
  `gold.customer_churn_predictions`, y viene con su evaluación al lado.
- **El modelo no se registra ni se sirve.** No hay MLflow Model Registry ni
  endpoint de inferencia: la fase de ML existe para demostrar que la capa Gold
  alimenta un caso de uso real, no para montar una plataforma de MLOps.
- **Rendimiento medido en bytes y ficheros leídos, no en tiempo de reloj.** En
  serverless no se controla el cómputo, así que el tiempo mide sobre todo el
  ruido de la plataforma.
- **Datos 100 % sintéticos.** Ninguna información personal real.
