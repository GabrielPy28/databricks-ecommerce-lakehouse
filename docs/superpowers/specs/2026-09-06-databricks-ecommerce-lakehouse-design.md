# Databricks E-Commerce Lakehouse — Diseño

**Fecha:** 2026-09-06
**Estado:** Propuesto
**Autor:** Gabriel P.

---

## 1. Contexto y objetivo

Proyecto de portafolio orientado a **Data Engineer / Databricks Specialist**. El objetivo es demostrar criterio de ingeniería: contratos de datos explícitos, procesamiento incremental, idempotencia, calidad de datos, infraestructura versionada y decisiones arquitectónicas justificables en una entrevista.

Se construye un **Lakehouse end-to-end** para una plataforma de e-commerce sintética, siguiendo la arquitectura Medallion (Bronze → Silver → Gold) sobre Databricks Free Edition.

### Criterios de éxito

1. El pipeline corre de punta a punta sin intervención manual, orquestado por Databricks Workflows definidos como código.
2. Ejecutarlo dos veces sobre el mismo lote **no altera el resultado** (idempotencia demostrable).
3. Un lote nuevo procesa solo los datos nuevos y las mutaciones, no el histórico completo.
4. Las tablas Gold responden preguntas de negocio concretas, consultables desde Databricks SQL.
5. Las métricas del README son **medidas**, no estimadas.
6. Un tercero clona el repo, levanta Docker y ejecuta los tests sin instalar nada más.

### Fuera de alcance (decisiones deliberadas)

| Excluido | Razón |
|---|---|
| Multi-divisa y conversión FX | Todo en USD. Añade complejidad sin demostrar habilidad nueva. `country` se conserva para segmentación geográfica. |
| Streaming continuo (24/7) | Auto Loader con trigger `availableNow`: da la semántica de streaming sin consumo continuo de cuota. |
| Características enterprise de Unity Catalog | Free Edition no las expone (lineage entre workspaces, delta sharing, control de acceso por filas). Se documentan conceptualmente. |
| PII real | Datos 100 % sintéticos. |
| ML como componente central | Fase final. El eje del proyecto es Data Engineering. |

---

## 2. Entorno de desarrollo

### Decisión: contenedor Docker como entorno único

**Contexto:** la máquina de desarrollo es Windows 11 con Python 3.13 y sin JDK. PySpark no soporta bien Python 3.13, requiere Java, y en Windows nativo exige `winutils.exe` y `hadoop.dll` configurados a mano — una fuente conocida de fallos difíciles de diagnosticar.

**Decisión:** todo el desarrollo local ocurre dentro de un contenedor Linux.

**Consecuencias:** el entorno queda versionado y reproducible; se elimina la clase entera de problemas de Spark en Windows; se fija Python 3.12 para coincidir con el runtime de Databricks. Sin embargo, Docker Desktop debe estar arrancado y el montaje del disco vía WSL2 es algo más lento que el acceso nativo (irrelevante a esta escala).

### Composición de la imagen

- Base `python:3.12-slim-bookworm`
- JRE 17 headless — PySpark necesita runtime de Java, no el JDK completo
- **Databricks CLI moderno**, instalado como binario oficial. `pip install databricks-cli` instala el CLI antiguo de Python, que **no soporta Asset Bundles**; es un error fácil de cometer y difícil de detectar.
- `JAVA_HOME` y `SPARK_LOCAL_IP=127.0.0.1` fijados en la imagen para evitar bloqueos de resolución de nombres dentro del contenedor.

### Versiones fijadas

| Paquete | Versión | Nota |
|---|---|---|
| Python | 3.12 | Coincide con el runtime de Databricks |
| `pyspark` | 4.2.0 | |
| `delta-spark` | 4.4.0 | Declara `pyspark>=4.0.1,<=4.2.0` — pareja verificada en PyPI |
| `faker`, `numpy`, `pyarrow`, `pandas` | fijadas | Generador de datos |
| `pytest`, `ruff` | fijadas | Desarrollo |

> **Resuelto en Fase 0.** El runtime más alto disponible en el workspace es **DBR 18.2, con Spark 4.1.0**. El par local se fijó en `pyspark==4.1.1` + `delta-spark==4.3.1`, la versión más cercana sin pasarse.
>
> La dirección del desajuste importa: probar en local contra un Spark *más nuevo* que el de destino permite usar APIs inexistentes en Databricks, y el fallo no aparece hasta el despliegue. Al revés es seguro. `test_spark_arranca` afirma explícitamente `4.1.x` para que cualquier actualización futura por delante de Databricks se detecte en CI.
>
> El SQL Warehouse informa `dbsql_version 2026.20` y `dbr_version` nulo: es un motor DBSQL, no un clúster, y no expone versión de Spark. La versión exacta del cómputo serverless de notebooks se confirmará en Fase 2, al ejecutar PySpark real sobre él.

### Interfaces

Un único `Dockerfile` consumido por dos vías:

- `.devcontainer/devcontainer.json` — abrir el repo dentro del contenedor desde VS Code
- `docker-compose.yml` — ejecutar comandos puntuales (`docker compose run --rm dev pytest`)

**Credenciales:** archivo `.env` (en `.gitignore`) con `DATABRICKS_HOST` y `DATABRICKS_TOKEN`, inyectado por Compose. Se versiona `.env.example` documentando las variables requeridas. Ningún secreto entra al repositorio.

---

## 3. Arquitectura

```
Generador (contenedor local, sin Spark)
        │  Parquet por lotes
        ▼
UC Volume  ecommerce.landing.raw/<entidad>/batch_NNN/
        │  Auto Loader (trigger availableNow)
        ▼
BRONZE   append-only · esquema preservado · metadatos de linaje
        │  tipado · validación · MERGE
        ▼
SILVER   claves de negocio · deduplicado · SCD2 · cuarentena
        │  agregación
        ▼
GOLD     productos de datos orientados a negocio
        │
        ├──▶ Databricks SQL · dashboard
        └──▶ ML (fase opcional, job aparte): predicción de churn
```

### Namespace en Unity Catalog

```
ecommerce
├── landing   (Volume `raw` — zona de aterrizaje de ficheros)
├── bronze
├── silver
├── gold
└── ops       (quality_results, model_metrics)
```

> **Resuelto en Fase 0.** Free Edition **sí** permite crear un catálogo propio, pero solo por la ruta SQL. El API (`databricks catalogs create`) falla con `Metastore storage root URL does not exist`, porque la cuenta tiene Default Storage activado y el API exige una `MANAGED LOCATION` explícita; `CREATE CATALOG` en SQL resuelve el almacenamiento por defecto sin más.
>
> El namespace completo (catálogo, 5 esquemas y el Volume `landing.raw`) se crea con `sql/00_bootstrap.sql`, ejecutado por `scripts/run_sql.py`. Es DDL idempotente y versionado, verificado ejecutándolo dos veces.
>
> Aun así, el nombre del catálogo se lee de configuración y nunca se codifica en el código, para que reapuntar a otro catálogo siga siendo un cambio de una línea.

---

## 4. Contrato de datos

Los esquemas se declaran una sola vez en `src/ecommerce/schemas/` y son la fuente de verdad compartida por el generador, la ingesta y los tests. El documento original tenía huecos (métricas de Gold sin campo de origen); estos esquemas los cierran.

### Dimensiones

**`customers`** — `customer_id` (PK), `first_name`, `last_name`, `email`, `country` (ISO-2), `city`, `registration_date`, `updated_at`

**`products`** — `product_id` (PK), `product_name`, `category`, `subcategory`, `brand`, `price`, `cost`, `is_active`, `updated_at`

### Hechos

**`orders`** — `order_id` (PK), `customer_id` (FK), `order_date`, `status`, `payment_method`, `shipping_country`, `gross_amount`, `discount_amount`, `total_amount`, `created_at`, **`updated_at`**

> `total_amount` y `discount_amount` faltaban pese a aparecer en las reglas de calidad y en `gold.daily_sales`. **`updated_at` es el campo crítico del proyecto**: es lo que permite el `MERGE` con desempate determinista y el tratamiento de datos que llegan tarde.

**`order_items`** — `order_item_id` (PK), `order_id` (FK), `product_id` (FK), `quantity`, `unit_price`, `discount_amount`, `line_total`

**`payments`** — `payment_id` (PK), `order_id` (FK), `payment_method`, `amount`, `payment_status`, `payment_date`

**`returns`** — `return_id` (PK), `order_id` (FK), `order_item_id` (FK), `product_id` (FK), `quantity`, `reason`, `refund_amount`, `return_date`

**`reviews`** — `review_id` (PK), `order_id` (FK), `product_id` (FK), `customer_id` (FK), `rating` (1-5), `review_date`

**`web_events`** — `event_id` (PK), `event_timestamp`, `session_id`, `customer_id` (nullable: sesiones anónimas), `event_type` (`page_view` / `add_to_cart` / `checkout_start` / `purchase`), `product_id` (nullable), `device`, `utm_source`

### Invariantes del modelo

- `orders.total_amount = gross_amount - discount_amount`
- `orders.gross_amount = SUM(order_items.line_total + order_items.discount_amount)`
- `order_items.line_total = quantity * unit_price - discount_amount`
- `order_items.unit_price` es el precio **en el momento de la venta**; nunca se recalcula desde `products.price`
- Toda mutación de negocio incrementa `updated_at`

---

## 5. Generador de datos

### Estrategia híbrida

Faker es Python puro y ronda las decenas de miles de filas por segundo: generar millones de órdenes tardaría horas. La estrategia:

- **Dimensiones** (miles de filas): Faker, con semilla fija. Nombres, correos y ciudades realistas.
- **Hechos** (millones de filas): `numpy` vectorizado. Fechas, cantidades y precios como arrays; escritura a Parquet por lotes con `pyarrow`.

Todo determinista: la misma semilla produce el mismo dataset. Es lo que hace reproducible el reporte de calidad y explicables sus cifras.

### Perfiles de escala

Un único parámetro gobierna el proyecto entero y protege la cuota de cómputo de Free Edition.

| Perfil | Clientes | Productos | Órdenes | Web events | Uso |
|---|---|---|---|---|---|
| `dev` | 1.000 | 200 | 10.000 | 100.000 | Iteración y tests |
| `demo` | 50.000 | 2.000 | 500.000 | 5.000.000 | Ejecuciones normales |
| `full` | 200.000 | 5.000 | 5.000.000 | 50.000.000 | Una sola corrida para las métricas del README |

### Salida por lotes

Desde el principio se escribe en lotes (`batch_000/`, `batch_001/`, …), no un volcado único, porque es lo que después alimenta Auto Loader y el procesamiento incremental sin reescribir nada.

Cada lote incluye un **manifiesto JSON**: semilla, perfil, número de filas por entidad, ventana temporal y tasas de anomalía aplicadas.

### Mutaciones entre lotes

El generador no solo añade filas nuevas: también **muta filas existentes**, que es lo que hace realista el pipeline.

- Órdenes que avanzan de estado (`pending → paid → shipped → delivered`), reemitidas con `updated_at` actualizado
- Devoluciones que llegan días después de la orden original
- Cambios en dimensiones (cliente que cambia de país, producto que cambia de precio) → alimentan SCD Tipo 2
- Un porcentaje de registros **llega tarde**: pertenecen a un lote anterior pero aparecen en el actual

### Anomalías controladas

Nada de aleatoriedad opaca. Cada tasa es configurable y queda registrada en el manifiesto:

| Anomalía | Tasa |
|---|---|
| Duplicados exactos de órdenes | 0,30 % |
| `customer_id` huérfano | 0,10 % |
| `quantity <= 0` | 0,05 % |
| Precio negativo | 0,02 % |
| Timestamp nulo o fuera de rango | 0,10 % |
| `status` con mayúsculas/espacios inconsistentes | 2,00 % |
| Correo con formato inválido | 0,50 % |

Cuando el reporte diga "2.314 registros en cuarentena", el origen de cada uno es explicable. Esa es exactamente la pregunta que se hace en entrevista.

---

## 6. Bronze — preservar lo que llegó

**Principio:** ninguna transformación de negocio. Todo llega como texto o con el tipo de origen; los errores de tipo se resuelven en Silver, no aquí.

- Ingesta con **Auto Loader** desde el Volume, trigger `availableNow` (semántica de streaming, consumo acotado)
- Tablas append-only, particionadas por fecha de ingesta
- `cloudFiles.schemaEvolutionMode = addNewColumns` y columna `_rescued_data` para capturar campos inesperados sin perderlos ni romper el job

**Metadatos de linaje** añadidos a cada fila: `_ingestion_timestamp`, `_source_file`, `_batch_id`, `_pipeline_run_id`.

**Idempotencia:** el checkpoint de Auto Loader garantiza que un fichero ya procesado no se reprocesa. Se demuestra con un test explícito: ejecutar dos veces el mismo lote y verificar que el conteo no cambia.

---

## 7. Silver — validar, conformar, historizar

### Tipado y normalización

Conversión explícita a `timestamp`, `decimal(18,2)` para importes (nunca `float`, por precisión), `int` para cantidades. Normalización de `status` (recorte de espacios, minúsculas) y de correos.

### Carga idempotente por `MERGE`

Los hechos se cargan con `MERGE INTO` sobre la clave de negocio, con desempate por `updated_at`: si llega una versión más reciente de una orden, se actualiza; si llega una más antigua (dato tardío), **se ignora**. Esto es lo que hace el pipeline correcto ante reprocesos y llegadas fuera de orden.

### SCD Tipo 2 en dimensiones

`customers` y `products` mantienen historial con `valid_from`, `valid_to`, `is_current`. Un cambio de país o de precio cierra el registro vigente y abre uno nuevo.

**Justificación:** `order_items.unit_price` preserva el precio de venta, así que los ingresos históricos ya son correctos sin SCD2. Pero el análisis por segmento sí lo necesita: sin historial, un cliente que se mudó de México a España reescribe retroactivamente todos los ingresos históricos de ambos países.

### Cuarentena

Las filas que fallan una regla bloqueante no se descartan ni contaminan la tabla principal: van a `<tabla>_quarantine` junto con la regla incumplida, el valor ofensor y el `_pipeline_run_id`. Son reprocesables una vez corregida la causa.

---

## 8. Calidad de datos — motor dirigido por configuración

**Decisión:** un motor de reglas configurable en vez de comprobaciones escritas a mano tabla por tabla.

**Alternativa descartada:** validaciones ad-hoc en cada notebook. Funciona, pero escala mal, duplica lógica y se lee como código junior.

**Alternativa descartada:** Great Expectations. Potente, pero añade una dependencia pesada y oscurece la habilidad que se quiere demostrar. Escribir el motor *es* la demostración.

### Diseño

Reglas declaradas en `src/ecommerce/quality/rules.yml`:

```yaml
silver.orders:
  - {column: order_id,     rule: not_null,     severity: quarantine}
  - {column: order_id,     rule: unique,       severity: quarantine}
  - {column: customer_id,  rule: foreign_key,  ref: silver.customers, severity: quarantine}
  - {column: status,       rule: in_set,       values: [pending, paid, shipped, delivered, cancelled, returned]}
  - {column: total_amount, rule: min_value,    value: 0, severity: quarantine}
  - {table: orders,        rule: row_count_delta, max_pct: 50, severity: warn}
```

Tres niveles de severidad: `warn` (registra y continúa), `quarantine` (aparta la fila, continúa), `fail` (aborta el pipeline). Un único ejecutor los aplica a todas las tablas.

**Salidas:** tabla `ops.quality_results` (histórico consultable, permite graficar la calidad en el tiempo) y un reporte legible por ejecución.

Complementariamente, las invariantes duras se declaran como `CONSTRAINT` de Delta a nivel de tabla, para que la base de datos las imponga por sí misma.

---

## 9. Gold — productos de datos

| Tabla | Grano | Responde |
|---|---|---|
| `daily_sales` | día | Ingresos, órdenes, ticket medio, descuentos, tendencia |
| `product_performance` | producto | Unidades, ingresos, margen, tasa de devolución |
| `category_performance` | categoría × mes | Qué categorías crecen |
| `customer_lifetime_value` | cliente | Gasto total, frecuencia, recencia, CLV |
| `customer_segments` | cliente | Segmentación RFM (campeones, en riesgo, perdidos…) |
| `marketing_funnel` | día × canal | Conversión `page_view → add_to_cart → checkout → purchase` |
| `customer_churn_predictions` | cliente | Probabilidad de fuga y tramo de riesgo (fase opcional) |

Cada tabla lleva comentarios de columna en Unity Catalog: es la capa que consume negocio y debe ser autoexplicativa.

> **Resuelto en Fase 8.** El diseño preveía una tabla `customer_features` como insumo del modelo. No se construyó, y el motivo es el que hace correcto al modelo: **materializar variables sin un corte asociado es exactamente el mecanismo de la fuga de etiqueta.** Una tabla de variables «actuales» invita a entrenar con ellas contra una etiqueta futura, y produce un AUC de 0,99 que no significa nada.
>
> Las variables se calculan por tanto en función de un corte (`ecommerce.ml.features`), y lo que se persiste es el resultado: `gold.customer_churn_predictions`. La evaluación va aparte, en `ops.model_metrics`, con los dos cortes, la prevalencia y la línea base junto a cada cifra.

**Recarga:** incremental por ventana. Solo se recalculan las particiones afectadas por el lote (más una ventana de retraso para datos tardíos), no la tabla completa.

---

## 10. Orquestación

**Decisión:** Databricks Asset Bundles (`databricks.yml`), no configuración manual en la interfaz.

**Justificación:** un Workflow creado clicando en la UI no deja evidencia en GitHub. Con Bundles, jobs, dependencias, parámetros y planificación viven versionados, revisables en un pull request, y `databricks bundle validate` entra en CI. Es la práctica actual en producción.

```
generate_and_land  →  ingest_bronze  →  build_silver  →  quality_gate  →  build_gold
                                                              │
                                                              └─ (fail) → notifica y detiene
```

Con `targets` para `dev` y `prod` que difieren en el perfil de escala y en el catálogo destino.

---

## 11. Rendimiento

**Restricción honesta:** en Free Edition el cómputo es serverless. No se elige tamaño de clúster ni se ajustan la mayoría de configuraciones de Spark. La comparativa "4m32s → 1m47s" del documento original **no es demostrable por esa vía**.

Lo que sí es medible y real:

- `OPTIMIZE` y clustering sobre las columnas de filtrado más frecuentes → **bytes escaneados**, antes y después
- Poda de particiones y proyección temprana de columnas → bytes escaneados
- `broadcast` explícito en joins de hecho contra dimensión → volumen de shuffle
- Comparación de planes con `EXPLAIN` antes y después

**Regla de integridad:** toda cifra del README procede de una ejecución real. Ningún número estimado. Si no se pudo medir, no se publica.

---

## 12. Testing y CI

**Principio estructural:** la lógica de transformación vive en `src/` como funciones puras `DataFrame → DataFrame`. Los notebooks son *drivers finos* que solo orquestan llamadas. Sin esta separación, `tests/` acaba siendo decorativo y la lógica se duplica entre notebooks y librería.

| Nivel | Qué verifica | Dónde |
|---|---|---|
| Generador | Determinismo por semilla, integridad referencial, tasas de anomalía en tolerancia, cuadre de totales | Local, sin Spark, segundos |
| Transformaciones | Cada función con casos límite: nulos, duplicados, datos tardíos, transiciones SCD2 | Local, Spark en contenedor |
| Calidad | Cada tipo de regla acierta y falla cuando debe | Local, Spark en contenedor |
| Integración | Pipeline completo en perfil `dev` | Databricks, manual |

**CI (GitHub Actions):** `ruff` + `pytest` + `databricks bundle validate` en cada push. Sin credenciales de Databricks en CI: todo lo que corre allí es local.

---

## 13. Estructura del repositorio

```
databricks-ecommerce-lakehouse/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── .devcontainer/devcontainer.json
├── .env.example
├── databricks.yml                  # Asset Bundle
├── pyproject.toml                  # ruff + pytest
├── requirements.txt / requirements-dev.txt
│
├── src/ecommerce/
│   ├── config.py                   # perfiles de escala, catálogo, esquemas
│   ├── schemas/                    # contrato de datos (fuente de verdad)
│   ├── generator/                  # dimensions, facts, mutations, dirt, cli
│   ├── ingestion/                  # Auto Loader → Bronze
│   ├── transformations/            # Silver: tipado, MERGE, SCD2
│   ├── quality/                    # motor de reglas + rules.yml
│   └── analytics/                  # constructores de Gold
│
├── scripts/
│   └── run_sql.py                  # ejecuta ficheros .sql contra el warehouse
│
├── notebooks/                      # drivers finos (01..05)
├── sql/                            # bootstrap del namespace + consultas
├── tests/
├── data/sample/                    # muestra pequeña para reproducibilidad
└── docs/
    ├── architecture.md
    ├── data_model.md               # ERD y diccionario
    └── decisions.md                # ADRs
```

---

## 14. Plan de entrega por fases

Se construye un **corte vertical** primero, no capa por capa. Con ocho entidades, completar Bronze entero significaría semanas sin una sola métrica funcionando, y todos los riesgos de integración (permisos de UC, checkpoints, comportamiento de `MERGE` en serverless, cuotas) aparecerían al final, que es cuando más caro sale corregirlos.

| Fase | Contenido | Resultado |
|---|---|---|
| **0** | Docker, estructura del repo, CI, bootstrap de Unity Catalog y Volume | `pytest` corre en contenedor; conexión al workspace verificada |
| **1** | Generador: `customers`, `products`, `orders`, `order_items` | Lotes en Parquet con manifiesto, deterministas |
| **2** | **Corte vertical:** Bronze → Silver → Gold `daily_sales` → consulta SQL | Un número de negocio correcto de punta a punta |
| **3** | Entidades restantes + motor de calidad + cuarentena | Reporte de calidad completo |
| **4** | SCD2, lotes incrementales, datos tardíos | Demostración de reproceso sin duplicados |
| **5** | Asset Bundles + Workflows | Pipeline orquestado, versionado |
| **6** | Databricks SQL + dashboard | Capturas para el README |
| **7** | Medición de rendimiento, demo de time travel, README final | Métricas reales |
| **8** | Predicción de churn | Validación fuera de tiempo: AUC-ROC 0,789 frente a 0,683 de la regla trivial |

---

## 15. Riesgos

| Riesgo | Mitigación |
|---|---|
| Free Edition no permite crear catálogo propio | Nombre de catálogo en configuración; caer a `workspace` es un cambio de una línea |
| Agotar la cuota de cómputo iterando | Todo el desarrollo en perfil `dev`; `full` se ejecuta una sola vez |
| Deriva de versión entre Spark local y Databricks | Contenedor con versión fijada; verificar contra el workspace en Fase 0 |
| Auto Loader no disponible o limitado en Free Edition | Verificar en Fase 2; alternativa: lectura por lotes con seguimiento de ficheros procesados en `ops` |
| El alcance crece y el proyecto no se termina | Las fases 0–6 son el proyecto entregable; 7 y 8 son mejoras |

---

## 16. Decisiones registradas

Se documentan en `docs/decisions.md` con contexto, alternativas descartadas y consecuencias:

1. Contenedor Docker como entorno de desarrollo único
2. Datos sintéticos generados frente a un dataset público
3. Faker para dimensiones y `numpy` para hechos
4. Divisa única (USD)
5. `MERGE` con desempate por `updated_at` frente a append
6. SCD Tipo 2 solo en dimensiones
7. Motor de calidad propio frente a Great Expectations
8. Asset Bundles frente a configuración en la interfaz
9. `src/` como librería y notebooks como drivers finos
10. Rendimiento medido en bytes escaneados, no en tiempo de reloj
