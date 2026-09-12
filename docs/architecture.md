# Arquitectura

```
                    ┌──────────────────────────────┐
                    │  Generador (contenedor local) │
                    │  Faker + numpy · sin Spark    │
                    └───────────────┬──────────────┘
                                    │ Parquet por lotes, todo texto
                                    ▼
                    ┌──────────────────────────────┐
                    │  UC Volume  landing.raw       │
                    │  <entidad>/batch_NNN/         │
                    └───────────────┬──────────────┘
                                    │ Auto Loader · availableNow
                                    ▼
   ┌────────────────────────────────────────────────────────────────┐
   │  BRONZE        append-only · esquema preservado                 │
   │                _ingestion_timestamp, _source_file,              │
   │                _batch_id, _pipeline_run_id                      │
   └───────────────────────────────┬────────────────────────────────┘
                                   │ try_cast · normalizar · deduplicar
                                   │ evaluar reglas · MERGE / SCD2
                                   ▼
   ┌────────────────────────────────────────────────────────────────┐
   │  SILVER        tipado · claves de negocio · historial           │
   │                <entidad>_quarantine con el motivo de rechazo    │
   └───────────────┬───────────────────────────────┬────────────────┘
                   │                               │
                   ▼                               ▼
   ┌───────────────────────────┐   ┌────────────────────────────────┐
   │  OPS                       │   │  GOLD                          │
   │  quality_results           │   │  7 productos de datos          │
   │  model_metrics             │   │  clustering, no particionado   │
   └───────────────┬───────────┘   └───────────────┬────────────────┘
                   │                               │
                   ▼                               ▼
   ┌───────────────────────────┐   ┌────────────────────────────────┐
   │  Puerta de calidad         │   │  Databricks SQL · dashboard    │
   │  detiene el pipeline       │   │  Modelo de fuga (job aparte)   │
   └───────────────────────────┘   └────────────────────────────────┘
```

---

## El DAG

```
ingest_bronze  →  build_silver  →  quality_gate  →  build_gold
```

Definido en [`databricks.yml`](../databricks.yml), desplegado con Asset Bundles,
planificado a diario y en pausa por defecto.

**`build_gold` depende de la puerta, no de Silver.** Colgándolo de Silver se
ganaría paralelismo, pero la puerta quedaría como un aviso decorativo: el
pipeline publicaría cifras aunque la calidad hubiera fallado. Verificado
provocando un fallo: `quality_gate` se detuvo y `build_gold` quedó omitida.

El identificador de la ejecución (`{{job.run_id}}`) se propaga a las tres
primeras tareas, así que cada fila de Bronze y cada resultado de calidad quedan
enlazados con la corrida que los produjo.

### Jobs fuera del camino crítico

Dos jobs más, sin planificación y sin dependencia del pipeline diario:

| Job | Qué hace | Por qué está aparte |
|---|---|---|
| `churn_model` | Entrena y publica `gold.customer_churn_predictions` y `ops.model_metrics` | Consume Gold. Si el modelo falla, las tablas de negocio se siguen publicando |
| `time_travel_demo` | Rompe `gold.daily_sales` y la recupera con `RESTORE` | Es destructivo por diseño |

`churn_model` declara `scikit-learn` en su entorno; el pipeline diario no, para
no alargar el arranque de sus cuatro tareas con una dependencia que no usa.

---

## Por qué cada capa hace lo que hace

### Bronze preserva

Ninguna regla de negocio, ningún tipado, ninguna fila descartada. Lo único que
se añade son metadatos de linaje.

La **idempotencia no se programa**: la aporta el checkpoint de Auto Loader, que
lleva su propio registro de qué ficheros procesó. Reejecutar la ingesta sobre un
lote ya ingerido no añade una sola fila — comprobado ejecutándola dos veces.

`cloudFiles.inferColumnTypes=false` a propósito: los ficheros ya son texto y el
tipado es trabajo de Silver. Inferir aquí adelantaría la decisión a la capa
equivocada y haría fallar la ingesta ante el primer valor inválido, que es
precisamente uno de los casos que el pipeline debe manejar.

La columna `_rescued_data` captura lo que no encaje en el esquema. Sin ella, un
valor inesperado desaparecería en silencio, que es lo contrario de lo que Bronze
promete.

### Silver valida y conforma

El orden de las operaciones no es casual:

1. **Normalizar** — ` DELIVERED ` es un estado legítimo mal escrito; rechazarlo
   por su forma en vez de por su contenido sería un falso positivo.
2. **Convertir con `try_cast`** — un valor inválido se vuelve nulo en lugar de
   abortar el job.
3. **Deduplicar** — un duplicado reemitido es una condición conocida que Silver
   resuelve, no una fila inválida.
4. **Evaluar reglas** — sobre las filas candidatas, no sobre la tabla ya
   construida: si se aplicaran después, el MERGE reinsertaría en cada ejecución
   las filas que la calidad acaba de apartar, en un bucle silencioso.
5. **Separar cuarentena** — ninguna fila se pierde.
6. **Cargar** — `MERGE` para los hechos, SCD Tipo 2 para las dimensiones.

### Gold modela para el negocio

Recarga por ventana en `daily_sales` con `replaceWhere`, más un margen hacia
atrás que cubre los datos tardíos. El día se reconstruye con **todas** sus
órdenes, no solo con las del lote: reconstruirlo con el lote lo reescribiría con
un total parcial y los ingresos caerían sin que nada fallara.

El resto se reconstruye completo: su grano es el cliente o el producto, y una
sola orden nueva puede cambiar el segmento de varios clientes al ser los
quintiles relativos.

`customer_churn_predictions` no la escribe este notebook: es la única tabla de
Gold que produce un job aparte. Está en la capa porque es un producto de datos
que el negocio consume, pero no en el camino crítico, porque un modelo que falla
no debe impedir que se publiquen los ingresos del día.

---

## Dónde vive la lógica

```
src/ecommerce/          funciones puras DataFrame -> DataFrame
notebooks/              drivers finos que solo las encadenan
```

Esa separación es la que permite probar las transformaciones en segundos dentro
de un contenedor en lugar de necesitar un workspace para cada cambio. Sin ella,
`tests/` sería decorativo y la lógica se duplicaría entre notebooks y librería.

El código viaja a los jobs como **wheel** que el bundle construye e instala, y
`src/` se excluye del sincronizado al workspace: una segunda copia sin versionar
permitiría que un notebook la importara por accidente y el job corriera con
código distinto del desplegado.

---

## Restricciones de Free Edition encontradas

Ninguna se dedujo del manual: todas aparecieron al ejecutar.

| Restricción | Consecuencia |
|---|---|
| `databricks catalogs create` exige `MANAGED LOCATION` con Default Storage | El namespace se crea con `CREATE CATALOG` en SQL |
| `.cache()` no está permitido en serverless | Los recuentos se pagan dos veces a propósito |
| Runtime máximo DBR 18.2 (Spark 4.1.0) | El entorno local se fija a 4.1.1, nunca por delante |
| `max_concurrent_runs: 1` encola las ejecuciones | Una ejecución olvidada bloquea las siguientes |
| La caché de disco del warehouse falsea las mediciones | El rendimiento se mide en layout, no en bytes leídos |
| `pyspark.ml` no funciona en serverless: sin `SparkContext` bajo Spark Connect | El modelo de fuga entrena con scikit-learn en el driver |

---

## Qué no hace este proyecto

- **Streaming continuo.** Auto Loader con `availableNow` da la semántica de
  streaming sin consumo permanente de cuota.
- **Multi-divisa.** Todo en USD; `country` se conserva para segmentación.
- **Gobernanza avanzada de Unity Catalog.** Free Edition no expone control de
  acceso por filas, Delta Sharing ni lineage entre workspaces.
- **CLV predictivo.** `lifetime_value` es gasto histórico y se documenta como
  tal.
- **MLOps.** El modelo de fuga no se registra en MLflow ni se sirve en un
  endpoint. Existe para demostrar que Gold alimenta un caso de uso real, no para
  montar una plataforma de servicio de modelos.
