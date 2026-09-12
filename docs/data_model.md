# Modelo de datos

El contrato vive en código, en [`src/ecommerce/schemas/`](../src/ecommerce/schemas/),
y es la fuente de verdad que comparten el generador, la ingesta y los tests.
Este documento explica **por qué** el modelo es como es; los tipos exactos están
en el código y hay tests que impiden que diverjan.

---

## Entidades

```
customers ──┬──< orders ──┬──< order_items ──< returns
            │             ├──< payments
            │             └──< reviews
            │
products ───┴──< order_items
            └──< web_events (opcional)

web_events >── customers (opcional: la mayoría del tráfico es anónimo)
```

### Dimensiones

| Entidad | Clave | Historial | Por qué |
|---|---|---|---|
| `customers` | `customer_id` | **SCD Tipo 2** | El país cambia y arrastra la atribución geográfica de todo el histórico |
| `products` | `product_id` | **SCD Tipo 2** | El coste cambia; sin historial, el margen pasado se reescribe solo |

### Hechos

| Entidad | Clave | Grano | Muta |
|---|---|---|---|
| `orders` | `order_id` | Pedido | **Sí**: `pending → paid → shipped → delivered → returned` |
| `order_items` | `order_item_id` | Línea de pedido | No |
| `payments` | `payment_id` | Pago | **Sí**: `pending → completed → refunded` |
| `reviews` | `review_id` | Reseña | No |
| `returns` | `return_id` | Línea devuelta | No |
| `web_events` | `event_id` | Evento de navegación | No |

---

## Las tres decisiones de modelado que importan

### `updated_at` es lo que sostiene el procesamiento incremental

Sin él, el `MERGE` no puede decidir qué versión de una fila es la buena y
degenera en un `append` que duplica. Lo llevan las entidades que mutan.

Las inmutables **no lo llevan a propósito**: un evento de navegación ya ocurrido
o una reseña ya escrita no cambian, y darles `updated_at` sugeriría lo contrario
e invitaría a escribir lógica de actualización que sobra. Silver las trata como
solo-inserción, desempatando por su propia clave.

El contrato lo declara en `ORDER_BY`, así que el notebook no lo adivina.

### `order_items.unit_price` es el precio del momento de la venta

Nunca se recalcula desde `products.price`. Si se hiciera, un cambio de precio
reescribiría retroactivamente los ingresos de todo el histórico.

Esto es también la razón de que SCD2 **no** sea redundante: el precio de venta
está congelado en la línea, pero el **coste** no. El margen histórico sí
dependía de la dimensión, y por eso Gold une contra la versión vigente en la
fecha de la venta.

### `web_events.customer_id` admite nulos

La mayor parte del tráfico de un e-commerce no ha iniciado sesión: en los datos
generados, el **72,5 %** de los eventos son anónimos. Exigir cliente obligaría a
inventarlos y falsearía por completo la parte alta del embudo.

---

## Invariantes

Verificadas por tests del generador sobre las filas limpias:

```
orders.total_amount   = gross_amount - discount_amount
orders.gross_amount   = SUM(order_items.line_total + order_items.discount_amount)
order_items.line_total = quantity * unit_price - discount_amount
```

Se calculan en **céntimos enteros** y solo se convierten a texto al final. Con
coma flotante, un total no cuadraría con la suma de sus líneas por unos
céntimos de forma impredecible: las invariantes se cumplirían por redondeo
afortunado y no por construcción.

---

## Las dos caras del contrato

**Lógica** (`CUSTOMERS`, `ORDERS`, …): tipada. Es lo que Silver produce y Gold
consume. El dinero es `decimal`, nunca `float`.

**De aterrizaje** (`to_landing_schema`): todo `string`. Es como llegan los
ficheros crudos.

La distinción no es un capricho. Una columna Parquet tipada como `timestamp` no
admite una fecha inválida, así que un generador que emitiera datos ya tipados
haría imposible inyectar la suciedad que Silver debe saber manejar — y es
además como llega un extracto de un sistema operacional.

---

## Metadatos de linaje

Bronze añade cuatro columnas a cada fila. No alteran el dato: lo explican.

| Columna | Para qué |
|---|---|
| `_ingestion_timestamp` | Cuándo se escribió |
| `_source_file` | De qué fichero salió, para rastrear una fila hasta su origen |
| `_batch_id` | A qué lote pertenece, para reprocesar o descartar uno entero |
| `_pipeline_run_id` | Qué ejecución la produjo; enlaza con `ops.quality_results` |

Se quedan en Bronze. Silver solo arrastra las columnas del contrato.

Hay un test que comprueba que **ninguna entidad de negocio use estos nombres**:
una colisión sobrescribiría un dato real con metadatos de ingesta, en silencio.

---

## Capa Gold

| Tabla | Grano | Responde |
|---|---|---|
| `daily_sales` | Día | Ingresos, órdenes, ticket medio, tendencia |
| `product_performance` | Producto | Unidades, margen real, tasa de devolución |
| `category_performance` | Categoría × mes | Qué partes del catálogo crecen |
| `customer_lifetime_value` | Cliente | Gasto acumulado, frecuencia, recencia |
| `customer_segments` | Cliente | Segmentación RFM accionable |
| `marketing_funnel` | Día × canal | Dónde se pierde el tráfico |
| `customer_churn_predictions` | Cliente | Quién va a dejar de comprar (fase opcional) |

`product_performance` y `category_performance` valoran cada línea con el **coste
vigente en la fecha de la venta**. Es donde la inversión en SCD2 rinde: con el
coste actual, cualquier cambio de precio de compra inventaría o destruiría
margen en todo el histórico.

`customer_segments` calcula la recencia contra una **fecha de referencia que
recibe**, no contra `current_date()`: con el reloj del sistema, dos ejecuciones
del mismo pipeline sobre los mismos datos darían segmentos distintos.

`customer_churn_predictions` es la única tabla de la capa que contiene una
**predicción** y no un hecho agregado. Sus filas no tienen etiqueta a propósito:
se calculan en el último corte disponible, sobre clientes cuyos 90 días todavía
no han pasado. Las tablas con las que se entrenó y evaluó el modelo no se
publican; lo que sí se publica es su evaluación, en `ops.model_metrics`, con los
dos cortes, la prevalencia y la línea base al lado de cada cifra.

---

## Anomalías inyectadas

El generador ensucia los datos con tasas fijas y recuentos exactos, registradas
en el manifiesto de cada lote. Ninguna anomalía del dataset es accidental.

| Anomalía | Tasa | Qué ejercita |
|---|---|---|
| Duplicados de orden | 0,30 % | Deduplicación por `updated_at` |
| `customer_id` huérfano | 0,10 % | Regla `foreign_key` |
| `quantity <= 0` | 0,05 % | Regla `min_value` |
| Precio negativo | 0,02 % | Regla `min_value` |
| Timestamp no parseable | 0,10 % | `try_cast` y cuarentena |
| `status` mal escrito | 2,00 % | Normalización antes de validar |
| Correo malformado | 0,50 % | Regla `matches`, severidad aviso |

El manifiesto **mide el resultado final**, no suma las anomalías pretendidas:
las anomalías interactúan. Con 10.000 órdenes, el 2 % son 200 estados sucios
inyectados y el manifiesto reporta **201**, porque un duplicado copió una fila
ya ensuciada.
