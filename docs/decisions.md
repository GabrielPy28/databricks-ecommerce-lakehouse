# Decisiones de arquitectura

Cada entrada recoge el contexto, la alternativa descartada y la consecuencia.
Varias nacieron de un fallo encontrado al ejecutar, no al diseñar; esas llevan
la cifra que lo destapó.

---

## 1. El entorno de desarrollo es un contenedor

**Contexto.** La máquina de desarrollo es Windows con Python 3.13 y sin JDK.
PySpark no soporta bien 3.13, requiere Java, y en Windows nativo exige
`winutils.exe` y `hadoop.dll` colocados a mano.

**Descartado.** Instalar Python 3.12, un JDK y el CLI en la máquina. Funciona,
pero el entorno no queda versionado y nadie más puede reproducirlo.

**Consecuencia.** El entorno viaja en el repositorio y se fija Python 3.12 para
coincidir con el runtime de Databricks. A cambio, Docker Desktop debe estar
arrancado.

---

## 2. El local nunca va por delante del destino

**Contexto.** El runtime más alto disponible en el workspace es DBR 18.2, con
Spark 4.1.0. La primera imagen traía Spark 4.2.0.

**Descartado.** Usar la última versión de PySpark. La dirección del desajuste
importa: probar contra un Spark más nuevo permite usar APIs que en Databricks no
existen, y el fallo aparece en el despliegue.

**Consecuencia.** `pyspark==4.1.1` + `delta-spark==4.3.1`, y un test que afirma
`4.1.x` para que una actualización por delante de Databricks la detecte CI.

---

## 3. Los ficheros de aterrizaje llevan todas las columnas como texto

**Contexto.** El proyecto necesita inyectar timestamps inválidos, precios
negativos y correos malformados para que Silver tenga trabajo real.

**Descartado.** Generar datos ya tipados. Una columna Parquet tipada como
`timestamp` **no admite** una fecha inválida, así que la inyección sería
imposible y el tipado de Silver, decorativo.

**Consecuencia.** `schemas/` define el contrato lógico tipado; el generador
serializa a texto. El trabajo de Silver es reconstruir el contrato, que es
además cómo llega un extracto de un sistema operacional.

---

## 4. `try_cast`, no `cast`

**Contexto.** Spark 4 activa el modo ANSI por defecto.

**Descartado.** `cast` a secas. Un valor inválido lanza excepción y **aborta el
job entero**: un solo registro corrupto entre millones tumbaría el pipeline.

**Consecuencia.** El valor inválido se vuelve nulo y la cuarentena lo recoge con
su motivo. El pipeline sigue y el problema queda registrado.

---

## 5. Cuarentena, no descarte

**Contexto.** Hay filas que no se pueden procesar.

**Descartado.** Filtrarlas. Perder datos en silencio es peor que no procesarlos:
nadie se entera y los totales no cuadran sin explicación.

**Consecuencia.** `silver.<entidad>_quarantine` conserva la fila y la columna
`_errors` con el motivo exacto. La suma de válidas y cuarentena es siempre el
total de entrada.

---

## 6. Una sola cuarentena para dos tipos de fallo

**Contexto.** Una fila puede fallar por conversión (una fecha que no es fecha) o
por regla de negocio (un cliente que no existe).

**Descartado.** Una tabla por mecanismo. Obligaría a mirar en dos sitios para
saber por qué se rechazó una fila y a reconciliar dos recuentos que deberían ser
uno.

**Consecuencia.** Ambos tipos acaban en la columna `_errors`.

---

## 7. La deduplicación va antes de validar

**Contexto.** El origen reemite registros; el generador inyecta un 0,3 % de
duplicados.

**Descartado.** Validar primero. La puerta de calidad lo detectó en cuanto se
ejecutó: la regla `unique` marcó las **60 filas** implicadas en los 30
duplicados y las mandó a cuarentena, cuando lo correcto es quedarse con la
versión más reciente.

**Consecuencia.** Un duplicado reemitido es una condición conocida que Silver
resuelve, no una fila inválida. `unique` queda como red de seguridad que no
debería saltar nunca.

---

## 8. El MERGE desempata por `updated_at`

**Contexto.** Una orden muta: `pending → paid → shipped → delivered`, y algunos
registros llegan tarde.

**Descartado.** `whenMatchedUpdateAll()` sin condición. Un mensaje retrasado
devolvería una orden entregada al estado `pending` sin que nada fallara.

**Consecuencia.** `s.updated_at > t.updated_at` resuelve tres cosas a la vez:
idempotencia, datos tardíos y actualización legítima.

---

## 9. SCD Tipo 2 solo en las dimensiones

**Contexto.** Clientes y productos cambian de país, precio y coste.

**Descartado.** Sobrescribir. `order_items.unit_price` ya preserva el precio de
venta, así que los ingresos históricos salen bien sin historial — pero el
**coste** y el **país** no. Un cliente que se muda reescribiría retroactivamente
los ingresos de dos países.

**Consecuencia.** `customers` y `products` guardan versiones con vigencia, y
Gold une cada hecho con la versión vigente **en su fecha**.

---

## 10. La reproducción del historial va por versión, no por fecha

**Contexto.** Silver recorre todo Bronze en cada ejecución, que acumula una fila
por cada cambio de la dimensión.

**Descartado.** Agrupar el replay por marca de tiempo. Parecía equivalente y no
lo es: en cuanto cada cliente pasó a marcarse con su fecha de alta, la carga
inicial de 1.000 clientes lanzaba unos 2.000 MERGE y `build_silver` pasó de
**171 s a más de 3.100 s**.

**Consecuencia.** Lo que obliga a separar pasadas es que una misma **clave**
traiga varias versiones, no que haya muchas fechas. El bucle va por número de
versión: dos pasadas con dos lotes, treinta y una con treinta.

---

## 11. El snapshot inicial se marca con la fecha de creación

**Contexto.** La primera versión de una dimensión necesita un `valid_from`.

**Descartado.** La fecha de extracción. Con ella, la primera versión solo era
válida desde hoy y **ninguna orden del histórico encontraba versión vigente**:
el coste salió nulo en **199 de 307 filas** de `product_performance`, con el
margen inventado.

**Consecuencia.** Los clientes se marcan con su fecha de alta y los productos
con el inicio del catálogo.

---

## 12. Las reglas de calidad son configuración

**Contexto.** Hacen falta 45 comprobaciones sobre 8 tablas.

**Descartado.** Escribirlas a mano en cada notebook (duplica lógica, escala mal)
y Great Expectations (dependencia pesada que oscurece la habilidad que se quiere
demostrar; escribir el motor *es* la demostración).

**Consecuencia.** `rules.yml` responde «¿qué comprueba este pipeline?» en un
fichero, y añadir una regla no toca Python. La validación ocurre al cargar: un
`not_nul` mal escrito falla al arrancar, no a mitad del pipeline.

---

## 13. La severidad por defecto es la menos destructiva

**Contexto.** Una regla sin `severity` necesita un valor.

**Descartado.** `quarantine`. Un descuido de configuración retiraría datos
buenos en silencio.

**Consecuencia.** Por defecto `warn`: informa y deja pasar.

---

## 14. La puerta de calidad falla si no encuentra resultados

**Contexto.** La puerta lee `ops.quality_results` de la ejecución en curso.

**Descartado.** Aprobar cuando no hay filas. Ocurrió de verdad: Silver falló, no
escribió resultados, y la puerta **dio vía libre** — el peor fallo posible en
una puerta de calidad, porque aprueba precisamente cuando el paso anterior no se
ejecutó.

**Consecuencia.** Sin resultados se detiene explícitamente.

---

## 15. `build_gold` depende de la puerta, no de Silver

**Contexto.** El DAG podría colgar Gold directamente de Silver y ganar
paralelismo.

**Descartado.** Hacerlo. La puerta quedaría como un aviso decorativo: el
pipeline publicaría cifras aunque la calidad hubiera fallado.

**Consecuencia.** Verificado provocando un fallo: `quality_gate` se detuvo y
`build_gold` quedó **omitida**.

---

## 16. El código viaja a los jobs como wheel

**Contexto.** Los notebooks necesitan importar `src/`.

**Descartado.** Añadir `src/` al `sys.path` y sincronizarlo al workspace. Deja
una segunda copia sin versionar que un notebook puede importar por accidente, y
el job correría con código distinto del desplegado.

**Consecuencia.** El bundle construye un wheel con `rules.yml` dentro y excluye
`src/` del sincronizado. Esa exclusión es además lo que **prueba** que el wheel
se usa: el pipeline funciona con `src/` ausente del workspace.

---

## 17. Ningún identificador de workspace en el repositorio

**Contexto.** El bundle necesita un host y el dashboard un warehouse.

**Descartado.** Escribirlos en `databricks.yml`. Ataría el repositorio a una
cuenta concreta.

**Consecuencia.** El host sale del entorno y el warehouse se resuelve por nombre
con `lookup`. Hay un test que lo comprueba.

---

## 18. La planificación se declara explícitamente en pausa

**Contexto.** `mode: development` pausa las planificaciones automáticamente.

**Descartado.** Confiar en ese comportamiento. Un job que se enciende por un
efecto colateral del modo de despliegue es un job que nadie recuerda haber
encendido.

**Consecuencia.** `pause_status` es una variable, `PAUSED` por defecto y
`UNPAUSED` en producción.

---

## 19. El dashboard se genera, no se escribe a mano

**Contexto.** El formato de un widget de tabla exige una veintena de claves por
columna.

**Descartado.** Escribirlo a mano. Se hizo, y faltaron `type`, `displayAs`,
`visible` y `title`: los tres paneles de tabla aparecieron con *«Invalid widget
definition is imported»*. **El despliegue no falló** —la validación ocurre al
renderizar—, así que el error parecía inexistente.

**Consecuencia.** `build_dashboard.py` contiene una declaración legible y expande
el formato. Un test comprueba que el JSON no se desincronice, y otro que toda
columna lleve las claves obligatorias. El formato correcto se obtuvo
**inspeccionando un dashboard que funciona**, no adivinando.

---

## 20. Las tablas Gold se agrupan por clustering, no por particiones

**Contexto.** `daily_sales`, `category_performance` y `marketing_funnel` tienen
una columna de fecha natural.

**Descartado.** Particionar por ella. Medirlo lo dejó claro: `daily_sales` acabó
con **698 ficheros de 2,2 KB —una fila por fichero—** y `marketing_funnel` con
721. Una partición por día sobre una tabla de agregados diarios crea una
partición por fila.

`OPTIMIZE` tampoco lo arreglaba: compacta *dentro* de cada partición, y con un
fichero por partición no tiene nada que hacer. Por eso ni dejaba entrada en el
historial.

**Consecuencia.** Liquid clustering agrupa por la misma columna sin trocear el
almacenamiento. `replaceWhere` sigue funcionando porque opera sobre columnas de
datos, no solo de partición.

---

## 21. El rendimiento se mide en layout, no en tiempo ni en bytes leídos

**Contexto.** Hay que demostrar que las optimizaciones sirven.

**Descartado.** Comparar tiempos de reloj —en serverless miden sobre todo el
estado del almacén— y comparar `read_bytes` de `system.query.history`.

Lo segundo se intentó y falló dos veces: primero la **caché de disco** devolvía
0 bytes leídos tras `OPTIMIZE` y declaraba una mejora del 100 % inexistente;
después, ni reiniciando el warehouse, porque esas columnas se enriquecen con un
retraso mayor que el que el script podía esperar.

**Consecuencia.** La medida publicada es el layout físico (`DESCRIBE DETAIL`):
número de ficheros y tamaño medio. No depende de cachés ni de latencias, y es la
que explica el problema.

---

## 22. El generador es una función pura de (semilla, perfil, lote)

**Contexto.** Los lotes incrementales necesitan mutar órdenes del pasado.

**Descartado.** Guardar estado entre ejecuciones. Un fichero de estado acaba
desincronizado del dato y vuelve irreproducible todo el proyecto.

**Consecuencia.** El lote N regenera el histórico de forma determinista para
extraer la muestra que muta. Cuesta O(histórico) por lote, asumible a las
escalas de desarrollo.

---

## 23. La suciedad se inyecta con recuentos exactos y se mide en el resultado

**Contexto.** El reporte de calidad tiene que ser explicable.

**Descartado.** Una tirada aleatoria por fila, y sumar las anomalías
*pretendidas*. Las anomalías interactúan: duplicar filas puede copiar una que ya
era huérfana. El manifiesto mentiría.

**Consecuencia.** `round(tasa × filas)` filas afectadas, y el manifiesto
**inspecciona el resultado final**. Se ve en los datos: el 2 % de 10.000 son 200
estados sucios inyectados, y el manifiesto reporta **201** porque un duplicado
copió una fila ya ensuciada.

---

## 24. Divisa única

**Contexto.** Los clientes son de ocho países.

**Descartado.** Multi-divisa con conversión FX. Añade complejidad sin demostrar
ninguna habilidad que el resto del proyecto no cubra.

**Consecuencia.** Todo en USD. `country` se conserva para segmentación. Es
alcance descartado a propósito, no un olvido.

---

## 25. `lifetime_value` es gasto histórico, no una predicción

**Contexto.** La tabla se llama `customer_lifetime_value`.

**Descartado.** Presentar el gasto acumulado como CLV a secas. Un CLV predictivo
necesita un modelo de supervivencia y de frecuencia de compra.

**Consecuencia.** La columna se calcula y se documenta como lo que es. Llamar
«valor de vida» a un modelo inexistente produce cifras que nadie puede defender
en una reunión.

---

## 26. Las variables del modelo de fuga se calculan respecto a un corte

**Contexto.** La fuga se define como «sin compras con ingreso reconocido en los
90 días siguientes». Las variables naturales —recencia, frecuencia, gasto— son
justo las que codifican esa respuesta si se calculan sobre todo el histórico.

**Descartado.** Calcular las variables con todos los datos disponibles y
etiquetar después. Es lo que hace la mayoría de los modelos de churn de
portafolio: da un AUC cercano a 1 y no predice nada, porque
`days_since_last_order` sobre el histórico completo **es** la etiqueta.

**Descartado también.** Un reparto aleatorio 80/20 del mismo periodo. Deja al
modelo ver el futuro de sus propios clientes y produce una cifra irreal.

**Consecuencia.** Todo cuelga de un corte: las variables solo miran fechas
`<= T`, la etiqueta solo fechas `> T`. Y hay dos cortes, T1 para entrenar y T2
para evaluar, de modo que la validación es **fuera de tiempo**. El test que
sostiene la propiedad no comprueba una cifra: añade actividad posterior al corte
y exige que no cambie ni una variable. Medido: AUC-ROC 0,789, que es lo que
parece un modelo honesto.

---

## 27. Toda métrica del modelo se publica junto a una línea base trivial

**Contexto.** Un AUC-ROC de 0,789 suena bien en aislamiento.

**Descartado.** Publicar solo las métricas del modelo. Sin referencia, cualquier
cifra parece buena y no se puede decidir si el modelo aporta algo.

**Consecuencia.** La regla que el negocio ya aplica sin modelo —ordenar por
recencia— se evalúa con las mismas métricas y se guarda en la misma tabla:
0,683. La mejora real es +0,106, no 0,789. Si el modelo no superase a la regla,
la conclusión correcta sería no desplegarlo.

---

## 28. scikit-learn en el driver, no MLlib

**Contexto.** El primer intento usó `pyspark.ml`, que es lo esperable en un
proyecto de Spark.

**Encontrado al ejecutar.** En serverless falla con `AssertionError` en
`_new_java_obj`: esos estimadores envuelven objetos de la JVM y Spark Connect no
expone un `SparkContext` desde el que crearlos.

**Descartado.** `pyspark.ml.connect`, que sí funciona en Connect pero solo
ofrece regresión logística: no permitiría comparar tres algoritmos.

**Consecuencia.** Spark hace el trabajo pesado —recorrer el histórico de órdenes
y agregarlo por cliente— y el entrenamiento se queda en el driver. A esta escala
es además lo correcto con independencia de la restricción: 914 clientes por diez
variables son kilobytes, y repartirlos por la red cuesta más de lo que ahorra.

---

## 29. Los tramos de riesgo salen del orden, no de umbrales absolutos

**Contexto.** La tabla de predicciones necesita un `risk_band` accionable.

**Descartado, tras medirlo.** Umbrales fijos en 0,7 y 0,4. Con una prevalencia
del 37 % el modelo casi nunca supera 0,7: el tramo «alto» se quedó con 3
clientes de 914. Un tramo de tres personas no es una lista de prioridad.

**Consecuencia.** El corte es por decil de riesgo —10 % alto, 20 % siguiente
medio—, que es estable aunque las probabilidades se desplacen entre
reentrenamientos y encaja con cómo trabaja un equipo de retención: capacidad
fija, se llama a los N más probables que se puedan atender.

**Efecto secundario que la tabla dejó ver.** El decil de mayor riesgo concentra
el 1,2 % del gasto histórico: son compradores de una sola orden. Riesgo y valor
van en direcciones opuestas, así que priorizar solo por probabilidad gasta el
presupuesto en los clientes más baratos de perder.

