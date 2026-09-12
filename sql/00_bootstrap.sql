-- Bootstrap del namespace en Unity Catalog.
--
-- Idempotente: se puede ejecutar tantas veces como haga falta. Es DDL, no
-- configuración hecha a mano en la interfaz, para que el entorno sea
-- reconstruible desde cero y quede versionado.
--
-- Ejecutar con:  python scripts/run_sql.py sql/00_bootstrap.sql
--
-- Nota sobre Free Edition: el API `databricks catalogs create` falla en esta
-- cuenta con "Metastore storage root URL does not exist", porque tiene Default
-- Storage activado y el API exige una MANAGED LOCATION explícita. La ruta SQL
-- sí resuelve el almacenamiento por defecto, así que es la que se usa.

CREATE CATALOG IF NOT EXISTS ecommerce
  COMMENT 'Lakehouse de e-commerce. Arquitectura Medallion: bronze -> silver -> gold.';

-- Zona de aterrizaje: ficheros crudos escritos por el generador y leídos por
-- Auto Loader. Es un esquema aparte porque contiene un Volume, no tablas.
CREATE SCHEMA IF NOT EXISTS ecommerce.landing
  COMMENT 'Zona de aterrizaje de ficheros crudos, previa a Bronze.';

CREATE VOLUME IF NOT EXISTS ecommerce.landing.raw
  COMMENT 'Lotes en Parquet generados. Ruta: /Volumes/ecommerce/landing/raw/<entidad>/batch_NNN/';

CREATE SCHEMA IF NOT EXISTS ecommerce.bronze
  COMMENT 'Datos crudos preservados tal como llegaron, con metadatos de linaje. Sin reglas de negocio.';

CREATE SCHEMA IF NOT EXISTS ecommerce.silver
  COMMENT 'Datos tipados, validados y deduplicados. Claves de negocio, SCD2 y tablas de cuarentena.';

CREATE SCHEMA IF NOT EXISTS ecommerce.gold
  COMMENT 'Productos de datos orientados a negocio, listos para consumo analítico.';

-- Esquema operativo: metadatos del propio pipeline, no datos de negocio.
-- Separarlo evita que las tablas de control ensucien las capas del Medallion.
CREATE SCHEMA IF NOT EXISTS ecommerce.ops
  COMMENT 'Observabilidad: resultados de calidad y metricas de evaluacion de modelos.';

-- Estado interno de Auto Loader: esquemas inferidos y checkpoints de streaming.
-- Va fuera de la zona de aterrizaje a propósito: Auto Loader vigila
-- `landing/raw/<entidad>/`, y si escribiera aquí dentro se detectaría a sí
-- mismo como datos nuevos que ingerir.
CREATE VOLUME IF NOT EXISTS ecommerce.ops.checkpoints
  COMMENT 'Checkpoints y esquemas inferidos de Auto Loader. No contiene datos de negocio.';
