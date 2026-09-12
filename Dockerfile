# Entorno de desarrollo del proyecto.
#
# Existe por tres razones, no solo por comodidad:
#   1. PySpark requiere Java, ausente en la máquina de desarrollo.
#   2. PySpark en Windows nativo exige winutils.exe y hadoop.dll configurados
#      a mano, y aun así falla de formas difíciles de diagnosticar. En Linux
#      ese problema no existe.
#   3. Fija Python 3.12 para coincidir con el runtime de Databricks. La máquina
#      de desarrollo tiene 3.13, que PySpark aún no soporta bien.
FROM python:3.12-slim-bookworm

# JRE 17: PySpark necesita el runtime de Java, no el JDK completo.
# procps aporta `ps`, que los scripts de arranque de Spark invocan.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        ca-certificates \
        curl \
        git \
        procps \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME resuelto desde el binario real en lugar de codificar la ruta:
# así que una ruta literal rompería la imagen en máquinas ARM.
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

# Databricks CLI.
#
# ATENCIÓN: `pip install databricks-cli` instala el CLI ANTIGUO de Python, que
# NO soporta Asset Bundles. El CLI moderno es un binario en Go y se instala con
# el script oficial.
#
# La versión se fija para que la imagen sea reproducible: con el argumento
# vacío el script instala "la última", y una reconstrucción dentro de unos
# meses produciría una imagen distinta sin que nada en el repo cambiara.
# Para probar otra versión puntualmente:
#     docker compose build --build-arg DATABRICKS_CLI_VERSION=v1.16.0
ARG DATABRICKS_CLI_VERSION="v1.15.0"
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh \
      | sh -s -- ${DATABRICKS_CLI_VERSION} \
    && databricks --version

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Las dependencias se copian antes que el código para que el caché de capas de
# Docker no se invalide cada vez que se edita un fichero fuente.
WORKDIR /workspace
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

# Spark, dentro de un contenedor, intenta resolver el nombre del host y puede
# quedarse bloqueado o elegir una interfaz equivocada. Fijar la IP local lo evita.
ENV SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python

# Delta Lake descarga sus JAR con Ivy la primera vez que se crea una sesión.
# Forzar esa descarga durante la construcción cumple dos funciones: la imagen
# queda autocontenida (arranca sin red y sin esperas) y, si algo está mal
# emparejado entre pyspark y delta-spark, la construcción falla aquí en lugar
# de fallar más tarde en mitad de un test.
RUN python -c "\
from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
b = SparkSession.builder.appName('warmup').master('local[1]') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.ui.enabled', 'false'); \
s = configure_spark_with_delta_pip(b).getOrCreate(); \
print('Spark', s.version); \
s.stop()"

# El código fuente no se copia a la imagen: se monta como volumen desde
# docker-compose, para que editar en el anfitrión se refleje al instante.
CMD ["bash"]
