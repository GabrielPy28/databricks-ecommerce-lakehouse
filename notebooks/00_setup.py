# Databricks notebook source
# MAGIC %md
# MAGIC # Arranque común
# MAGIC
# MAGIC Deja disponible el paquete `ecommerce` para el resto de notebooks.
# MAGIC
# MAGIC Los notebooks de este proyecto son **drivers finos**: no contienen lógica
# MAGIC de transformación, solo encadenan funciones de `src/`. Esa separación es
# MAGIC lo que permite probar las transformaciones en segundos dentro de un
# MAGIC contenedor, en lugar de necesitar un workspace para cada cambio.
# MAGIC
# MAGIC Se invoca con `%run ./00_setup` desde los demás notebooks.

# COMMAND ----------

import sys

try:
    # Camino normal: el Asset Bundle construye un wheel y lo instala en el
    # entorno serverless del job. El paquete llega versionado y con su
    # `rules.yml` dentro, en vez de depender de que exista un directorio al
    # lado del notebook.
    import ecommerce

    print(f"Paquete instalado: ecommerce {getattr(ecommerce, '__version__', '(sin versión)')}")
except ModuleNotFoundError:
    # Alternativa para ejecutar un notebook a mano desde un Git Folder, sin
    # pasar por el bundle. Se conserva porque durante el desarrollo es cómodo
    # lanzar un notebook suelto sin desplegar nada.
    import os

    _src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
    if _src not in sys.path:
        sys.path.insert(0, _src)

    import ecommerce

    print(f"Paquete no instalado ({ecommerce.__name__}); se usa el árbol de fuentes en {_src}")
