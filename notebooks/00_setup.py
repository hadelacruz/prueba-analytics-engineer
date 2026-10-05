# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Esquemas bronze, silver y gold, volúmenes de archivos y carga de los CSV al volumen de entrada.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

for esquema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE}.landing COMMENT 'Archivos de entrada'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {GOLD}.exports COMMENT 'Archivos exportados'")

# COMMAND ----------

import shutil

for nombre in ("clientes.csv", "cuentas.csv", "transacciones.csv"):
    origen = os.path.join(REPO_ROOT, "data", "raw", nombre)
    destino = f"{LANDING_PATH}/{nombre}"
    if os.path.exists(origen):
        shutil.copy(origen, destino)
    elif not os.path.exists(destino):
        raise FileNotFoundError(f"No se encontró {nombre} en data/raw/ ni en {LANDING_PATH}")

display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conectividad con el Banguat
# MAGIC Free Edition restringe la salida a internet, así que se verifica que el servicio sea accesible.

# COMMAND ----------

import requests

from src.banguat import URL

try:
    r = requests.get(f"{URL}?WSDL", timeout=15)
    print(f"Banguat accesible desde Databricks: HTTP {r.status_code}")
except requests.RequestException as e:
    print(f"Banguat no accesible desde Databricks: {type(e).__name__}: {e}")
