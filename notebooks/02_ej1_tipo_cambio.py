# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ejercicio 1: tipo de cambio del Banguat
# MAGIC Método del WSDL: `TipoCambioRango(fechainit, fechafin)`, con fechas como texto `dd/mm/yyyy`.
# MAGIC Responde `Vars/Var` con `moneda`, `fecha`, `venta` y `compra`. El cliente SOAP está en `src/banguat.py`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import uuid
from datetime import date, datetime, timezone

from pyspark.sql import functions as F

from src import banguat

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rango de fechas de las transacciones

# COMMAND ----------

rango = spark.sql(f"""
    SELECT to_date(min(ts)) AS fecha_ini,
           to_date(max(ts)) AS fecha_fin,
           count_if(ts IS NULL) AS no_parseables
    FROM (SELECT try_to_timestamp(trim(fecha_hora), 'yyyy-MM-dd HH:mm:ss') AS ts
          FROM {BRONZE}.transacciones_raw)
""").first()

FECHA_INI, FECHA_FIN = rango.fecha_ini, rango.fecha_fin
print(f"Rango de transacciones: {FECHA_INI} a {FECHA_FIN}")
if rango.no_parseables:
    print(f"{rango.no_parseables} transacciones con fecha_hora no interpretable quedan fuera del rango")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE}.tipo_cambio_raw (
        id_consulta    STRING,
        fecha_consulta TIMESTAMP COMMENT 'UTC',
        fecha_ini      DATE,
        fecha_fin      DATE,
        endpoint       STRING,
        http_status    INT,
        intentos       INT,
        error          STRING,
        xml_crudo      STRING
    )
    COMMENT 'Respuestas del servicio TipoCambioRango del Banguat, una fila por consulta'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER}.tipo_cambio (
        fecha          DATE          NOT NULL,
        tasa           DECIMAL(10,5) NOT NULL COMMENT 'Quetzales por 1 USD',
        id_consulta    STRING,
        fecha_consulta TIMESTAMP
    )
    COMMENT 'Tipo de cambio de referencia USD/GTQ del Banguat por día'
""")

# COMMAND ----------

ESQUEMA_BRONZE = spark.table(f"{BRONZE}.tipo_cambio_raw").schema


def registrar_bronze(fecha_ini, fecha_fin, *, respuesta=None, error=None):
    """Agrega una consulta a bronze.tipo_cambio_raw, exitosa o fallida."""
    id_consulta = str(uuid.uuid4())
    fila = (
        id_consulta,
        respuesta.fecha_consulta if respuesta else datetime.now(timezone.utc),
        fecha_ini,
        fecha_fin,
        respuesta.endpoint if respuesta else banguat.URL,
        respuesta.http_status if respuesta else None,
        respuesta.intentos if respuesta else None,
        error,
        respuesta.xml_crudo if respuesta else None,
    )
    spark.createDataFrame([fila], ESQUEMA_BRONZE).write.mode("append").saveAsTable(f"{BRONZE}.tipo_cambio_raw")
    return id_consulta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: respuesta XML cruda

# COMMAND ----------

try:
    respuesta = banguat.consultar_tipo_cambio_rango(FECHA_INI, FECHA_FIN)
except banguat.BanguatError as e:
    registrar_bronze(FECHA_INI, FECHA_FIN, error=f"{type(e).__name__}: {e}")
    raise

id_consulta = registrar_bronze(FECHA_INI, FECHA_FIN, respuesta=respuesta)
print(f"Consulta {id_consulta}: HTTP {respuesta.http_status}, {respuesta.intentos} intento(s)")

# COMMAND ----------

display(spark.table(f"{BRONZE}.tipo_cambio_raw").orderBy(F.desc("fecha_consulta")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: tabla `tipo_cambio`

# COMMAND ----------

df_tc = banguat.parsear_tipo_cambio(respuesta.xml_crudo)
faltantes = banguat.validar_rango(df_tc, respuesta.fecha_ini, respuesta.fecha_fin)
if faltantes:
    print(f"Fechas sin tasa: {faltantes}")

implausibles = df_tc[(df_tc["tasa"] < 5) | (df_tc["tasa"] > 12)]
if not implausibles.empty:
    raise ValueError(f"Tasas fuera de rango:\n{implausibles}")

filas = [(r.fecha, r.tasa, id_consulta, respuesta.fecha_consulta) for r in df_tc.itertuples(index=False)]
(
    spark.createDataFrame(filas, "fecha DATE, tasa DECIMAL(10,5), id_consulta STRING, fecha_consulta TIMESTAMP")
    .createOrReplaceTempView("tipo_cambio_nuevo")
)

display(spark.sql(f"""
    MERGE INTO {SILVER}.tipo_cambio AS t
    USING tipo_cambio_nuevo AS s
    ON t.fecha = s.fecha
    WHEN MATCHED AND t.tasa <> s.tasa THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""))

# COMMAND ----------

display(spark.table(f"{SILVER}.tipo_cambio").orderBy("fecha"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportación del XML crudo y de `tipo_cambio`

# COMMAND ----------

with open(f"{EXPORTS_PATH}/tipo_cambio_raw.xml", "w", encoding="utf-8") as f:
    f.write(respuesta.xml_crudo)

(
    spark.table(f"{SILVER}.tipo_cambio").select("fecha", "tasa").orderBy("fecha")
    .toPandas().to_csv(f"{EXPORTS_PATH}/tipo_cambio.csv", index=False)
)
display(dbutils.fs.ls(EXPORTS_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manejo de errores

# COMMAND ----------

casos = {
    "timeout (0.001 s, 2 intentos)": lambda: banguat.consultar_tipo_cambio_rango(
        FECHA_INI, FECHA_FIN, timeout=0.001, max_intentos=2, backoff_seg=0.5),
    "endpoint inexistente (HTTP 500, 2 intentos)": lambda: banguat.consultar_tipo_cambio_rango(
        FECHA_INI, FECHA_FIN, url=banguat.URL.replace("TipoCambio.asmx", "NoExiste.asmx"),
        max_intentos=2, backoff_seg=0.5),
    "XML inválido": lambda: banguat.parsear_tipo_cambio("<html>Service Unavailable</html"),
    "sin datos (rango futuro)": lambda: banguat.obtener_tipo_cambio(date(2030, 6, 1), date(2030, 6, 10)),
}
for nombre, caso in casos.items():
    try:
        caso()
        print(f"{nombre}: sin error")
    except banguat.BanguatError as e:
        print(f"{nombre}: {type(e).__name__} → {str(e)[:150]}")
