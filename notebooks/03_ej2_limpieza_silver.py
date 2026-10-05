# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ejercicio 2: limpieza e integración con el tipo de cambio
# MAGIC Entrada: tablas `*_raw` de bronze y `silver.tipo_cambio`.
# MAGIC Salida: `silver.clientes`, `silver.cuentas`, `silver.transacciones_gtq` y `silver.dq_reporte`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

DEPARTAMENTOS_GT = [
    "Alta Verapaz", "Baja Verapaz", "Chimaltenango", "Chiquimula", "El Progreso", "Escuintla",
    "Guatemala", "Huehuetenango", "Izabal", "Jalapa", "Jutiapa", "Petén", "Quetzaltenango", "Quiché",
    "Retalhuleu", "Sacatepéquez", "San Marcos", "Santa Rosa", "Sololá", "Suchitepéquez", "Totonicapán", "Zacapa",
]

hallazgos = []


def registrar(tabla, regla, df_afectados, llave, accion):
    """Agrega una regla de calidad al reporte con su conteo e identificadores."""
    ids = [str(r[0]) for r in df_afectados.select(llave).collect()]
    hallazgos.append((tabla, regla, len(ids), ", ".join(sorted(set(ids))), accion))


def deduplicar(df, tabla, llave):
    """Elimina filas idénticas en las columnas de negocio y reporta llaves repetidas con datos distintos."""
    negocio = [c for c in df.columns if not c.startswith("_")]
    orden = [F.col(c).asc_nulls_last() for c in df.columns]

    df = df.withColumn("_copia", F.row_number().over(Window.partitionBy(*negocio).orderBy(*orden)))
    registrar(tabla, "duplicado_exacto", df.filter("_copia > 1"), llave, "eliminado")
    df = df.filter("_copia = 1").drop("_copia")

    df = df.withColumn("_version", F.row_number().over(Window.partitionBy(llave).orderBy(*orden)))
    registrar(tabla, "llave_repetida_con_datos_distintos", df.filter("_version > 1"), llave,
              "se conserva una versión y se reporta")
    return df.filter("_version = 1").drop("_version")


def a_fecha(columna, formato):
    return F.expr(f"to_date(try_to_timestamp(trim({columna}), '{formato}'))")


def sin_auxiliares(df):
    return df.drop(*[c for c in df.columns if c.startswith("_")])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clientes

# COMMAND ----------

clientes = deduplicar(
    spark.table(f"{BRONZE}.clientes_raw").select(
        F.expr("try_cast(trim(cliente_id) AS INT)").alias("cliente_id"),
        F.trim("nombre").alias("nombre"),
        F.coalesce(a_fecha("fecha_alta", "yyyy-MM-dd"), a_fecha("fecha_alta", "dd/MM/yyyy")).alias("fecha_alta"),
        F.initcap(F.trim("departamento")).alias("departamento"),
        F.expr("coalesce(nullif(trim(segmento), ''), 'No definido')").alias("segmento"),
        F.col("fecha_alta").alias("_fecha_alta_orig"),
        F.col("departamento").alias("_departamento_orig"),
        F.col("segmento").alias("_segmento_orig"),
    ),
    "clientes", "cliente_id",
)

registrar("clientes", "fecha_alta_en_formato_dd/MM/yyyy",
          clientes.filter(a_fecha("_fecha_alta_orig", "yyyy-MM-dd").isNull() & F.col("fecha_alta").isNotNull()),
          "cliente_id", "convertida a DATE")
registrar("clientes", "fecha_alta_no_interpretable", clientes.filter(F.col("fecha_alta").isNull()),
          "cliente_id", "queda NULL")
registrar("clientes", "departamento_normalizado", clientes.filter(F.col("_departamento_orig") != F.col("departamento")),
          "cliente_id", "initcap(trim(departamento))")
registrar("clientes", "departamento_no_reconocido",
          clientes.filter(~F.coalesce(F.col("departamento").isin(DEPARTAMENTOS_GT), F.lit(False))),
          "cliente_id", "se conserva")
registrar("clientes", "segmento_faltante", clientes.filter(F.expr("nullif(trim(_segmento_orig), '') IS NULL")),
          "cliente_id", "se asigna 'No definido'")

clientes = sin_auxiliares(clientes)
display(clientes.orderBy("cliente_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuentas

# COMMAND ----------

cuentas = deduplicar(
    spark.table(f"{BRONZE}.cuentas_raw").select(
        F.expr("try_cast(trim(cuenta_id) AS INT)").alias("cuenta_id"),
        F.expr("try_cast(trim(cliente_id) AS INT)").alias("cliente_id"),
        F.upper(F.trim("tipo_cuenta")).alias("tipo_cuenta"),
        F.coalesce(a_fecha("fecha_apertura", "yyyy-MM-dd"), a_fecha("fecha_apertura", "dd/MM/yyyy")).alias("fecha_apertura"),
        F.upper(F.trim("estado")).alias("estado"),
    ),
    "cuentas", "cuenta_id",
)

cuentas = (
    cuentas.join(clientes.select("cliente_id", F.lit(True).alias("_cliente_existe")), "cliente_id", "left")
    .withColumn("flag_cliente_inexistente", F.col("_cliente_existe").isNull())
    .select("cuenta_id", "cliente_id", "tipo_cuenta", "fecha_apertura", "estado", "flag_cliente_inexistente")
)

registrar("cuentas", "cliente_id_inexistente", cuentas.filter("flag_cliente_inexistente"), "cuenta_id",
          "se conserva con flag_cliente_inexistente")
display(cuentas.orderBy("cuenta_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transacciones en quetzales
# MAGIC `monto_gtq = abs(monto) × tasa_aplicada`: tasa 1 para GTQ y la del Banguat del día para USD, redondeado a centavos.

# COMMAND ----------

transacciones = deduplicar(
    spark.table(f"{BRONZE}.transacciones_raw").select(
        F.trim("transaccion_id").alias("transaccion_id"),
        F.expr("try_cast(trim(cuenta_id) AS INT)").alias("cuenta_id"),
        F.expr("try_to_timestamp(trim(fecha_hora), 'yyyy-MM-dd HH:mm:ss')").alias("fecha_hora"),
        F.expr("try_cast(trim(monto) AS DECIMAL(18,2))").alias("monto"),
        F.upper(F.trim("moneda")).alias("moneda"),
        F.upper(F.trim("tipo")).alias("tipo"),
        F.upper(F.trim("canal")).alias("canal"),
        F.upper(F.trim("estado")).alias("estado"),
        F.col("monto").alias("_monto_orig"),
        F.col("moneda").alias("_moneda_orig"),
    ),
    "transacciones", "transaccion_id",
)

tasas_usd = spark.table(f"{SILVER}.tipo_cambio").select("fecha", F.col("tasa").alias("_tasa_usd"))
cuentas_existentes = cuentas.select("cuenta_id", F.lit(True).alias("_cuenta_existe"))

transacciones_gtq = (
    transacciones.withColumn("fecha", F.to_date("fecha_hora"))
    .join(tasas_usd, "fecha", "left")
    .join(cuentas_existentes, "cuenta_id", "left")
    .withColumn("tasa_aplicada",
                F.when(F.col("moneda") == "GTQ", F.lit(1).cast("decimal(10,5)"))
                 .when(F.col("moneda") == "USD", F.col("_tasa_usd")))
    .withColumn("monto_gtq", F.round(F.abs("monto") * F.col("tasa_aplicada"), 2).cast("decimal(18,2)"))
    .withColumn("flag_monto_nulo", F.col("monto").isNull())
    .withColumn("flag_monto_negativo", F.coalesce(F.col("monto") < 0, F.lit(False)))
    .withColumn("flag_usd_sin_tasa", F.coalesce((F.col("moneda") == "USD") & F.col("_tasa_usd").isNull(), F.lit(False)))
    .withColumn("flag_cuenta_inexistente", F.col("_cuenta_existe").isNull())
)

registrar("transacciones", "fecha_hora_no_interpretable", transacciones_gtq.filter(F.col("fecha_hora").isNull()),
          "transaccion_id", "queda NULL, sin tasa asignada")
registrar("transacciones", "moneda_normalizada", transacciones_gtq.filter(F.col("_moneda_orig") != F.col("moneda")),
          "transaccion_id", "upper(trim(moneda))")
registrar("transacciones", "moneda_no_soportada",
          transacciones_gtq.filter(~F.coalesce(F.col("moneda").isin("GTQ", "USD"), F.lit(False))),
          "transaccion_id", "monto_gtq NULL")
registrar("transacciones", "tipo_no_reconocido",
          transacciones_gtq.filter(~F.coalesce(F.col("tipo").isin("CREDITO", "DEBITO"), F.lit(False))),
          "transaccion_id", "se conserva")
registrar("transacciones", "monto_nulo",
          transacciones_gtq.filter(F.col("flag_monto_nulo") & F.expr("nullif(trim(_monto_orig), '') IS NULL")),
          "transaccion_id", "flag_monto_nulo, monto_gtq NULL")
registrar("transacciones", "monto_no_numerico",
          transacciones_gtq.filter(F.col("flag_monto_nulo") & F.expr("nullif(trim(_monto_orig), '') IS NOT NULL")),
          "transaccion_id", "flag_monto_nulo, monto_gtq NULL")
registrar("transacciones", "monto_negativo", transacciones_gtq.filter("flag_monto_negativo"),
          "transaccion_id", "flag_monto_negativo, monto_gtq con valor absoluto")
registrar("transacciones", "cuenta_inexistente", transacciones_gtq.filter("flag_cuenta_inexistente"),
          "transaccion_id", "se conserva con flag_cuenta_inexistente")
registrar("transacciones", "usd_sin_tasa", transacciones_gtq.filter("flag_usd_sin_tasa"),
          "transaccion_id", "flag_usd_sin_tasa, monto_gtq NULL")

transacciones_gtq = transacciones_gtq.select(
    "transaccion_id", "cuenta_id", "fecha_hora", "fecha", "tipo", "canal", "estado",
    "moneda", "monto", "tasa_aplicada", "monto_gtq",
    "flag_monto_nulo", "flag_monto_negativo", "flag_usd_sin_tasa", "flag_cuenta_inexistente",
)
display(transacciones_gtq.orderBy("transaccion_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura en silver y reporte de calidad

# COMMAND ----------

for nombre, df in (("clientes", clientes), ("cuentas", cuentas), ("transacciones_gtq", transacciones_gtq)):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SILVER}.{nombre}")

spark.sql(f"COMMENT ON TABLE {SILVER}.transacciones_gtq IS "
          "'Transacciones sin duplicados, con monto en quetzales y flags de calidad. Grano: una transacción.'")

dq_reporte = (
    spark.createDataFrame(hallazgos, "tabla STRING, regla STRING, registros INT, ids STRING, accion STRING")
    .withColumn("fecha_ejecucion", F.current_timestamp())
)
dq_reporte.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SILVER}.dq_reporte")
display(spark.table(f"{SILVER}.dq_reporte").orderBy(F.desc("registros"), "tabla"))

# COMMAND ----------

for tabla, llave in (("clientes", "cliente_id"), ("cuentas", "cuenta_id"), ("transacciones_gtq", "transaccion_id")):
    total, distintos = spark.table(f"{SILVER}.{tabla}").selectExpr("count(*)", f"count(DISTINCT {llave})").first()
    assert total == distintos, f"{tabla}: {total} filas pero {distintos} llaves distintas"
    print(f"{SILVER}.{tabla}: {total} filas, llave única")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportación de `transacciones_gtq` y del reporte de calidad

# COMMAND ----------

for nombre in ("transacciones_gtq", "dq_reporte"):
    spark.table(f"{SILVER}.{nombre}").toPandas().to_csv(f"{EXPORTS_PATH}/{nombre}.csv", index=False)
display(dbutils.fs.ls(EXPORTS_PATH))
