# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingesta de los CSV
# MAGIC Las tres extracciones se cargan sin transformar, con todas las columnas como texto, más el archivo de origen
# MAGIC y la hora de carga.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

for tabla in ("clientes", "cuentas", "transacciones"):
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(f"{LANDING_PATH}/{tabla}.csv")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{BRONZE}.{tabla}_raw")
    print(f"{BRONZE}.{tabla}_raw: {df.count()} filas")

# COMMAND ----------

display(spark.table(f"{BRONZE}.transacciones_raw"))
