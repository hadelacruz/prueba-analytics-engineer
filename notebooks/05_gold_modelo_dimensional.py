# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold: modelo dimensional
# MAGIC Modelo estrella con `fact_transacciones` (grano: una transacción) y las dimensiones cliente, cuenta y fecha.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dim_cliente = spark.sql(f"""
    SELECT cliente_id, nombre, departamento, segmento, fecha_alta
    FROM {SILVER}.clientes
    UNION ALL
    SELECT -1, 'Desconocido', 'Desconocido', 'No definido', CAST(NULL AS DATE)
""")

dim_cuenta = spark.sql(f"""
    SELECT cuenta_id, cliente_id, tipo_cuenta, fecha_apertura, estado, flag_cliente_inexistente
    FROM {SILVER}.cuentas
""")

dim_fecha = spark.sql(f"""
    WITH fechas AS (
        SELECT fecha_alta AS fecha FROM {SILVER}.clientes
        UNION ALL SELECT fecha_apertura FROM {SILVER}.cuentas
        UNION ALL SELECT fecha FROM {SILVER}.transacciones_gtq
    ),
    limites AS (
        SELECT min(fecha) AS inicio, max(fecha) AS fin FROM fechas
    ),
    calendario AS (
        SELECT explode(sequence(inicio, fin, INTERVAL 1 DAY)) AS fecha FROM limites
    )
    SELECT
        CAST(date_format(fecha, 'yyyyMMdd') AS INT) AS fecha_key,
        fecha,
        year(fecha)    AS anio,
        quarter(fecha) AS trimestre,
        month(fecha)   AS mes,
        element_at(array('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto',
                         'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'), month(fecha)) AS nombre_mes,
        day(fecha)     AS dia,
        element_at(array('Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'),
                   dayofweek(fecha)) AS nombre_dia,
        dayofweek(fecha) IN (1, 7) AS es_fin_de_semana
    FROM calendario
""")

fact_transacciones = spark.sql(f"""
    SELECT
        t.transaccion_id,
        CAST(date_format(t.fecha, 'yyyyMMdd') AS INT) AS fecha_key,
        t.cuenta_id,
        CASE WHEN cu.cliente_id IS NULL OR cu.flag_cliente_inexistente THEN -1 ELSE cu.cliente_id END AS cliente_id,
        t.fecha_hora,
        t.tipo,
        t.canal,
        t.estado,
        t.moneda,
        t.monto AS monto_original,
        t.tasa_aplicada,
        t.monto_gtq,
        CASE t.tipo WHEN 'CREDITO' THEN t.monto_gtq WHEN 'DEBITO' THEN -t.monto_gtq END AS monto_gtq_neto,
        t.flag_monto_nulo,
        t.flag_monto_negativo,
        t.flag_usd_sin_tasa,
        t.flag_cuenta_inexistente
    FROM {SILVER}.transacciones_gtq t
    LEFT JOIN {SILVER}.cuentas cu ON cu.cuenta_id = t.cuenta_id
""")

for nombre, df in (("dim_cliente", dim_cliente), ("dim_cuenta", dim_cuenta),
                   ("dim_fecha", dim_fecha), ("fact_transacciones", fact_transacciones)):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.{nombre}")
    print(f"{GOLD}.{nombre}: {spark.table(f'{GOLD}.{nombre}').count()} filas")

# COMMAND ----------

gold_total, silver_total = spark.sql(f"""
    SELECT (SELECT SUM(monto_gtq) FROM {GOLD}.fact_transacciones),
           (SELECT SUM(monto_gtq) FROM {SILVER}.transacciones_gtq)
""").first()
assert gold_total == silver_total, f"gold {gold_total} ≠ silver {silver_total}"
print(f"Monto total en GTQ, gold y silver: {gold_total}")
