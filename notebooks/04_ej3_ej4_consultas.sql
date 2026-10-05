-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Ejercicio 3 (SQL) y ejercicio 4 (depuración)
-- MAGIC Motor: Databricks SQL (Spark SQL sobre tablas Delta). Montos en quetzales (`monto_gtq`), solo transacciones `COMPLETADA`.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

USE SCHEMA silver;

-- COMMAND ----------

-- Validación: monto_gtq de las transacciones en USD contra tipo_cambio
SELECT
    t.transaccion_id,
    t.fecha,
    t.monto,
    tc.tasa,
    t.monto_gtq,
    ROUND(ABS(t.monto) * tc.tasa, 2) AS monto_gtq_recalculado
FROM transacciones_gtq t
LEFT JOIN tipo_cambio tc ON tc.fecha = t.fecha
WHERE t.moneda = 'USD'
ORDER BY t.transaccion_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.1 Monto total de créditos y de débitos por cliente

-- COMMAND ----------

SELECT
    c.cliente_id,
    c.nombre,
    COALESCE(SUM(CASE WHEN t.tipo = 'CREDITO' THEN t.monto_gtq END), 0) AS total_creditos_gtq,
    COALESCE(SUM(CASE WHEN t.tipo = 'DEBITO'  THEN t.monto_gtq END), 0) AS total_debitos_gtq,
    COUNT(CASE WHEN t.transaccion_id IS NOT NULL AND t.monto_gtq IS NULL THEN 1 END) AS transacciones_sin_monto
FROM clientes c
LEFT JOIN cuentas cu
       ON cu.cliente_id = c.cliente_id
LEFT JOIN transacciones_gtq t
       ON t.cuenta_id = cu.cuenta_id
      AND t.estado = 'COMPLETADA'
GROUP BY c.cliente_id, c.nombre
ORDER BY c.cliente_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.2 Clientes sin ninguna transacción

-- COMMAND ----------

WITH clientes_con_transacciones AS (
    SELECT DISTINCT cu.cliente_id
    FROM transacciones_gtq t
    JOIN cuentas cu ON cu.cuenta_id = t.cuenta_id
    WHERE t.estado = 'COMPLETADA'
)
SELECT c.cliente_id, c.nombre, c.departamento, c.segmento
FROM clientes c
LEFT JOIN clientes_con_transacciones x ON x.cliente_id = c.cliente_id
WHERE x.cliente_id IS NULL
ORDER BY c.cliente_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.3 Cliente con mayor monto total transaccionado por departamento

-- COMMAND ----------

WITH monto_por_cliente AS (
    SELECT
        c.departamento,
        c.cliente_id,
        c.nombre,
        COALESCE(SUM(t.monto_gtq), 0) AS monto_total_gtq
    FROM clientes c
    LEFT JOIN cuentas cu
           ON cu.cliente_id = c.cliente_id
    LEFT JOIN transacciones_gtq t
           ON t.cuenta_id = cu.cuenta_id
          AND t.estado = 'COMPLETADA'
    GROUP BY c.departamento, c.cliente_id, c.nombre
),
ranking AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY departamento ORDER BY monto_total_gtq DESC) AS posicion
    FROM monto_por_cliente
)
SELECT departamento, cliente_id, nombre, monto_total_gtq
FROM ranking
WHERE posicion = 1
ORDER BY departamento;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ejercicio 4: depuración
-- MAGIC Datos de ejemplo para `saldos` (no se entregaron), para comparar la consulta original con la corregida.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW saldos AS
SELECT * FROM VALUES
    (101, DATE'2025-06-28', 1000.00), (101, DATE'2025-06-29', 1100.00), (101, DATE'2025-06-30', 1200.00),
    (102, DATE'2025-06-28',  500.00), (102, DATE'2025-06-29',  450.00), (102, DATE'2025-06-30',  400.00),
    (103, DATE'2025-06-30', 1500.00)
AS s(cuenta_id, fecha, saldo);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW transacciones AS
SELECT * FROM transacciones_gtq;

-- COMMAND ----------

-- Filas que genera el join por cuenta
WITH s AS (SELECT cuenta_id, COUNT(*) AS n_saldos FROM saldos GROUP BY cuenta_id),
     t AS (SELECT cuenta_id, COUNT(*) AS n_transacciones FROM transacciones GROUP BY cuenta_id)
SELECT
    cu.cuenta_id,
    s.n_saldos,
    t.n_transacciones,
    s.n_saldos * t.n_transacciones AS filas_en_el_join
FROM cuentas cu
JOIN clientes c ON c.cliente_id = cu.cliente_id
LEFT JOIN s ON s.cuenta_id = cu.cuenta_id
LEFT JOIN t ON t.cuenta_id = cu.cuenta_id
WHERE c.segmento = 'Premium'
ORDER BY cu.cuenta_id;

-- COMMAND ----------

-- Consulta original
SELECT
    c.segmento,
    SUM(s.saldo) AS saldo_total,
    COUNT(t.transaccion_id) AS num_transacciones
FROM clientes c
JOIN cuentas cu ON c.cliente_id = cu.cliente_id
JOIN saldos s ON cu.cuenta_id = s.cuenta_id
JOIN transacciones t ON cu.cuenta_id = t.cuenta_id
WHERE c.segmento = 'Premium'
GROUP BY c.segmento;

-- COMMAND ----------

-- Consulta corregida
WITH saldo_actual AS (
    SELECT cuenta_id, saldo
    FROM (
        SELECT
            cuenta_id,
            saldo,
            ROW_NUMBER() OVER (PARTITION BY cuenta_id ORDER BY fecha DESC) AS rn
        FROM saldos
    ) s
    WHERE rn = 1
),
transacciones_por_cuenta AS (
    SELECT cuenta_id, COUNT(*) AS num_transacciones
    FROM transacciones
    GROUP BY cuenta_id
)
SELECT
    c.segmento,
    SUM(s.saldo) AS saldo_total,
    COALESCE(SUM(t.num_transacciones), 0) AS num_transacciones
FROM clientes c
JOIN cuentas cu ON c.cliente_id = cu.cliente_id
LEFT JOIN saldo_actual s ON cu.cuenta_id = s.cuenta_id
LEFT JOIN transacciones_por_cuenta t ON cu.cuenta_id = t.cuenta_id
WHERE c.segmento = 'Premium'
GROUP BY c.segmento;
