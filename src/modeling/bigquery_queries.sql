-- BIGQUERY DDL & QUERIES FOR CALA ANALYTICS

-- PART 2.1: Designing staging and final tables

-- Staging Atenciones
CREATE OR REPLACE TABLE `cala_analytics.stg_atenciones` (
    id_atencion INT64,
    id_cliente INT64,
    documento_cliente STRING,
    fecha_atencion TIMESTAMP,
    fecha_proceso DATE,
    valor_facturado FLOAT64,
    estado STRING,
    codigo_cups STRING,
    canal_ingreso STRING,
    diagnostico STRING,
    medico STRING
)
PARTITION BY fecha_proceso
CLUSTER BY id_cliente, documento_cliente;

-- Final Fact Atenciones
CREATE OR REPLACE TABLE `cala_analytics.fct_atenciones` (
    id_atencion INT64,
    id_cliente INT64,
    documento_cliente STRING,
    fecha_atencion TIMESTAMP,
    fecha_proceso DATE,
    valor_facturado FLOAT64,
    estado STRING,
    codigo_cups STRING,
    canal_ingreso STRING,
    diagnostico STRING,
    medico STRING,
    etled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY fecha_proceso
CLUSTER BY id_cliente, codigo_cups;

-- PART 2.5: Optimized Queries

-- 1. KPIs Diarios
-- Calcula volumen y valor facturado por día, canal y estado.
SELECT 
    fecha_proceso,
    canal_ingreso,
    estado,
    COUNT(id_atencion) as total_atenciones,
    SUM(valor_facturado) as total_facturado
FROM `cala_analytics.fct_atenciones`
WHERE fecha_proceso >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) -- Ejemplo de filtrado por partición
GROUP BY 1, 2, 3;

-- 2. Recurrencia 30 días
-- Identifica clientes que han tenido más de una atención en los últimos 30 días.
WITH cliente_atenciones AS (
    SELECT 
        id_cliente,
        COUNT(id_atencion) as conteo
    FROM `cala_analytics.fct_atenciones`
    WHERE fecha_atencion >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY id_cliente
)
SELECT * FROM cliente_atenciones WHERE conteo > 1;

-- 3. Detección de duplicados
-- Verifica si hay id_atencion duplicados que sobrevivieron al pipeline.
SELECT id_atencion, COUNT(*) as conteo
FROM `cala_analytics.fct_atenciones`
GROUP BY 1
HAVING conteo > 1;

-- 4. Join eventos-atenciones
-- Une eventos de la app con atenciones realizadas.
SELECT 
    a.id_atencion,
    a.id_cliente,
    a.fecha_atencion,
    e.tipo_evento,
    e.timestamp as fecha_evento
FROM `cala_analytics.fct_atenciones` a
INNER JOIN `cala_analytics.fct_eventos_app` e 
    ON a.id_cliente = e.id_cliente
    AND DATE(a.fecha_atencion) = DATE(e.timestamp)
WHERE a.fecha_proceso >= '2025-01-01';
