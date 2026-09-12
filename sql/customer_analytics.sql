-- Analítica de clientes sobre las tablas Gold.
--
-- Ejecutar con:  python scripts/run_sql.py sql/customer_analytics.sql

-- Composición de la base de clientes por segmento RFM.
--
-- Es la vista que justifica la segmentación: si los segmentos no se
-- diferencian en gasto y recencia, no sirven para decidir nada.
SELECT
    segment,
    count(*)                            AS clientes,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_base,
    round(avg(total_spend), 2)          AS gasto_medio,
    round(sum(total_spend), 2)          AS gasto_total,
    round(avg(orders), 1)               AS ordenes_medias,
    round(avg(recency_days))            AS dias_sin_comprar
FROM ecommerce.gold.customer_segments
GROUP BY segment
ORDER BY gasto_total DESC;

-- Concentración del ingreso: qué parte del negocio depende de qué parte de los
-- clientes. Responde a la pregunta incómoda de cuánto se perdería si se fueran
-- los diez mejores.
WITH ordenado AS (
    SELECT
        customer_id,
        total_spend,
        row_number() OVER (ORDER BY total_spend DESC) AS puesto,
        count(*)    OVER ()                           AS total_clientes,
        sum(total_spend) OVER ()                      AS ingreso_total
    FROM ecommerce.gold.customer_lifetime_value
)
SELECT
    CASE
        WHEN puesto <= total_clientes * 0.01 THEN '01 - Top 1%'
        WHEN puesto <= total_clientes * 0.05 THEN '02 - Top 5%'
        WHEN puesto <= total_clientes * 0.20 THEN '03 - Top 20%'
        WHEN puesto <= total_clientes * 0.50 THEN '04 - Top 50%'
        ELSE                                      '05 - Resto'
    END                                                   AS tramo,
    count(*)                                              AS clientes,
    round(sum(total_spend), 2)                            AS ingreso,
    round(100.0 * sum(total_spend) / max(ingreso_total), 1) AS pct_del_ingreso
FROM ordenado
GROUP BY tramo
ORDER BY tramo;

-- Valor de los clientes por país, con su versión vigente.
SELECT
    country,
    count(*)                   AS clientes,
    round(sum(total_spend), 2) AS ingreso,
    round(avg(average_order_value), 2) AS ticket_medio
FROM ecommerce.gold.customer_lifetime_value
WHERE country IS NOT NULL
GROUP BY country
ORDER BY ingreso DESC;

-- Clientes en riesgo con más valor: a quién merece la pena intentar recuperar.
SELECT
    customer_id,
    country,
    segment,
    orders,
    total_spend,
    recency_days,
    last_order
FROM ecommerce.gold.customer_segments
WHERE segment = 'At Risk'
ORDER BY total_spend DESC
LIMIT 20;
