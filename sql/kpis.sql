-- KPIs de cabecera del corte vertical.
--
-- Ejecutar con:  python scripts/run_sql.py sql/kpis.sql

-- Resumen ejecutivo: los números que abren el dashboard.
SELECT
    count(*)                                AS dias_con_ventas,
    sum(orders)                             AS ordenes,
    sum(net_revenue)                        AS ingreso_neto,
    sum(discounts)                          AS descuentos,
    round(avg(average_order_value), 2)      AS ticket_medio,
    min(date)                               AS desde,
    max(date)                               AS hasta
FROM ecommerce.gold.daily_sales;

-- Reconciliación Gold <-> Silver.
--
-- Es la comprobación que separa un pipeline en el que se confía de uno en el
-- que no: el agregado tiene que poder recalcularse desde su origen y dar lo
-- mismo. Si `diferencia` no es cero, Gold miente.
SELECT
    g.ingreso_gold,
    s.ingreso_silver,
    g.ingreso_gold - s.ingreso_silver AS diferencia
FROM
    (SELECT sum(net_revenue) AS ingreso_gold FROM ecommerce.gold.daily_sales) g,
    (SELECT sum(total_amount) AS ingreso_silver
     FROM ecommerce.silver.orders
     WHERE status IN ('paid', 'shipped', 'delivered')) s;

-- Trazabilidad completa del lote: de lo que llegó a lo que quedó.
SELECT
    (SELECT count(*) FROM ecommerce.bronze.orders_raw)         AS bronze_crudas,
    (SELECT count(DISTINCT order_id) FROM ecommerce.bronze.orders_raw) AS bronze_distintas,
    (SELECT count(*) FROM ecommerce.silver.orders)             AS silver_validas,
    (SELECT count(*) FROM ecommerce.silver.orders_quarantine)  AS silver_cuarentena;

-- Los diez mejores días por ingreso.
SELECT date, orders, customers, net_revenue, average_order_value
FROM ecommerce.gold.daily_sales
ORDER BY net_revenue DESC
LIMIT 10;
