-- Analítica de producto y catálogo sobre las tablas Gold.
--
-- Ejecutar con:  python scripts/run_sql.py sql/product_analytics.sql

-- Los veinte productos que más ingreso dejan, con su margen real.
--
-- El coste está valorado al que estaba en vigor en la fecha de cada venta,
-- tomado del historial SCD2 de `products`. Con el coste actual, cualquier
-- cambio de precio de compra reescribiría el margen histórico.
SELECT
    product_name,
    category,
    units_sold,
    revenue,
    profit,
    round(margin_pct, 1)  AS margen_pct,
    round(return_rate, 1) AS devolucion_pct
FROM ecommerce.gold.product_performance
ORDER BY revenue DESC
LIMIT 20;

-- Productos que venden bien pero dejan poco margen: el caso que un ranking por
-- ingresos esconde y que suele ser el más rentable de corregir.
SELECT
    product_name,
    category,
    units_sold,
    revenue,
    round(margin_pct, 1) AS margen_pct
FROM ecommerce.gold.product_performance
WHERE units_sold > 0
ORDER BY revenue DESC, margin_pct ASC
LIMIT 15;

-- Productos con devolución anormalmente alta. Una tasa alta con volumen alto
-- apunta a un problema de producto o de descripción, no a mala suerte.
SELECT
    product_name,
    category,
    units_sold,
    returned_units,
    round(return_rate, 1) AS devolucion_pct,
    refunded_amount
FROM ecommerce.gold.product_performance
WHERE units_sold >= 5
ORDER BY return_rate DESC
LIMIT 15;

-- Evolución mensual por categoría: qué partes del catálogo crecen.
SELECT
    category,
    month,
    orders,
    units_sold,
    revenue,
    profit,
    round(100.0 * profit / nullif(revenue, 0), 1) AS margen_pct
FROM ecommerce.gold.category_performance
ORDER BY month DESC, revenue DESC;

-- Crecimiento mes contra mes por categoría.
SELECT
    category,
    month,
    revenue,
    lag(revenue) OVER (PARTITION BY category ORDER BY month) AS mes_anterior,
    round(
        100.0 * (revenue - lag(revenue) OVER (PARTITION BY category ORDER BY month))
        / nullif(lag(revenue) OVER (PARTITION BY category ORDER BY month), 0),
        1
    ) AS crecimiento_pct
FROM ecommerce.gold.category_performance
ORDER BY category, month;
