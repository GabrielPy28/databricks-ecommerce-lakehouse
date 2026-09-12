-- Embudo de marketing sobre `gold.marketing_funnel`.
--
-- Ejecutar con:  python scripts/run_sql.py sql/marketing_funnel.sql
--
-- Todas las cifras cuentan **sesiones, no eventos**: tres vistas de página de
-- la misma visita son una visita. Contando eventos, la conversión quedaría
-- dividida por el número de páginas que el visitante mirase, y un canal que
-- trae gente curiosa parecería peor que uno que trae gente que rebota.

-- Embudo agregado de todo el periodo, paso a paso.
SELECT
    'Vistas de página' AS paso, sum(page_views)   AS sesiones, 1 AS orden FROM ecommerce.gold.marketing_funnel
UNION ALL SELECT
    'Añadió al carrito', sum(add_to_carts),  2 FROM ecommerce.gold.marketing_funnel
UNION ALL SELECT
    'Inició el pago',    sum(checkouts),     3 FROM ecommerce.gold.marketing_funnel
UNION ALL SELECT
    'Compró',            sum(purchases),     4 FROM ecommerce.gold.marketing_funnel
ORDER BY orden;

-- Rendimiento por canal: dónde se pierde la gente en cada paso.
--
-- Un canal con buena conversión y poco volumen es una oportunidad de invertir;
-- uno con mucho volumen y mala conversión, un problema de segmentación.
SELECT
    channel,
    sum(page_views)     AS visitas,
    sum(purchases)      AS compras,
    round(100.0 * sum(add_to_carts) / nullif(sum(page_views), 0), 1)   AS pct_al_carrito,
    round(100.0 * sum(checkouts)    / nullif(sum(add_to_carts), 0), 1) AS pct_a_pago,
    round(100.0 * sum(purchases)    / nullif(sum(checkouts), 0), 1)    AS pct_pago_completado,
    round(100.0 * sum(purchases)    / nullif(sum(page_views), 0), 2)   AS conversion_global
FROM ecommerce.gold.marketing_funnel
GROUP BY channel
ORDER BY visitas DESC;

-- Evolución semanal de la conversión: separa la tendencia del ruido diario.
SELECT
    date_trunc('week', date) AS semana,
    sum(page_views)          AS visitas,
    sum(purchases)           AS compras,
    round(100.0 * sum(purchases) / nullif(sum(page_views), 0), 2) AS conversion_pct
FROM ecommerce.gold.marketing_funnel
GROUP BY semana
ORDER BY semana DESC
LIMIT 20;

-- Dónde se pierde más gente en términos absolutos: el paso con mayor caída es
-- el que más rinde arreglar.
SELECT
    sum(page_views)   - sum(add_to_carts) AS perdidos_antes_del_carrito,
    sum(add_to_carts) - sum(checkouts)    AS perdidos_antes_del_pago,
    sum(checkouts)    - sum(purchases)    AS perdidos_durante_el_pago
FROM ecommerce.gold.marketing_funnel;
