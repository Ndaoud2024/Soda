{{ config(materialized='view') }}  -- dbt va créer la vue pour toi

SELECT
    "Country",
    COUNT(*) AS total_customers,
    AVG(chiffreaffaire) AS avg_revenue,
    SUM(chiffreaffaire) AS total_revenue,
    AVG(bandecommande) AS avg_order_band
FROM {{ ref('transform_btoc_customers') }}  -- dbt gère les dépendances ici
GROUP BY "Country"
