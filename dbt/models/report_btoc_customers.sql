create view "postgres"."public"."report_btoc_customers"
    
    
  as (
    

SELECT
    "Country",
    COUNT(*) AS total_customers,
    AVG(chiffreaffaire) AS avg_revenue,
    SUM(chiffreaffaire) AS total_revenue,
    AVG(bandecommande) AS avg_order_band
FROM "postgres"."public"."transform_btoc_customers"
GROUP BY "Country"
  );
