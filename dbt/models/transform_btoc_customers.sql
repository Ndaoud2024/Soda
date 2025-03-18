create  table "postgres"."public"."transform_btoc_customers__dbt_tmp_workflow"
    as
  (
WITH cleaned_data AS (
    SELECT
        id,
        created_at,
        LOWER("Gender") AS gender_normalized, 
        "Title",
        "FirstName",
        "Surname",
        "StreetAddress",
        "Country",
        "CountryFull",
        "EmailAddress",
        chiffreaffaire,
        bandecommande
    FROM "postgres"."public"."btoc_customers"  
    WHERE "EmailAddress" IS NOT NULL  
)
SELECT *
FROM cleaned_data
  );
  
