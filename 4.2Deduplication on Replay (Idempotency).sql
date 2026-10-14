-- Example Silver Layer Deduplication View/Merge for Customers
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.customers` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    postal_code,
    country,
    SAFE.PARSE_DATE('%Y-%m-%d', registration_date) AS registration_date,
    ingestion_timestamp AS updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY ingestion_timestamp DESC
    ) as row_num
  FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.customers_raw`
)
WHERE row_num = 1;