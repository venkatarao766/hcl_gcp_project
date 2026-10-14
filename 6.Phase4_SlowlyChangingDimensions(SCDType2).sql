--Step 1: Populate the Date Dimension (dim_date)
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_date` AS
SELECT
  CAST(FORMAT_DATE('%Y%m%d', d) AS INT64) AS date_key,
  d AS full_date,
  EXTRACT(DAYOFWEEK FROM d) AS day_of_week,
  FORMAT_DATE('%A', d) AS day_name,
  EXTRACT(DAY FROM d) AS day_of_month,
  EXTRACT(MONTH FROM d) AS month_number,
  FORMAT_DATE('%B', d) AS month_name,
  EXTRACT(QUARTER FROM d) AS quarter,
  EXTRACT(YEAR FROM d) AS year,
  CASE WHEN EXTRACT(DAYOFWEEK FROM d) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2030-12-31', INTERVAL 1 DAY)) AS d;

--Step 2: Populate Static Dimensions (dim_branch, dim_account_type)
-- Branch Dimension
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_branch` AS
SELECT
  GENERATE_UUID() AS branch_sk,
  branch_id,
  branch_name,
  city,
  state,
  country,
  manager_name
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.branches`;

-- Account Type Dimension
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account_type` AS
SELECT
  GENERATE_UUID() AS account_type_sk,
  account_type_id,
  type_name,
  description,
  interest_rate
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.account_types`;

-- Step 3: SCD Type 2 Implementation (dim_customer & dim_account)
-- This logic tracks attribute mutations between Day 0, Day 1, and Day 2, closing old versions (effective_to, is_current = FALSE) and opening new versions (effective_from, is_current = TRUE).

-- 3.1 Customer SCD Type 2
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_customer` AS
WITH source_changes AS (
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    country,
    registration_date AS effective_from_ts,
    COALESCE(
      LEAD(registration_date) OVER (PARTITION BY customer_id ORDER BY registration_date),
      TIMESTAMP('9999-12-31 23:59:59')
    ) AS effective_to_ts
  FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.customers`
)
SELECT
  GENERATE_UUID() AS customer_sk,
  customer_id,
  CONCAT(first_name, ' ', last_name) AS full_name,
  email,
  phone,
  city,
  state,
  country,
  CAST(effective_from_ts AS DATE) AS effective_from,
  CAST(
    CASE WHEN effective_to_ts = TIMESTAMP('9999-12-31 23:59:59') 
         THEN '9999-12-31' 
         ELSE CAST(effective_to_ts AS STRING) 
    END AS DATE
  ) AS effective_to,
  CASE WHEN effective_to_ts = TIMESTAMP('9999-12-31 23:59:59') THEN TRUE ELSE FALSE END AS is_current
FROM source_changes;

-- 3.2 Account SCD Type 2
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account` AS
WITH source_changes AS (
  SELECT
    account_id,
    customer_id,
    account_type_id,
    branch_id,
    status,
    opening_date AS effective_from_date,
    COALESCE(
      LEAD(opening_date) OVER (PARTITION BY account_id ORDER BY opening_date),
      DATE('9999-12-31')
    ) AS effective_to_date
  FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.accounts`
)
SELECT
  GENERATE_UUID() AS account_sk,
  account_id,
  customer_id,
  account_type_id,
  branch_id,
  status,
  effective_from_date AS effective_from,
  effective_to_date AS effective_to,
  CASE WHEN effective_to_date = DATE('9999-12-31') THEN TRUE ELSE FALSE END AS is_current
FROM source_changes;

-- Step 4: Populate Transaction Fact Table (fact_transactions)
-- Joins cleaned transactions with surrogate keys from dimensions using valid effective date ranges.
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.fact_transactions` 
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY customer_sk, account_sk AS
SELECT
  t.transaction_id,
  t.transaction_timestamp,
  d.date_key,
  c.customer_sk,
  a.account_sk,
  b.branch_sk,
  at.account_type_sk,
  t.amount,
  t.currency,
  t.transaction_type,
  t.channel
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.transactions` t
JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account` a 
  ON t.account_id = a.account_id 
  AND DATE(t.transaction_timestamp) BETWEEN a.effective_from AND a.effective_to
JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_customer` c 
  ON a.customer_id = c.customer_id 
  AND DATE(t.transaction_timestamp) BETWEEN c.effective_from AND c.effective_to
LEFT JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_branch` b 
  ON a.branch_id = b.branch_id
LEFT JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account_type` at 
  ON a.account_type_id = at.account_type_id
LEFT JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_date` d 
  ON CAST(FORMAT_DATE('%Y%m%d', DATE(t.transaction_timestamp)) AS INT64) = d.date_key;


-- Step 5: Gold Analytics & Required KPI Views
-- 1. Top 5 Customers KPI (Ranked by Total Transaction Value)
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.vw_kpi_top_5_customers` AS
SELECT
  c.customer_id,
  c.full_name,
  COUNT(f.transaction_id) AS total_transactions,
  SUM(f.amount) AS total_transaction_value -- Chosen measure: Total monetary volume moved
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.fact_transactions` f
JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_customer` c ON f.customer_sk = c.customer_sk
WHERE c.is_current = TRUE
GROUP BY 1, 2
ORDER BY total_transaction_value DESC
LIMIT 5;

-- 2. Product-Wise Revenue Contribution KPI
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.vw_kpi_product_contribution` AS
SELECT
  at.type_name AS account_type,
  COUNT(DISTINCT f.account_sk) AS active_accounts,
  COUNT(f.transaction_id) AS total_transactions,
  SUM(f.amount) AS total_transaction_value
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.fact_transactions` f
JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account_type` at ON f.account_type_sk = at.account_type_sk
GROUP BY 1
ORDER BY total_transaction_value DESC;

-- 3. Inactive Accounts KPI (No activity in the last 3 months / 90 days)
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.vw_kpi_inactive_accounts` AS
SELECT
  a.account_id,
  a.customer_id,
  a.status,
  MAX(f.transaction_timestamp) AS last_transaction_timestamp,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(f.transaction_timestamp), DAY) AS days_since_last_activity
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account` a
LEFT JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.fact_transactions` f ON a.account_sk = f.account_sk
GROUP BY 1, 2, 3
HAVING last_transaction_timestamp IS NULL 
   OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(f.transaction_timestamp), DAY) > 90;

-- 4. Suspicious Transactions KPI (Transactions exceeding £100,000 threshold)
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.vw_kpi_suspicious_high_value` AS
SELECT
  transaction_id,
  transaction_timestamp,
  customer_sk,
  account_sk,
  amount,
  currency,
  channel
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.fact_transactions`
WHERE amount >= 100000;

-- 5. Loan Health & Card Adoption Portfolio Views (Conditional on Source Tables)
-- Loan Portfolio & Repayment Health KPI view (if loan entity is populated in silver)
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.vw_kpi_loan_portfolio_health` AS
SELECT
  'Portfolio Status' AS metric_category,
  COUNT(*) AS total_loans,
  SUM(CASE WHEN status = 'Default' THEN 1 ELSE 0 END) AS defaulted_loans
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.accounts` -- Expand when loans_raw/silver.loans is integrated
WHERE status LIKE '%loan%';

-- Card Product Adoption & Cross-Sell KPI view
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.vw_kpi_card_adoption` AS
SELECT
  c.customer_id,
  c.full_name,
  COUNT(DISTINCT a.account_id) AS total_accounts
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_customer` c
JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account` a ON c.customer_id = a.customer_id
WHERE c.is_current = TRUE
GROUP BY 1, 2;
