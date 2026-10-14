-- 1. Dimension: Customer
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_customer` (
  customer_sk STRING NOT NULL, -- Surrogate Key
  customer_id STRING NOT NULL,
  full_name STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  country STRING,
  registration_date DATE,
  is_active BOOL
);

-- 2. Dimension: Account
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account` (
  account_sk STRING NOT NULL,
  account_id STRING NOT NULL,
  customer_id STRING,
  account_type_id STRING,
  branch_id STRING,
  opening_date DATE,
  status STRING
);

-- 3. Dimension: Branch
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_branch` (
  branch_sk STRING NOT NULL,
  branch_id STRING NOT NULL,
  branch_name STRING,
  city STRING,
  state STRING,
  country STRING,
  manager_name STRING
);

-- 4. Dimension: Account Type
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_account_type` (
  account_type_sk STRING NOT NULL,
  account_type_id STRING NOT NULL,
  type_name STRING,
  description STRING,
  interest_rate NUMERIC(5, 4)
);

-- 5. Dimension: Date
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_date` (
  date_key INT64 NOT NULL, -- Format: YYYYMMDD
  full_date DATE NOT NULL,
  day_of_week INT64,
  day_name STRING,
  day_of_month INT64,
  month_number INT64,
  month_name STRING,
  quarter INT64,
  year INT64,
  is_weekend BOOL
);

-- 6. Fact: Transactions
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.fact_transactions` (
  transaction_id STRING NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  date_key INT64,
  customer_sk STRING,
  account_sk STRING,
  branch_sk STRING,
  account_type_sk STRING,
  amount NUMERIC(18, 2),
  currency STRING,
  transaction_type STRING,
  channel STRING
)
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY customer_sk, account_sk;

-- 7. KPI View / Aggregate Table: Monthly Customer Activity & Balances
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.vw_monthly_customer_kpis` AS
SELECT
  c.customer_id,
  c.full_name,
  DATE_TRUNC(t.transaction_timestamp, MONTH) AS transaction_month,
  COUNT(t.transaction_id) AS total_transactions,
  SUM(t.amount) AS total_transaction_amount,
  AVG(t.amount) AS avg_transaction_amount
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.fact_transactions` t
JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold.dim_customer` c ON t.customer_sk = c.customer_sk
GROUP BY 1, 2, 3;