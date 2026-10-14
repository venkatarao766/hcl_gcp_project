-- 1. Customers Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.customers` (
  customer_id STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  address STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  country STRING,
  registration_date DATE,
  updated_at TIMESTAMP
);

-- 2. Accounts Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.accounts` (
  account_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  account_type_id STRING,
  branch_id STRING,
  opening_date DATE,
  balance NUMERIC(18, 2),
  status STRING,
  updated_at TIMESTAMP
);

-- 3. Transactions Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.transactions` (
  transaction_id STRING NOT NULL,
  account_id STRING NOT NULL,
  transaction_timestamp TIMESTAMP,
  amount NUMERIC(18, 2),
  currency STRING,
  transaction_type STRING,
  merchant_id STRING,
  channel STRING,
  updated_at TIMESTAMP
)
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY account_id, transaction_type;

-- 4. Branches Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.branches` (
  branch_id STRING NOT NULL,
  branch_name STRING,
  address STRING,
  city STRING,
  state STRING,
  country STRING,
  manager_name STRING,
  updated_at TIMESTAMP
);

-- 5. Account Types Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.account_types` (
  account_type_id STRING NOT NULL,
  type_name STRING,
  description STRING,
  interest_rate NUMERIC(5, 4),
  updated_at TIMESTAMP
);

-- 6. Products Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.products` (
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  fee_structure STRING,
  updated_at TIMESTAMP
);

-- 7. Data Quality (DQ) Results Table
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.dq_results` (
  execution_id STRING,
  table_name STRING,
  rule_name STRING,
  rows_evaluated INT64,
  rows_failed INT64,
  pass_percentage FLOAT64,
  evaluation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 8. Quarantine Records (Failed validation logs)
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.quarantine_records` (
  quarantine_id STRING,
  source_table STRING,
  record_payload STRING,
  failure_reason STRING,
  quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);