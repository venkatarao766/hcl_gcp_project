-- Create datasets if they don't exist
CREATE SCHEMA IF NOT EXISTS `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze`;
CREATE SCHEMA IF NOT EXISTS `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver`;
CREATE SCHEMA IF NOT EXISTS `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_gold`;

-- 1. Customers Raw
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.customers_raw` (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  address STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  country STRING,
  registration_date STRING,
  raw_payload STRING, -- Full JSON payload if ingested via Pub/Sub or API
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source_file_name STRING
);

-- 2. Accounts Raw
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.accounts_raw` (
  account_id STRING,
  customer_id STRING,
  account_type_id STRING,
  branch_id STRING,
  opening_date STRING,
  balance STRING,
  status STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source_file_name STRING
);

-- 3. Transactions Raw
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.transactions_raw` (
  transaction_id STRING,
  account_id STRING,
  transaction_date STRING,
  amount STRING,
  currency STRING,
  transaction_type STRING,
  merchant_id STRING,
  channel STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source_file_name STRING
)
PARTITION BY DATE(ingestion_timestamp);

-- 4. Branches Raw
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.branches_raw` (
  branch_id STRING,
  branch_name STRING,
  address STRING,
  city STRING,
  state STRING,
  country STRING,
  manager_name STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Account Types Raw
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.account_types_raw` (
  account_type_id STRING,
  type_name STRING,
  description STRING,
  interest_rate STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 6. Products Raw
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.products_raw` (
  product_id STRING,
  product_name STRING,
  category STRING,
  fee_structure STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Ingestion Audit Table
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.ingestion_audit` (
  job_id STRING,
  pipeline_name STRING,
  source_system STRING,
  table_name STRING,
  rows_read INT64,
  rows_written INT64,
  status STRING, -- SUCCESS, FAILED, WARNING
  error_message STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP
);

-- 8. Metadata & Configuration Tables
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.config_pipeline_settings` (
  config_key STRING,
  config_value STRING,
  description STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

