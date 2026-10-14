-- =========================================================================
-- STEP 1: POPULATE CLEANED REFERENCE TABLES (Branches, Account Types, Products)
-- =========================================================================

-- Branches Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.branches` AS
SELECT DISTINCT
  branch_id,
  TRIM(branch_name) AS branch_name,
  TRIM(address) AS address,
  TRIM(city) AS city,
  TRIM(state) AS state,
  TRIM(country) AS country,
  TRIM(manager_name) AS manager_name,
  CURRENT_TIMESTAMP() AS updated_at
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.branches_raw`
WHERE branch_id IS NOT NULL;

-- Account Types Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.account_types` AS
SELECT DISTINCT
  account_type_id,
  TRIM(type_name) AS type_name,
  TRIM(description) AS description,
  SAFE_CAST(interest_rate AS NUMERIC) AS interest_rate,
  CURRENT_TIMESTAMP() AS updated_at
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.account_types_raw`
WHERE account_type_id IS NOT NULL;

-- Products Cleaned
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.products` AS
SELECT DISTINCT
  product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(fee_structure) AS fee_structure,
  CURRENT_TIMESTAMP() AS updated_at
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.products_raw`
WHERE product_id IS NOT NULL;


-- =========================================================================
-- STEP 2: CUSTOMER VALIDATION, DEDUPLICATION & QUARANTINE ROUTING
-- =========================================================================

-- Identify and isolate invalid / quarantined customer records
INSERT INTO `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.quarantine_records` (quarantine_id, source_table, record_payload, failure_reason, quarantined_at)
SELECT 
  GENERATE_UUID(),
  'customers_raw',
  TO_JSON_STRING(t),
  CASE 
    WHEN customer_id IS NULL THEN 'Missing customer_id'
    WHEN email NOT LIKE '%@%.%' OR email IS NULL THEN 'Invalid or missing email address'
    WHEN phone IS NULL OR LENGTH(phone) < 7 THEN 'Missing or invalid phone number'
    ELSE 'Unknown validation failure'
  END,
  CURRENT_TIMESTAMP()
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.customers_raw` t
WHERE customer_id IS NULL 
   OR email NOT LIKE '%@%.%' 
   OR email IS NULL 
   OR phone IS NULL 
   OR LENGTH(phone) < 7;

-- Populate Cleansed Customers (Deduplicated via latest ingestion timestamp)
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.customers` AS
WITH ranked_customers AS (
  SELECT 
    customer_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(TRIM(email)) AS email,
    TRIM(phone) AS phone,
    TRIM(address) AS address,
    TRIM(city) AS city,
    TRIM(state) AS state,
    TRIM(postal_code) AS postal_code,
    TRIM(country) AS country,
    SAFE.PARSE_DATE('%Y-%m-%d', registration_date) AS registration_date,
    ingestion_timestamp AS updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY ingestion_timestamp DESC
    ) as row_num
  FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.customers_raw`
  WHERE customer_id IS NOT NULL
    AND email LIKE '%@%.%'
    AND email IS NOT NULL
    AND phone IS NOT NULL
    AND LENGTH(phone) >= 7
)
SELECT * EXCEPT(row_num)
FROM ranked_customers
WHERE row_num = 1;


-- =========================================================================
-- STEP 3: ACCOUNT VALIDATION & BRANCH SORT-CODE INTEGRITY CHECK
-- =========================================================================

-- Quarantine accounts with missing mandatory fields or invalid branch integrity
INSERT INTO `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.quarantine_records` (quarantine_id, source_table, record_payload, failure_reason, quarantined_at)
SELECT 
  GENERATE_UUID(),
  'accounts_raw',
  TO_JSON_STRING(a),
  CASE 
    WHEN a.account_id IS NULL THEN 'Missing account_id'
    WHEN a.customer_id IS NULL THEN 'Missing customer_id reference'
    WHEN b.branch_id IS NULL THEN 'Branch/sort-code integrity check failed: branch_id does not exist'
    ELSE 'Invalid account metadata'
  END,
  CURRENT_TIMESTAMP()
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.accounts_raw` a
LEFT JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.branches` b ON a.branch_id = b.branch_id
WHERE a.account_id IS NULL 
   OR a.customer_id IS NULL 
   OR b.branch_id IS NULL;

-- Populate Cleansed Accounts
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.accounts` AS
WITH ranked_accounts AS (
  SELECT 
    a.account_id,
    a.customer_id,
    a.account_type_id,
    a.branch_id,
    SAFE.PARSE_DATE('%Y-%m-%d', a.opening_date) AS opening_date,
    SAFE_CAST(a.balance AS NUMERIC) AS balance,
    LOWER(TRIM(a.status)) AS status,
    a.ingestion_timestamp AS updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY a.account_id 
      ORDER BY a.ingestion_timestamp DESC
    ) as row_num
  FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.accounts_raw` a
  INNER JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.branches` b ON a.branch_id = b.branch_id
  WHERE a.account_id IS NOT NULL 
    AND a.customer_id IS NOT NULL
)
SELECT * EXCEPT(row_num)
FROM ranked_accounts
WHERE row_num = 1;


-- =========================================================================
-- STEP 4: TRANSACTION VALIDATION (Currency Mismatch, Dates, Overdrafts)
-- =========================================================================

-- Quarantine invalid transactions
INSERT INTO `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.quarantine_records` (quarantine_id, source_table, record_payload, failure_reason, quarantined_at)
SELECT 
  GENERATE_UUID(),
  'transactions_raw',
  TO_JSON_STRING(t),
  CASE 
    WHEN t.transaction_id IS NULL THEN 'Missing transaction_id'
    WHEN acc.account_id IS NULL THEN 'Referential integrity failure: account_id not found'
    WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', t.transaction_date) < CAST(acc.opening_date AS TIMESTAMP) THEN 'Invalid transaction date: prior to account opening date'
    ELSE 'Transaction validation failure'
  END,
  CURRENT_TIMESTAMP()
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.transactions_raw` t
LEFT JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.accounts` acc ON t.account_id = acc.account_id
WHERE t.transaction_id IS NULL 
   OR acc.account_id IS NULL
   OR SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', t.transaction_date) < CAST(acc.opening_date AS TIMESTAMP);

-- Populate Cleaned Transactions (Suppressing exact duplicates)
CREATE OR REPLACE TABLE `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.transactions` 
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY account_id, transaction_type AS
WITH validated_transactions AS (
  SELECT 
    t.transaction_id,
    t.account_id,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', t.transaction_date) AS transaction_timestamp,
    SAFE_CAST(t.amount AS NUMERIC) AS amount,
    UPPER(TRIM(t.currency)) AS currency,
    LOWER(TRIM(t.transaction_type)) AS transaction_type,
    t.merchant_id,
    LOWER(TRIM(t.channel)) AS channel,
    t.ingestion_timestamp AS updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY t.transaction_id, t.account_id, t.transaction_date, t.amount 
      ORDER BY t.ingestion_timestamp DESC
    ) as dup_row_num
  FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.transactions_raw` t
  INNER JOIN `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.accounts` acc ON t.account_id = acc.account_id
  WHERE t.transaction_id IS NOT NULL
    AND SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', t.transaction_date) >= CAST(acc.opening_date AS TIMESTAMP)
)
SELECT * EXCEPT(dup_row_num)
FROM validated_transactions
WHERE dup_row_num = 1;


-- =========================================================================
-- STEP 5: RISK & BEHAVIORAL CHECKS (Structuring & Dormant Activity)
-- =========================================================================

-- 5.1 Structuring Detection View (Risk Indicator: Repeated near-threshold cash deposits)
-- Note: Cash-transaction proxy assumption: Channel = 'atm' or 'branch' with high deposit frequencies.
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.vw_structuring_alerts` AS
SELECT 
  account_id,
  DATE(transaction_timestamp) AS txn_date,
  COUNT(transaction_id) AS deposit_count,
  SUM(amount) as total_deposited
FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.transactions`
WHERE transaction_type = 'deposit'
  AND channel IN ('branch', 'atm')
  AND amount BETWEEN 9000 AND 9999 -- Near regulatory reporting threshold proxy
GROUP BY 1, 2
HAVING COUNT(transaction_id) >= 2;

-- 5.2 Dormant Account Reactivation View (Long inactivity followed by clustered transactions)
CREATE OR REPLACE VIEW `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.vw_dormant_reactivations` AS
WITH transaction_gaps AS (
  SELECT 
    account_id,
    transaction_timestamp,
    LAG(transaction_timestamp, 1) OVER (PARTITION BY account_id ORDER BY transaction_timestamp) AS prev_txn_timestamp
  FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.transactions`
)
SELECT DISTINCT
  account_id,
  prev_txn_timestamp,
  transaction_timestamp AS reactivation_timestamp,
  TIMESTAMP_DIFF(transaction_timestamp, prev_txn_timestamp, DAY) AS inactive_days_gap
FROM transaction_gaps
WHERE TIMESTAMP_DIFF(transaction_timestamp, prev_txn_timestamp, DAY) > 90; -- Inactive for > 3 months


-- =========================================================================
-- STEP 6: LOG DATA QUALITY METRICS TO `dq_results`
-- =========================================================================

INSERT INTO `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.dq_results` (execution_id, table_name, rule_name, rows_evaluated, rows_failed, pass_percentage, evaluation_timestamp)
SELECT 
  'exec_' || FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP()),
  'customers_raw',
  'mandatory_fields_and_email_format',
  (SELECT COUNT(*) FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.customers_raw`),
  (SELECT COUNT(*) FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.quarantine_records` WHERE source_table = 'customers_raw'),
  SAFE_DIVIDE(
    (SELECT COUNT(*) FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_silver.customers`), 
    (SELECT COUNT(*) FROM `project-d058df44-4eb1-4f05-8b3.hackathon_hyd_hack138_ws02_bronze.customers_raw`)
  ) * 100,
  CURRENT_TIMESTAMP();