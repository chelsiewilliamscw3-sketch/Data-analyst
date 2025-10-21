
-- STEPS Project Simulation: SQL DDL + Helpful Queries

-- 1) FEATURES
CREATE TABLE features (
  feature_code VARCHAR(10) PRIMARY KEY,
  feature_name VARCHAR(100),
  module VARCHAR(50),
  target_sla_hours DECIMAL(5,2)
);

-- 2) USERS
CREATE TABLE users (
  user_id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(100),
  department VARCHAR(50),
  role VARCHAR(50),
  region VARCHAR(20),
  is_active TINYINT
);

-- 3) COSTS (monthly)
CREATE TABLE costs_monthly (
  month CHAR(7),          -- YYYY-MM
  infra_cost DECIMAL(12,2),
  support_cost DECIMAL(12,2),
  dev_cost DECIMAL(12,2),
  other_cost DECIMAL(12,2)
);

-- 4) TRANSACTIONS
CREATE TABLE transactions (
  transaction_id VARCHAR(15) PRIMARY KEY,
  user_id VARCHAR(10) REFERENCES users(user_id),
  region VARCHAR(20),
  feature_code VARCHAR(10) REFERENCES features(feature_code),
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  cycle_hours DECIMAL(6,2),
  status VARCHAR(20),            -- Completed / Failed / Reprocessed
  error_code VARCHAR(30),        -- nullable
  amount_usd DECIMAL(12,2)
);

-- 5) TARGETS (monthly KPIs)
CREATE TABLE targets (
  month CHAR(7),       -- YYYY-MM
  target_avg_cycle_hours DECIMAL(6,2),
  target_error_rate_pct DECIMAL(5,2),
  target_cost_per_txn DECIMAL(10,2)
);

-- ==============================
-- Helpful Queries for KPIs
-- ==============================

-- Transactions per month
SELECT DATE_TRUNC('month', start_time) AS month,
       COUNT(*) AS transactions
FROM transactions
GROUP BY 1
ORDER BY 1;

-- Avg processing (cycle) time by month
SELECT DATE_TRUNC('month', start_time) AS month,
       AVG(cycle_hours) AS avg_cycle_hours
FROM transactions
WHERE status IN ('Completed','Reprocessed')
GROUP BY 1
ORDER BY 1;

-- Error rate by month
WITH monthly AS (
  SELECT DATE_TRUNC('month', start_time) AS month,
         COUNT(*) AS total_txn,
         SUM(CASE WHEN status='Failed' THEN 1 ELSE 0 END) AS failed_txn
  FROM transactions
  GROUP BY 1
)
SELECT month,
       100.0 * failed_txn / NULLIF(total_txn,0) AS error_rate_pct
FROM monthly
ORDER BY month;

-- Feature utilization (volume + share)
WITH vol AS (
  SELECT feature_code, COUNT(*) AS cnt
  FROM transactions
  GROUP BY 1
)
SELECT f.feature_name, v.cnt,
       100.0 * v.cnt / SUM(v.cnt) OVER () AS feature_share_pct
FROM vol v
JOIN features f ON f.feature_code = v.feature_code
ORDER BY v.cnt DESC;

-- Cost per transaction by month (allocate monthly costs across transactions)
WITH txn AS (
  SELECT DATE_TRUNC('month', start_time) AS month,
         COUNT(*) AS txn_count
  FROM transactions
  GROUP BY 1
),
c AS (
  SELECT TO_DATE(month || '-01','YYYY-MM-DD') AS month_dt,
         infra_cost + support_cost + dev_cost + other_cost AS total_cost
  FROM costs_monthly
)
SELECT c.month_dt AS month,
       c.total_cost / NULLIF(t.txn_count,0) AS cost_per_txn
FROM c
LEFT JOIN txn t ON DATE_TRUNC('month', c.month_dt) = DATE_TRUNC('month', t.month)
ORDER BY 1;

-- Compare against targets (cycle time + error rate + cost per txn)
WITH avg_cycle AS (
  SELECT DATE_TRUNC('month', start_time) AS month,
         AVG(cycle_hours) AS avg_cycle_hours
  FROM transactions
  WHERE status IN ('Completed','Reprocessed')
  GROUP BY 1
),
err AS (
  SELECT DATE_TRUNC('month', start_time) AS month,
         100.0 * SUM(CASE WHEN status='Failed' THEN 1 ELSE 0 END) / COUNT(*) AS error_rate_pct
  FROM transactions
  GROUP BY 1
),
cpt AS (
  WITH txn AS (
    SELECT DATE_TRUNC('month', start_time) AS month,
           COUNT(*) AS txn_count
    FROM transactions
    GROUP BY 1
  ),
  c AS (
    SELECT TO_DATE(month || '-01','YYYY-MM-DD') AS month_dt,
           infra_cost + support_cost + dev_cost + other_cost AS total_cost
    FROM costs_monthly
  )
  SELECT c.month_dt AS month,
         c.total_cost / NULLIF(t.txn_count,0) AS cost_per_txn
  FROM c LEFT JOIN txn t ON DATE_TRUNC('month', c.month_dt) = DATE_TRUNC('month', t.month)
)
SELECT TO_CHAR(a.month,'YYYY-MM') AS month,
       a.avg_cycle_hours,
       t.target_avg_cycle_hours,
       e.error_rate_pct,
       t.target_error_rate_pct,
       p.cost_per_txn,
       t.target_cost_per_txn
FROM avg_cycle a
JOIN err e ON e.month = a.month
JOIN cpt p ON p.month = a.month
JOIN targets t ON t.month = TO_CHAR(a.month,'YYYY-MM')
ORDER BY month;

-- Region & department performance snapshot
SELECT u.region, u.department,
       COUNT(t.transaction_id) AS volume,
       AVG(t.cycle_hours) AS avg_cycle_hours,
       100.0 * SUM(CASE WHEN t.status='Failed' THEN 1 ELSE 0 END) / COUNT(*) AS error_rate_pct
FROM transactions t
JOIN users u ON u.user_id = t.user_id
GROUP BY u.region, u.department
ORDER BY u.region, u.department;
