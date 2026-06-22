-- =============================================================================
-- Snowflake Dynamic Tables - Real-Time Fraud Detection
-- =============================================================================
-- Dynamic Tables auto-refresh when upstream data changes, eliminating
-- the need for manual scheduling or triggers.
-- Run these in order in your Snowflake worksheet.
-- =============================================================================

-- 1. Database & Schema Setup
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS FRAUD_DB;
USE DATABASE FRAUD_DB;

CREATE SCHEMA IF NOT EXISTS STREAMING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA STREAMING;

-- 2. Raw Fraud Scores Table (written to by PySpark foreachBatch)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FRAUD_SCORES (
    user_id              VARCHAR(50),
    window_start         TIMESTAMP_NTZ,
    window_end           TIMESTAMP_NTZ,
    txn_count_1h         INTEGER,
    total_amount_1h      FLOAT,
    avg_amount_1h        FLOAT,
    stddev_amount_1h     FLOAT,
    max_amount_1h        FLOAT,
    country_count_1h     INTEGER,
    zscore_amount        FLOAT,
    zscore_fraud_flag    BOOLEAN,
    rule_high_velocity   BOOLEAN,
    rule_country_velocity BOOLEAN,
    rule_large_amount    BOOLEAN,
    is_fraud_suspected   BOOLEAN,
    fraud_score          FLOAT,
    processed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Dynamic Table: Per-Minute Fraud Summary
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.FRAUD_SUMMARY_1MIN
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    DATE_TRUNC('MINUTE', window_start)     AS minute_bucket,
    COUNT(*)                               AS total_users_flagged,
    SUM(CASE WHEN is_fraud_suspected THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN is_fraud_suspected THEN total_amount_1h ELSE 0 END) AS fraud_amount_at_risk,
    AVG(fraud_score)                       AS avg_fraud_score,
    MAX(fraud_score)                       AS max_fraud_score,
    SUM(txn_count_1h)                      AS total_txn_volume,
    ROUND(
        SUM(CASE WHEN is_fraud_suspected THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    )                                      AS fraud_rate_pct
FROM STREAMING.FRAUD_SCORES
GROUP BY DATE_TRUNC('MINUTE', window_start);


-- 4. Dynamic Table: User Risk Profiles (Rolling 24h)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.USER_RISK_PROFILE
    TARGET_LAG = '2 minutes'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    user_id,
    MAX(processed_at)                       AS last_seen_at,
    COUNT(*)                                AS total_windows,
    SUM(txn_count_1h)                       AS total_txn_24h,
    SUM(total_amount_1h)                    AS total_amount_24h,
    AVG(avg_amount_1h)                      AS avg_txn_amount_24h,
    MAX(max_amount_1h)                      AS max_single_txn_24h,
    MAX(country_count_1h)                   AS max_country_velocity,
    SUM(CASE WHEN is_fraud_suspected THEN 1 ELSE 0 END) AS fraud_flags_count,
    MAX(fraud_score)                        AS peak_fraud_score,
    AVG(fraud_score)                        AS avg_fraud_score,
    CASE
        WHEN MAX(fraud_score) >= 0.8  THEN 'CRITICAL'
        WHEN MAX(fraud_score) >= 0.5  THEN 'HIGH'
        WHEN MAX(fraud_score) >= 0.25 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                     AS risk_tier,
    SUM(CASE WHEN zscore_fraud_flag THEN 1 ELSE 0 END) AS zscore_flags,
    SUM(CASE WHEN rule_high_velocity THEN 1 ELSE 0 END) AS velocity_flags,
    SUM(CASE WHEN rule_country_velocity THEN 1 ELSE 0 END) AS geo_flags,
    SUM(CASE WHEN rule_large_amount THEN 1 ELSE 0 END) AS large_txn_flags
FROM STREAMING.FRAUD_SCORES
WHERE processed_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY user_id;


-- 5. Dynamic Table: High-Risk Alerts Feed
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.FRAUD_ALERTS
    TARGET_LAG = '30 seconds'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    user_id,
    window_start,
    fraud_score,
    CASE
        WHEN zscore_fraud_flag AND rule_high_velocity AND rule_large_amount THEN 'MULTI_SIGNAL_FRAUD'
        WHEN zscore_fraud_flag AND rule_country_velocity THEN 'GEO_ANOMALY_FRAUD'
        WHEN rule_high_velocity AND rule_large_amount THEN 'VELOCITY_FRAUD'
        WHEN zscore_fraud_flag THEN 'STATISTICAL_ANOMALY'
        WHEN rule_country_velocity THEN 'GEO_VELOCITY'
        ELSE 'RULE_BASED_FLAG'
    END                         AS alert_type,
    max_amount_1h               AS flagged_amount,
    country_count_1h            AS country_count,
    txn_count_1h                AS transaction_count,
    processed_at                AS alert_generated_at
FROM STREAMING.FRAUD_SCORES
WHERE is_fraud_suspected = TRUE
  AND processed_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
ORDER BY fraud_score DESC;


-- 6. Dynamic Table: Real-Time KPI Dashboard Feed
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DASHBOARD_KPI
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    CURRENT_TIMESTAMP()                         AS refreshed_at,
    COUNT(DISTINCT user_id)                     AS active_users_1h,
    SUM(txn_count_1h)                           AS total_transactions_1h,
    SUM(total_amount_1h)                        AS total_volume_usd_1h,
    SUM(CASE WHEN is_fraud_suspected THEN 1 ELSE 0 END)    AS fraud_alerts_1h,
    SUM(CASE WHEN is_fraud_suspected THEN total_amount_1h ELSE 0 END) AS at_risk_usd_1h,
    ROUND(
        AVG(DATEDIFF('millisecond', window_start, processed_at)),
        0
    )                                           AS avg_detection_latency_ms,
    COUNT(DISTINCT CASE WHEN risk_tier_flag = 'CRITICAL' THEN user_id END) AS critical_users
FROM (
    SELECT
        *,
        CASE
            WHEN fraud_score >= 0.8 THEN 'CRITICAL'
            ELSE 'OTHER'
        END AS risk_tier_flag
    FROM STREAMING.FRAUD_SCORES
    WHERE processed_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
);


-- 7. Helper: View for Power BI DirectQuery
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ANALYTICS.POWERBI_FRAUD_FEED AS
SELECT
    a.user_id,
    a.alert_type,
    a.fraud_score,
    a.flagged_amount,
    a.country_count,
    a.transaction_count,
    a.alert_generated_at,
    r.risk_tier,
    r.avg_fraud_score,
    r.total_amount_24h
FROM ANALYTICS.FRAUD_ALERTS a
LEFT JOIN ANALYTICS.USER_RISK_PROFILE r USING (user_id)
ORDER BY a.fraud_score DESC;

-- End of dynamic_tables.sql
