# Power BI Real-Time Dashboard Setup Guide

This guide walks through connecting Power BI to the fraud detection pipeline's Snowflake Dynamic Tables for real-time dashboards and automated alerts.

---

## Prerequisites

- Power BI Desktop (latest version)
- Power BI Pro or Premium license (for auto-refresh < 1 hour)
- Snowflake account with access to `FRAUD_DB.ANALYTICS`
- Snowflake ODBC driver installed

---

## Step 1: Connect Power BI to Snowflake

1. Open Power BI Desktop
2. Click **Get Data** > **Snowflake**
3. Enter connection details:
   - **Server**: `<your-account>.snowflakecomputing.com`
   - **Warehouse**: `COMPUTE_WH`
   - **Database**: `FRAUD_DB`
   - **Schema**: `ANALYTICS`
4. Select **DirectQuery** mode (required for real-time data)
5. Authenticate with your Snowflake credentials

---

## Step 2: Import Views

Import the following views/tables:

| View / Table | Description |
|---|---|
| `POWERBI_FRAUD_FEED` | Main feed: live fraud alerts + user risk |
| `FRAUD_SUMMARY_1MIN` | Per-minute fraud volume and rate |
| `USER_RISK_PROFILE` | User-level risk scores and tiers |
| `FRAUD_ALERTS` | Active fraud alert events |
| `DASHBOARD_KPI` | Real-time KPI metrics |

---

## Step 3: Build Dashboard Visuals

### Page 1: Executive Overview

| Visual | Field | Purpose |
|--------|-------|---------|
| KPI Card | `fraud_alerts_1h` | Total alerts in last hour |
| KPI Card | `at_risk_usd_1h` | USD at risk |
| KPI Card | `avg_detection_latency_ms` | Detection speed |
| KPI Card | `fraud_rate_pct` | Fraud rate % |
| Line Chart | `minute_bucket` vs `fraud_count` | Fraud trend over time |
| Gauge | `fraud_rate_pct` | Live fraud rate gauge |

### Page 2: User Risk Heatmap

| Visual | Field | Purpose |
|--------|-------|---------|
| Table | `user_id`, `risk_tier`, `peak_fraud_score` | User risk ranking |
| Bar Chart | `risk_tier` count | Risk tier distribution |
| Scatter Plot | `total_amount_24h` vs `fraud_flags_count` | Risk vs. volume |
| Slicer | `risk_tier` | Filter by CRITICAL/HIGH/MEDIUM/LOW |

### Page 3: Alert Detail Feed

| Visual | Field | Purpose |
|--------|-------|---------|
| Table | All `FRAUD_ALERTS` columns | Live alert feed |
| Donut Chart | `alert_type` distribution | Alert breakdown |
| Map | `merchant_country` (if available) | Geographic fraud |

---

## Step 4: Configure Auto-Refresh

### DirectQuery (Recommended for Real-Time)

1. In Power BI Desktop, go to **File > Options > Current File > DirectQuery**
2. Enable **Automatic page refresh**
3. Set refresh interval to **30 seconds** or **1 minute**

### Scheduled Refresh (Power BI Service)

1. Publish the report to Power BI Service
2. Go to **Dataset Settings > Scheduled Refresh**
3. Set to refresh every **15 minutes** (minimum without Premium)
4. With Premium: set to every **1 second** using DirectQuery

---

## Step 5: Set Up Alerts

### Alert 1: High Fraud Rate

1. Pin the `fraud_rate_pct` KPI card to a Dashboard
2. Click the bell icon on the pinned tile
3. Set condition: **Above 5%**
4. Notification: Email + mobile push

### Alert 2: Critical User Detected

1. Pin the `critical_users` KPI card
2. Alert condition: **Above 0**
3. Add webhook to Slack using Power Automate:
   - Trigger: Power BI Data Alert
   - Action: Post Slack message with user details

### Alert 3: Detection Latency Spike

1. Pin `avg_detection_latency_ms` card
2. Alert condition: **Above 2000ms**
3. Escalate via email to on-call engineer

---

## Step 6: Power Automate Integration

For advanced alerting, use Power Automate:

```
Trigger: Power BI Data Alert (fraud_rate > 5%)
   |
   v
Action 1: Get Snowflake rows from FRAUD_ALERTS
   |
   v
Action 2: Filter rows WHERE fraud_score > 0.8
   |
   v
Action 3: Post to Slack #fraud-alerts channel
   |
   v
Action 4: Send email to fraud-team@company.com
   |
   v
Action 5: Create incident in PagerDuty
```

---

## Step 7: DAX Measures for Advanced Analytics

```dax
-- Fraud Rate (last 1 hour)
Fraud Rate % = 
    DIVIDE(
        COUNTROWS(FILTER(FRAUD_ALERTS, FRAUD_ALERTS[fraud_score] > 0.5)),
        COUNTROWS(FRAUD_SCORES),
        0
    ) * 100

-- Average Detection Latency
Avg Latency (ms) = 
    AVERAGEX(
        DASHBOARD_KPI,
        DASHBOARD_KPI[avg_detection_latency_ms]
    )

-- At-Risk Amount (USD)
At Risk USD = 
    SUMX(
        FILTER(USER_RISK_PROFILE, USER_RISK_PROFILE[risk_tier] = "CRITICAL"),
        USER_RISK_PROFILE[total_amount_24h]
    )
```

---

## Dashboard Color Schema

| Risk Level | Color | Hex |
|---|---|---|
| CRITICAL | Red | `#E53935` |
| HIGH | Orange | `#FF9800` |
| MEDIUM | Yellow | `#FDD835` |
| LOW | Green | `#43A047` |
| Normal | Blue | `#1E88E5` |

---

## Performance Tips

- Use **DirectQuery** + Snowflake Dynamic Tables for true real-time data
- Enable **Query folding** in Power Query to push filters to Snowflake
- Create Snowflake **Clustering Keys** on `processed_at` for faster queries
- Use **Aggregation Tables** in Power BI for large datasets
- Set Snowflake warehouse to **auto-suspend after 1 minute** to control costs

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Slow refresh | Check Snowflake warehouse size; upsize if needed |
| No data in DirectQuery | Verify Dynamic Tables have refreshed |
| Alert not firing | Check Power BI service is connected to dataset |
| Authentication error | Re-enter Snowflake credentials in dataset settings |
