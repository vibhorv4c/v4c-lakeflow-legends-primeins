# Databricks notebook source
# MAGIC %md
# MAGIC # 🚗 Unsold Cars Alert System
# MAGIC **Catalog:** `primeins.gold`  
# MAGIC **Purpose:** Detect aging unsold inventory, score redistribution opportunities, and push a daily Slack alert.  
# MAGIC
# MAGIC ---
# MAGIC ### View architecture
# MAGIC | View | Source | What it does |
# MAGIC |---|---|---|
# MAGIC | `v_aging_inventory` | `fact_car_sales` (is_unsold=1) | Every unsold car with aging tier — **source of truth for car counts** |
# MAGIC | `v_regional_demand_signal` | `fact_car_sales` (is_unsold=0) | How fast each model sells per region — benchmark only |
# MAGIC | `v_alert_scored` | Both views above | Unsold cars joined to demand — **1 car can have multiple rows** (one per destination) |
# MAGIC | `v_alert_summary` | `v_alert_scored` | Aggregated by region and tier for dashboard use |
# MAGIC
# MAGIC > ⚠️ **Never count cars from `v_alert_scored` or `v_alert_summary`** — always use `v_aging_inventory` for counts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — One-time setup: store Slack webhook in Databricks secrets
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load Slack webhook

# COMMAND ----------

SLACK_WEBHOOK = dbutils.secrets.get(scope="primeins-alerts", key="slack_webhook")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create views
# MAGIC ### 2a. `v_aging_inventory` — unsold cars with aging tiers
# MAGIC - Source: `fact_car_sales WHERE is_unsold = 1`
# MAGIC - One row per unsold car — **no duplicates**
# MAGIC - Tiers: CRITICAL (90+ days), HIGH (60–90), WATCH (30–60)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW primeins.gold.v_aging_inventory AS
# MAGIC SELECT
# MAGIC     fs.sales_id,
# MAGIC     fs.region,
# MAGIC     fs.state,
# MAGIC     fs.city,
# MAGIC     dc.model,
# MAGIC     dc.name             AS car_name,
# MAGIC     dc.fuel,
# MAGIC     dc.transmission,
# MAGIC     fs.days_on_market,
# MAGIC     fs.original_selling_price,
# MAGIC     fs.ad_placed_on,
# MAGIC     CASE
# MAGIC         WHEN fs.days_on_market >= 90 THEN 'CRITICAL'
# MAGIC         WHEN fs.days_on_market >= 60 THEN 'HIGH'
# MAGIC         WHEN fs.days_on_market >= 30 THEN 'WATCH'
# MAGIC         ELSE 'OK'
# MAGIC     END                 AS aging_tier,
# MAGIC     ROUND(fs.original_selling_price * 0.15, 0) AS at_risk_revenue
# MAGIC FROM primeins.gold.fact_car_sales  fs
# MAGIC JOIN primeins.gold.dim_car         dc ON fs.car_sk = dc.car_sk AND dc.is_current = TRUE
# MAGIC WHERE fs.is_unsold = 1
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. `v_regional_demand_signal` — how fast models sell per region
# MAGIC - Source: `fact_car_sales WHERE is_unsold = 0` (sold cars only)
# MAGIC - Anchored to the latest sale date in the data (not today) to handle historical datasets
# MAGIC - Used as a benchmark to identify redistribution destinations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW primeins.gold.v_regional_demand_signal AS
# MAGIC WITH latest_date AS (
# MAGIC     -- Anchor to data's latest date, not CURRENT_DATE() — handles historical datasets
# MAGIC     SELECT MAX(sold_on) AS max_sold_on
# MAGIC     FROM primeins.gold.fact_car_sales
# MAGIC     WHERE is_unsold = 0
# MAGIC )
# MAGIC SELECT
# MAGIC     dc.model,
# MAGIC     dc.fuel,
# MAGIC     dc.transmission,
# MAGIC     fs.region,
# MAGIC     COUNT(*)                                     AS total_sold,
# MAGIC     ROUND(AVG(fs.days_on_market), 1)             AS avg_days_to_sell,
# MAGIC     ROUND(PERCENTILE(fs.days_on_market, 0.5), 1) AS median_days_to_sell
# MAGIC FROM primeins.gold.fact_car_sales  fs
# MAGIC JOIN primeins.gold.dim_car         dc ON fs.car_sk = dc.car_sk AND dc.is_current = TRUE
# MAGIC CROSS JOIN latest_date
# MAGIC WHERE
# MAGIC     fs.is_unsold = 0
# MAGIC     AND fs.sold_on >= DATE_SUB(latest_date.max_sold_on, 90)
# MAGIC GROUP BY dc.model, dc.fuel, dc.transmission, fs.region
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. `v_alert_scored` — unsold cars scored with redistribution opportunities
# MAGIC - Joins unsold cars (from `v_aging_inventory`) with demand signal (from `v_regional_demand_signal`)
# MAGIC - **⚠️ One unsold car can produce multiple rows** — one row per matching demand region
# MAGIC - Example: a Petrol Auto Maruti stuck in East may match demand in West, North, and South → 3 rows for 1 car
# MAGIC - Always use `COUNT(DISTINCT sales_id)` when counting cars from this view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW primeins.gold.v_alert_scored AS
# MAGIC WITH aging AS (
# MAGIC     SELECT * FROM primeins.gold.v_aging_inventory
# MAGIC     WHERE aging_tier IN ('CRITICAL', 'HIGH', 'WATCH')
# MAGIC ),
# MAGIC demand AS (
# MAGIC     SELECT * FROM primeins.gold.v_regional_demand_signal
# MAGIC     WHERE median_days_to_sell <= 21
# MAGIC ),
# MAGIC scored AS (
# MAGIC     SELECT
# MAGIC         a.sales_id,
# MAGIC         a.region                                AS stuck_region,
# MAGIC         a.state,
# MAGIC         a.city,
# MAGIC         a.car_name,
# MAGIC         a.model,
# MAGIC         a.fuel,
# MAGIC         a.days_on_market,
# MAGIC         a.aging_tier,
# MAGIC         a.original_selling_price,
# MAGIC         a.at_risk_revenue,
# MAGIC         a.ad_placed_on,
# MAGIC         d.region                                AS demand_region,
# MAGIC         d.median_days_to_sell                   AS demand_median_days,
# MAGIC         ROUND(
# MAGIC             (a.days_on_market / 90.0) * 50
# MAGIC             + (a.original_selling_price / 1000000.0) * 30
# MAGIC             + CASE WHEN d.region IS NOT NULL THEN 20 ELSE 0 END
# MAGIC         , 1)                                    AS risk_score
# MAGIC     FROM aging a
# MAGIC     LEFT JOIN demand d
# MAGIC         ON  a.model        = d.model
# MAGIC         AND a.fuel         = d.fuel
# MAGIC         AND a.transmission = d.transmission
# MAGIC         AND a.region      != d.region
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC     RANK() OVER (ORDER BY risk_score DESC) AS priority_rank,
# MAGIC     CASE
# MAGIC         WHEN demand_region IS NOT NULL
# MAGIC         THEN CONCAT('Consider moving to ', demand_region,
# MAGIC                     ' — sold in ', demand_median_days, ' days there')
# MAGIC         ELSE 'No fast-moving region identified — review pricing'
# MAGIC     END                                         AS recommended_action
# MAGIC FROM scored
# MAGIC ORDER BY risk_score DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. `v_alert_summary` — aggregated by region and tier
# MAGIC - Used for dashboard and high-level revenue numbers
# MAGIC - ⚠️ `cars_at_risk` here reflects scored rows, not unique cars — use `v_aging_inventory` for true counts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW primeins.gold.v_alert_summary AS
# MAGIC SELECT
# MAGIC     aging_tier,
# MAGIC     stuck_region,
# MAGIC     COUNT(DISTINCT sales_id)                            AS cars_at_risk,
# MAGIC     ROUND(SUM(DISTINCT at_risk_revenue), 0)             AS total_at_risk_revenue,
# MAGIC     COUNT(DISTINCT CASE WHEN demand_region IS NOT NULL
# MAGIC                         THEN sales_id END)              AS redistributable_cars,
# MAGIC     ROUND(SUM(DISTINCT CASE WHEN demand_region IS NOT NULL
# MAGIC                             THEN at_risk_revenue
# MAGIC                        END), 0)                         AS recoverable_revenue
# MAGIC FROM primeins.gold.v_alert_scored
# MAGIC GROUP BY aging_tier, stuck_region
# MAGIC ORDER BY
# MAGIC     CASE aging_tier
# MAGIC         WHEN 'CRITICAL' THEN 1
# MAGIC         WHEN 'HIGH'     THEN 2
# MAGIC         WHEN 'WATCH'    THEN 3
# MAGIC     END,
# MAGIC     total_at_risk_revenue DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Validate all views
# MAGIC Run this after creating views to confirm row counts are sensible.

# COMMAND ----------

counts = {
    "v_aging_inventory":        spark.sql("SELECT COUNT(*) FROM primeins.gold.v_aging_inventory").collect()[0][0],
    "v_regional_demand_signal": spark.sql("SELECT COUNT(*) FROM primeins.gold.v_regional_demand_signal").collect()[0][0],
    "v_alert_scored":           spark.sql("SELECT COUNT(*) FROM primeins.gold.v_alert_scored").collect()[0][0],
    "v_alert_summary":          spark.sql("SELECT COUNT(*) FROM primeins.gold.v_alert_summary").collect()[0][0],
}

print("=" * 50)
print("View row counts")
print("=" * 50)
for view, count in counts.items():
    print(f"  {view:<35} {count:>6} rows")
    if count == 0:
        raise Exception(f"ERROR: {view} returned 0 rows — investigate before sending alerts")

print()
print("NOTE: v_alert_scored will have MORE rows than v_aging_inventory")
print("      because one unsold car can match multiple demand regions.")
print("      This is expected behaviour — not a bug.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Daily Slack alert
# MAGIC
# MAGIC ### Data sources used in this cell
# MAGIC | What | Source | Why |
# MAGIC |---|---|---|
# MAGIC | Total unsold cars, counts by tier, revenue at risk | `v_aging_inventory` | No duplicates — 1 row = 1 car |
# MAGIC | Cars that can be redistributed + where to send them | `v_alert_scored` with `COUNT(DISTINCT sales_id)` | Deduplicates the multi-row join |

# COMMAND ----------

import requests
from datetime import date

# ── 1. Car counts and revenue — from v_aging_inventory (no duplicates)
unsold_df = spark.sql("""
    SELECT
        COUNT(*)                                                AS total_unsold,
        COUNT(CASE WHEN aging_tier = 'CRITICAL' THEN 1 END)    AS critical_cars,
        COUNT(CASE WHEN aging_tier = 'HIGH'     THEN 1 END)    AS high_cars,
        COUNT(CASE WHEN aging_tier = 'WATCH'    THEN 1 END)    AS watch_cars,
        ROUND(SUM(at_risk_revenue), 0)                         AS total_at_risk_revenue
    FROM primeins.gold.v_aging_inventory
""").toPandas()

total_unsold   = int(unsold_df['total_unsold'].iloc[0])
total_critical = int(unsold_df['critical_cars'].iloc[0])
total_high     = int(unsold_df['high_cars'].iloc[0])
total_watch    = int(unsold_df['watch_cars'].iloc[0])
total_risk     = int(unsold_df['total_at_risk_revenue'].iloc[0])

# ── 2. Redistribution opportunities — from v_alert_scored with DISTINCT to avoid double counting
reco_df = spark.sql("""
    SELECT
        stuck_region,
        aging_tier,
        COUNT(DISTINCT sales_id)                                AS unsold_cars,
        ROUND(SUM(DISTINCT at_risk_revenue), 0)                 AS at_risk_revenue,
        COUNT(DISTINCT CASE WHEN demand_region IS NOT NULL
                            THEN sales_id END)                  AS redistributable_cars,
        ROUND(SUM(DISTINCT CASE WHEN demand_region IS NOT NULL
                                THEN at_risk_revenue END), 0)   AS recoverable_revenue,
        MAX(CASE WHEN demand_region IS NOT NULL
                 THEN CONCAT('Move to ', demand_region,
                             ' (sells in ~', CAST(demand_median_days AS INT), ' days)')
            END)                                                AS best_action
    FROM primeins.gold.v_alert_scored
    WHERE aging_tier IN ('CRITICAL', 'HIGH')
    GROUP BY stuck_region, aging_tier
    ORDER BY aging_tier, at_risk_revenue DESC
""").toPandas()

total_recoverable = int(reco_df['recoverable_revenue'].sum())
recovery_pct      = round((total_recoverable / total_risk * 100) if total_risk > 0 else 0, 1)

# ── 3. Build per-region lines
region_lines = []
for _, row in reco_df.head(6).iterrows():
    action = row['best_action'] if row['best_action'] else 'Review pricing — no fast-moving region found'
    region_lines.append(
        f"• *{row.stuck_region}* `{row.aging_tier}` — "
        f"{int(row.unsold_cars)} unsold · "
        f"₹{int(row.at_risk_revenue):,} at risk · "
        f"{int(row.redistributable_cars)} can be moved\n"
        f"  _↳ {action}_"
    )

region_block = "\n".join(region_lines)

# ── 4. Build and send Slack payload
payload = {
    "blocks": [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚗 Inventory Alert — {date.today().strftime('%d %b %Y')}",
                "emoji": True
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Total Unsold Cars*\n{total_unsold} cars"},
                {"type": "mrkdwn", "text": f"*Total Revenue at Risk*\n\u20b9{total_risk:,}"}
            ]
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Critical (90+ days)*\n{total_critical} cars"},
                {"type": "mrkdwn", "text": f"*High Risk (60–90 days)*\n{total_high} cars"},
                {"type": "mrkdwn", "text": f"*Watch (30–60 days)*\n{total_watch} cars"},
                {"type": "mrkdwn", "text": f"*Recoverable via Redistribution*\n\u20b9{total_recoverable:,} ({recovery_pct}%)"}
            ]
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*\u26a0\ufe0f Top Regions Needing Action*\n\n{region_block}"
            }
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "_PrimeIns · primeins.gold.v_aging_inventory · Refreshed daily at 08:00_"
                }
            ]
        }
    ]
}

response = requests.post(SLACK_WEBHOOK, json=payload)
print(f"Slack response: {response.status_code}")
print()
print("── Summary sent ──")
print(f"  Total unsold cars  : {total_unsold}")
print(f"  Critical (90+ days): {total_critical}")
print(f"  High (60–90 days)  : {total_high}")
print(f"  Watch (30–60 days) : {total_watch}")
print(f"  Total at risk      : \u20b9{total_risk:,}")
print(f"  Recoverable        : \u20b9{total_recoverable:,} ({recovery_pct}%)")
assert total_critical + total_high + total_watch <= total_unsold, \
    f"ERROR: tier counts ({total_critical + total_high + total_watch}) exceed total unsold ({total_unsold})"
