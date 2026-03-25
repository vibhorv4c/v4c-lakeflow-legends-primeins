from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from datetime import datetime

# =============================================================================
# SHARED CONSTANTS & FUNCTIONS
# =============================================================================
VALID_REGIONS = ["East", "West", "Central", "South", "North"]
MARITAL_VALS  = ["single", "married", "divorced"]
DATE_CORRUPT  = r"^\d{1,3}:\d{2}\.\d+$"   # Excel serial fragment: "27:00.0"
KM_MAX        = 500_000
KGM_TO_NM     = 9.80665

STATE_MAP = {
    "IL":"Illinois",     "NC":"North Carolina", "OH":"Ohio",
    "PA":"Pennsylvania", "SC":"South Carolina", "VA":"Virginia",
    "WV":"West Virginia","IN":"Indiana",        "NY":"New York",
    "CA":"California",   "TX":"Texas",          "FL":"Florida",
    "GA":"Georgia",      "MI":"Michigan",       "NJ":"New Jersey",
    "AZ":"Arizona",      "WA":"Washington",     "MA":"Massachusetts",
    "MN":"Minnesota",    "MO":"Missouri",       "TN":"Tennessee",
    "WI":"Wisconsin",    "CO":"Colorado",
}
VALID_STATES = list(STATE_MAP.values())

def add_audit(df, stage):
    """Helper to stamp the silver processing time and stage."""
    return (df
        .withColumn("_silver_load_ts", F.current_timestamp())
        .withColumn("_stage",          F.lit(stage)))

def unify_cased_columns(df):
    """
    Detects columns that have the same name but different casing (e.g., 'ClaimID' and 'claimid').
    Coalesces them into a single case-insensitive (lowercase) column to prevent data loss,
    and standardizes all remaining columns to lowercase.
    """
    col_map = {}
    for col in df.columns:
        lower_col = col.lower()
        if lower_col not in col_map:
            col_map[lower_col] = []
        col_map[lower_col].append(col)

    for lower_col, orig_cols in col_map.items():
        if len(orig_cols) > 1:
            # Coalesce the multiple cased columns into the lowercase version
            df = df.withColumn(lower_col, F.coalesce(*[F.col(c) for c in orig_cols]))
            # Drop the original columns (unless one of them exactly matches the lowercase name)
            for c in orig_cols:
                if c != lower_col:
                    df = df.drop(c)
        else:
            # Rename to lowercase if it isn't already
            if orig_cols[0] != lower_col:
                df = df.withColumnRenamed(orig_cols[0], lower_col)
                
    return df


# # =============================================================================
# # ENTITY 1 — CUSTOMERS
# # =============================================================================
# @dp.view(name="customers_prepared")
# def customers_prepared():
#     df = spark.readStream.table("primeins.bronze.customers")
    
#     # Apply standardisation immediately
#     df = unify_cased_columns(df)

#     # S1 & S2 — Unify semantic duplicates and explicitly drop old columns
#     semantic_mappings = {
#         "customer_id": ["customerid", "customer_id", "cust_id"],
#         "region_raw":  ["region", "reg"],
#         "city":        ["city_in_state", "city"],
#         "education":   ["education", "edu"],
#         "marital":     ["marital_status", "marital"]
#     }

#     for target_col, source_cols in semantic_mappings.items():
#         # Find which of these variants actually exist in the dataframe right now
#         existing_sources = [c for c in source_cols if c in df.columns]
        
#         if existing_sources:
#             # Coalesce whichever variants exist into a temporary column
#             df = df.withColumn(f"_{target_col}_temp", F.coalesce(*[F.col(c) for c in existing_sources]))
#             # Explicitly drop all the old messy variants!
#             df = df.drop(*existing_sources)
#             # Rename the temp column to the clean target name
#             df = df.withColumnRenamed(f"_{target_col}_temp", target_col)
#         else:
#             # If none exist, create it as a null column to enforce the schema
#             df = df.withColumn(target_col, F.lit(None).cast(T.StringType()))
            
#     for c in ["region_raw","education","marital","city"]:
#         if c not in df.columns:
#             df = df.withColumn(c, F.lit(None).cast(T.StringType()))

#     # S4 — Expand region abbreviations
#     df = (df.withColumn("region",
#         F.when(F.upper(F.col("region_raw")) == "E", "East")
#          .when(F.upper(F.col("region_raw")) == "W", "West")
#          .when(F.upper(F.col("region_raw")) == "C", "Central")
#          .when(F.upper(F.col("region_raw")) == "S", "South")
#          .when(F.upper(F.col("region_raw")) == "N", "North")
#          .otherwise(F.initcap(F.col("region_raw")))).drop("region_raw")
#     )

#     # S7 — Cast Balance
#     if "balance" in df.columns:
#         df = df.withColumn("balance", F.col("balance").cast(T.DoubleType()))
        
#     df = add_audit(df, "silver_customers")
    
#     # Apply DQ Flags
#     return (df
#         .withColumn("is_valid_r1", F.col("customer_id").isNotNull())
#         .withColumn("is_valid_r2", F.col("region").isin(VALID_REGIONS))
#     )

# @dp.table(name="primeins.silver.silver_customers_quarantine", table_properties={"quality":"silver_quarantine"})
# def silver_customers_quarantine():
#     return (
#         spark.readStream.table("customers_prepared")
#         .filter(~F.col("is_valid_r1") | ~F.col("is_valid_r2"))
#         .withColumn("_reject_reason", 
#             F.when(~F.col("is_valid_r1"), F.lit("R1: customer_id is null"))
#              .otherwise(F.lit("R2: invalid region code")))
#     )

# @dp.view(name="customers_clean_stream")
# @dp.expect("R3_valid_education", "education IS NULL OR education IN ('primary', 'secondary', 'tertiary')")
# @dp.expect("R4_positive_balance", "balance IS NULL OR balance >= 0")
# @dp.expect("R5_valid_marital", "marital IS NULL OR marital IN ('single', 'married', 'divorced')")
# def customers_clean_stream():
#     return (
#         spark.readStream.table("customers_prepared")
#         .filter(F.col("is_valid_r1") & F.col("is_valid_r2"))
#         .drop("is_valid_r1", "is_valid_r2")
#     )

# dp.create_streaming_table(name="primeins.silver.silver_customers", comment="SCD Type 2 Customer Table")
# dp.apply_changes(
#     target="primeins.silver.silver_customers",
#     source="customers_clean_stream",
#     keys=["customer_id"],
#     sequence_by="_silver_load_ts",
#     stored_as_scd_type=2 
# )


# # =============================================================================
# # ENTITY 2 — CLAIMS
# # =============================================================================
# @dp.view(name="claims_prepared")
# def claims_prepared():
#     df = spark.readStream.table("primeins.bronze.claims")
    
#     # Apply standardisation immediately
#     df = unify_cased_columns(df)

#     if "claim_processed_on" in df.columns:
#         df = df.withColumn("claim_processed_on", F.when(F.upper(F.col("claim_processed_on")) == "NULL", F.lit(None)).otherwise(F.col("claim_processed_on")))
#     for bc in ["property_damage","police_report_available"]:
#         if bc in df.columns:
#             df = df.withColumn(bc, F.when(F.col(bc) == "?", F.lit(None)).otherwise(F.col(bc)))

#     date_cols = [c for c in ["incident_date","claim_logged_on","claim_processed_on"] if c in df.columns]
#     corrupt = F.lit(False)
#     for dc in date_cols:
#         corrupt = corrupt | (F.col(dc).isNotNull() & F.col(dc).rlike(DATE_CORRUPT))
    
#     df = add_audit(df, "silver_claims")

#     has_cid = "claimid" in df.columns
#     has_pid = "policyid" in df.columns

#     return (df
#         .withColumn("is_valid_r1", F.col("claimid").isNotNull() if has_cid else F.lit(False))
#         .withColumn("is_valid_r2", F.col("policyid").isNotNull() if has_pid else F.lit(False))
#         .withColumn("is_valid_r3", ~corrupt)
#     )

# @dp.table(name="primeins.silver.silver_claims_quarantine", table_properties={"quality":"silver_quarantine"})
# def silver_claims_quarantine():
#     return (
#         spark.readStream.table("claims_prepared")
#         .filter(~F.col("is_valid_r1") | ~F.col("is_valid_r2") | ~F.col("is_valid_r3"))
#         .withColumn("_reject_reason", 
#             F.when(~F.col("is_valid_r1"), F.lit("R1: ClaimID is null"))
#              .when(~F.col("is_valid_r2"), F.lit("R2: PolicyID is null"))
#              .otherwise(F.lit("R3: Excel date serial corruption")))
#     )

# @dp.view(name="claims_clean_stream")
# @dp.expect("R5_valid_incident_severity", "incident_severity IS NULL OR incident_severity IN ('Major Damage', 'Minor Damage', 'Trivial Damage')")
# def claims_clean_stream():
#     return (
#         spark.readStream.table("claims_prepared")
#         .filter(F.col("is_valid_r1") & F.col("is_valid_r2") & F.col("is_valid_r3"))
#         .drop("is_valid_r1", "is_valid_r2", "is_valid_r3")
#     )

# dp.create_streaming_table(name="primeins.silver.silver_claims", comment="SCD Type 2 Claims Table")
# dp.apply_changes(
#     target="primeins.silver.silver_claims",
#     source="claims_clean_stream",
#     keys=["claimid"], # Now lowercase due to standardisation
#     sequence_by="_silver_load_ts",
#     stored_as_scd_type=2 
# )

# # =============================================================================
# # ENTITY 3 — SALES
# # =============================================================================
# @dp.view(name="sales_prepared")
# def sales_prepared():
#     df = spark.readStream.table("primeins.bronze.sales")
    
#     # Apply standardisation immediately
#     df = unify_cased_columns(df)

#     # S1 — Drop entirely blank rows (zero information)
#     data_cols = [c for c in df.columns if not c.startswith("_")]
#     not_blank = F.greatest(*[
#         F.when(F.col(c).isNotNull() & (F.trim(F.col(c).cast(T.StringType())) != ""), F.lit(1))
#          .otherwise(F.lit(0)) for c in data_cols
#     ]) > 0
#     df = df.filter(not_blank)

#     # S2 — Parse dates to TIMESTAMP
#     for dc in ["ad_placed_on", "sold_on"]:
#         if dc in df.columns:
#             df = df.withColumn(dc, F.to_timestamp(F.col(dc), "dd-MM-yyyy HH:mm"))

#     # S3 — Flag unsold inventory
#     df = df.withColumn("is_unsold", F.col("sold_on").isNull() if "sold_on" in df.columns else F.lit(True))

#     # S4 — Cast numeric fields
#     for nc in ["original_selling_price", "km_driven"]:
#         if nc in df.columns:
#             df = df.withColumn(nc, F.col(nc).cast(T.DoubleType()))

#     df = add_audit(df, "silver_sales")
    
#     # Evaluate Quality Rules
#     has_sid   = "sales_id" in df.columns
#     has_car   = "car_id" in df.columns
#     has_price = "original_selling_price" in df.columns
#     has_dates = all(c in df.columns for c in ["sold_on", "ad_placed_on"])

#     return (df
#         .withColumn("is_valid_pk", F.col("sales_id").isNotNull() if has_sid else F.lit(False))
#         .withColumn("is_valid_r2", F.col("car_id").isNotNull() if has_car else F.lit(False))
#         .withColumn("is_valid_r3", (F.col("original_selling_price").isNotNull() & (F.col("original_selling_price") > 0)) if has_price else F.lit(False))
#         .withColumn("is_valid_r4", (F.col("sold_on").isNull() | F.col("ad_placed_on").isNull() | (F.col("sold_on") >= F.col("ad_placed_on"))) if has_dates else F.lit(False))
#     )

# @dp.table(name="primeins.silver.silver_sales_quarantine", table_properties={"quality":"silver_quarantine"})
# def silver_sales_quarantine():
#     return (
#         spark.readStream.table("sales_prepared")
#         .filter(~F.col("is_valid_pk") | ~F.col("is_valid_r2") | ~F.col("is_valid_r3") | ~F.col("is_valid_r4"))
#         .withColumn("_reject_reason",
#             F.when(~F.col("is_valid_pk"), F.lit("PK: sales_id is null"))
#              .when(~F.col("is_valid_r2"), F.lit("R2: car_id is null"))
#              .when(~F.col("is_valid_r3"), F.lit("R3: original_selling_price is null or <= 0"))
#              .otherwise(F.lit("R4: sold_on is before ad_placed_on")))
#     )

# @dp.view(name="sales_clean_stream")
# @dp.expect("R5_valid_seller", "seller_type IS NULL OR lower(seller_type) IN ('individual', 'dealer')")
# def sales_clean_stream():
#     return (
#         spark.readStream.table("sales_prepared")
#         .filter(F.col("is_valid_pk") & F.col("is_valid_r2") & F.col("is_valid_r3") & F.col("is_valid_r4"))
#         .drop("is_valid_pk", "is_valid_r2", "is_valid_r3", "is_valid_r4")
#     )

# dp.create_streaming_table(name="primeins.silver.silver_sales", comment="SCD Type 2 Sales Table")
# dp.apply_changes(
#     target="primeins.silver.silver_sales",
#     source="sales_clean_stream",
#     keys=["sales_id"],
#     sequence_by="_silver_load_ts",
#     stored_as_scd_type=2
# )

# # =============================================================================
# # ENTITY 4 — CARS
# # =============================================================================
# @dp.view(name="cars_prepared")
# def cars_prepared():
#     df = spark.readStream.table("primeins.bronze.cars")
    
#     # Apply standardisation immediately
#     df = unify_cased_columns(df)

#     if "mileage" in df.columns:
#         df = (df.withColumn("mileage_unit", F.regexp_extract(F.col("mileage"), r"([a-zA-Z/]+)", 1))
#                 .withColumn("mileage", F.regexp_extract(F.col("mileage"), r"([\d.]+)", 1).cast(T.DoubleType())))
#     if "engine" in df.columns:
#         df = df.withColumn("engine_cc", F.regexp_extract(F.col("engine"), r"(\d+)", 1).cast(T.IntegerType())).drop("engine")
#     if "max_power" in df.columns:
#         df = df.withColumn("max_power_bhp", F.regexp_extract(F.col("max_power"), r"([\d.]+)", 1).cast(T.DoubleType())).drop("max_power")
#     if "torque" in df.columns:
#         df = (df.withColumn("torque_raw", F.col("torque"))
#                 .withColumn("_tu", F.when(F.lower(F.col("torque")).contains("kgm"), "kgm").otherwise("Nm"))
#                 .withColumn("_tv", F.regexp_extract(F.col("torque"), r"([\d]+\.?[\d]*)", 1).cast(T.DoubleType()))
#                 .withColumn("torque_nm", F.when(F.col("_tu") == "kgm", F.round(F.col("_tv") * KGM_TO_NM, 2)).otherwise(F.col("_tv")))
#                 .drop("torque", "_tu", "_tv"))

#     for nc in ["km_driven"]:
#         if nc in df.columns:
#             df = df.withColumn(nc, F.col(nc).cast(T.DoubleType()))

#     df = add_audit(df, "silver_cars")
    
#     has_n = "name" in df.columns
#     has_k = "km_driven" in df.columns
#     has_y = "year_of_manufacture" in df.columns
#     has_p = "selling_price" in df.columns

#     return (df
#         .withColumn("is_valid_r1", F.col("name").isNotNull() if has_n else F.lit(False))
#         .withColumn("is_valid_r2", (F.col("km_driven").isNull() | (F.col("km_driven") <= KM_MAX)) if has_k else F.lit(False))
#     )

# @dp.table(name="primeins.silver.silver_cars_quarantine", table_properties={"quality":"silver_quarantine"})
# def silver_cars_quarantine():
#     return (
#         spark.readStream.table("cars_prepared")
#         .filter(~F.col("is_valid_r1") | ~F.col("is_valid_r2") )
#         .withColumn("_reject_reason",
#             F.when(~F.col("is_valid_r1"), F.lit("R1: car primary key (name) is null"))
#              .otherwise(F.concat(F.lit("R2: km_driven exceeds "), F.lit(KM_MAX)))
#         )     
#     )

# @dp.view(name="cars_clean_stream")
# def cars_clean_stream():
#     return (
#         spark.readStream.table("cars_prepared")
#         .filter(F.col("is_valid_r1") & F.col("is_valid_r2"))
#         .drop("is_valid_r1", "is_valid_r2")
#     )

# dp.create_streaming_table(name="primeins.silver.silver_cars", comment="SCD Type 2 Cars Table")
# dp.apply_changes(
#     target="primeins.silver.silver_cars",
#     source="cars_clean_stream",
#     keys=["name"], 
#     sequence_by="_silver_load_ts",
#     stored_as_scd_type=2
# )

# # =============================================================================
# # ENTITY 5 — POLICY (Optimized for Memory)
# # =============================================================================
# @dp.view(name="policy_prepared")
# def policy_prepared():
#     df = spark.readStream.table("primeins.bronze.policy")
    
#     # Apply standardisation immediately (You can safely uncomment this now!)
#     df = unify_cased_columns(df)

#     # 🌟 OPTIMIZATION: Replaced the nested F.when loop with a highly efficient Map lookup
#     state_col = next((c for c in ["policy_state", "state"] if c in df.columns), None)
#     if state_col:
#         # 1. Convert the Python STATE_MAP into a flat list of Spark literals: [lit("IL"), lit("Illinois"), ...]
#         map_args = []
#         for abbr, name in STATE_MAP.items():
#             map_args.extend([F.lit(abbr), F.lit(name)])
        
#         # 2. Create a native Spark Map column
#         spark_state_map = F.create_map(*map_args)
        
#         # 3. Look up the state code in the map. If it misses, coalesce falls back to the initcap version.
#         df = (df
#             .withColumn("policy_state_full", 
#                 F.coalesce(
#                     spark_state_map.getItem(F.upper(F.col(state_col))), 
#                     F.initcap(F.col(state_col))
#                 )
#             )
#             .withColumnRenamed(state_col, "policy_state_code")
#         )

#     for nc in ["policy_annual_premium", "policy_deductable", "umbrella_limit"]:
#         if nc in df.columns:
#             df = df.withColumn(nc, F.col(nc).cast(T.DoubleType()))

#     if "umbrella_limit" in df.columns:
#         df = df.withColumn("_umbrella_limit_zero_flag", F.col("umbrella_limit") == 0)

#     df = add_audit(df, "silver_policy")
    
#     has_pn = "policy_number" in df.columns
#     has_ci = "customer_id" in df.columns
#     has_st = "policy_state_full" in df.columns

#     return (df
#         .withColumn("is_valid_r1", F.col("policy_number").isNotNull() if has_pn else F.lit(False))
#         .withColumn("is_valid_r2", F.col("customer_id").isNotNull() if has_ci else F.lit(False))
#         .withColumn("is_valid_r3", (F.col("policy_state_full").isNotNull() & F.col("policy_state_full").isin(VALID_STATES)) if has_st else F.lit(False))
#     )

# @dp.table(name="primeins.silver.silver_policy_quarantine", table_properties={"quality":"silver_quarantine"})
# def silver_policy_quarantine():
#     return (
#         spark.readStream.table("policy_prepared")
#         .filter(~F.col("is_valid_r1") | ~F.col("is_valid_r2") | ~F.col("is_valid_r3"))
#         .withColumn("_reject_reason",
#             F.when(~F.col("is_valid_r1"), F.lit("R1: policy_number is null"))
#              .when(~F.col("is_valid_r2"), F.lit("R2: customer_id is null (orphan policy)"))
#              .otherwise(F.concat(F.lit("R3: unknown state = "), F.coalesce(F.col("policy_state_full"), F.lit("null")))))
#     )

# @dp.view(name="policy_clean_stream")
# @dp.expect("R4_umbrella_limit_zero_warn", "_umbrella_limit_zero_flag = False OR _umbrella_limit_zero_flag IS NULL")
# def policy_clean_stream():
#     return (
#         spark.readStream.table("policy_prepared")
#         .filter(F.col("is_valid_r1") & F.col("is_valid_r2") & F.col("is_valid_r3"))
#         .drop("is_valid_r1", "is_valid_r2", "is_valid_r3")
#     )

# dp.create_streaming_table(name="primeins.silver.silver_policy", comment="SCD Type 2 Policy Table")
# dp.apply_changes(
#     target="primeins.silver.silver_policy",
#     source="policy_clean_stream",
#     keys=["policy_number"],
#     sequence_by="_silver_load_ts",
#     stored_as_scd_type=2
# )


# =============================================================================
# DQ ISSUES LOG (Static Warning Table)
# =============================================================================
@dp.table(
    name="primeins.silver.dq_issues_log",
    comment="Static log of structural fixes and pipeline alerts."
)
def silver_dq_issues_log():
    from pyspark.sql import Row
    now = datetime.now().isoformat()
    rows = [
        Row(logged_at=now, table_name="primeins.silver.customers", rule_name="primeins.silver.S5_terto_fix", consequence="fix", detail="73 rows in customers_5 had Education='terto'", suggested_fix="Fix terto to tertiary"),
        Row(logged_at=now, table_name="primeins.silver.sales", rule_name="primeins.silver.missing_file_sales_3", consequence="warn", detail="sales_3.csv does not exist.", suggested_fix="Request re-extraction")
    ]
    return spark.createDataFrame(rows)
