from pyspark import pipelines as dp
import pyspark.sql.functions as F

# =============================================================================
# PART 1: GOLD DIMENSIONS
# =============================================================================

@dp.table(name="primeins.gold.dim_customer", comment="Gold Customer Dimension")
def dim_customer():
    df = spark.read.table("primeins.silver.silver_customers")
    return (
        df.withColumn("customer_sk", F.md5(F.concat_ws("|", F.col("customer_id"), F.col("__START_AT").cast("string"))))
          .withColumn("is_current", F.col("__END_AT").isNull()) 
          .withColumnRenamed("__START_AT", "valid_from")
          .withColumnRenamed("__END_AT", "valid_to")
    )

@dp.table(name="primeins.gold.dim_policy", comment="Gold Policy Dimension")
def dim_policy():
    df = spark.read.table("primeins.silver.silver_policy")
    return (
        df.withColumn("policy_sk", F.md5(F.concat_ws("|", F.col("policy_number"), F.col("__START_AT").cast("string"))))
          .withColumn("is_current", F.col("__END_AT").isNull()) 
          .withColumnRenamed("__START_AT", "valid_from")
          .withColumnRenamed("__END_AT", "valid_to")
    )

@dp.table(name="primeins.gold.dim_car", comment="Gold Car Dimension")
def dim_car():
    df = spark.read.table("primeins.silver.silver_cars")
    return (
        df.withColumn("car_sk", F.md5(F.concat_ws("|", F.col("car_id"), F.col("__START_AT").cast("string"))))
          .withColumn("is_current", F.col("__END_AT").isNull()) 
          .withColumnRenamed("__START_AT", "valid_from")
          .withColumnRenamed("__END_AT", "valid_to")
    )

# =============================================================================
# PART 2: GOLD FACT TABLES
# =============================================================================

@dp.table(name="primeins.gold.fact_claims", comment="Claims Fact Table linked to historical dimensions")
def fact_claims():
    claims = spark.readStream.table("primeins.silver.silver_claims")
    
    # 🌟 THE FIX: No LIVE prefix. Just use the exact 3-part name!
    dim_pol = spark.read.table("primeins.gold.dim_policy")   
    dim_cust = spark.read.table("primeins.gold.dim_customer")
    
    pol_join = claims.join(
        dim_pol,
        (claims.policyid == dim_pol.policy_number),
        # (claims.incident_date >= dim_pol.valid_from) &
        # (claims.incident_date <= F.coalesce(dim_pol.valid_to, F.to_timestamp(F.lit("9999-12-31")))),
        "left"
    )
    
    cust_join = pol_join.join(
        dim_cust,
        (dim_pol.customer_id == dim_cust.customer_id),
        # (pol_join.incident_date >= dim_cust.valid_from) &
        # (pol_join.incident_date <= F.coalesce(dim_cust.valid_to, F.to_timestamp(F.lit("9999-12-31")))),
        "left"
    )
    
    return cust_join.select(
        F.col("claimid").alias("claim_id"),
        F.col("policy_sk"),
        F.col("customer_sk"),
        F.col("incident_date"),
        F.col("incident_state_full"),
        F.col("number_of_vehicles_involved"),
        F.col("incident_severity"),
        #F.col("total_claim_amount"),
        F.col("claim_processed_on"),
        F.col("claim_logged_on"),
        F.datediff(F.col("claim_processed_on"), F.col("claim_logged_on")).alias("processing_time_days"),
        F.when(F.col("claim_rejected") == True, 1).otherwise(0).alias("is_rejected_int")
    )

@dp.table(name="primeins.gold.fact_car_sales", comment="Car Sales Fact Table")
def fact_car_sales():
    sales = spark.readStream.table("primeins.silver.silver_sales")
    dim_car = spark.read.table("primeins.gold.dim_car")
    
    joined = sales.join(
        dim_car,
        (sales.car_id == dim_car.car_id) &
        (sales.ad_placed_on >= dim_car.valid_from) &
        (sales.ad_placed_on <= F.coalesce(dim_car.valid_to, F.to_timestamp(F.lit("9999-12-31")))),
        "left"
    )
    
    return joined.select(
        F.col("sales_id"),
        F.col("car_sk"),
        F.col("region"),
        F.col("state"),
        F.col("city"),
        F.col("ad_placed_on"),
        F.col("sold_on"),
        F.col("original_selling_price"), 
        F.datediff(F.coalesce(F.col("sold_on"), F.current_timestamp()), F.col("ad_placed_on")).alias("days_on_market"),
        F.when(F.col("sold_on").isNull(), 1).otherwise(0).alias("is_unsold")
    )

# =============================================================================
# PART 3: BUSINESS DATA MARTS (Presentation Layer)
# =============================================================================

@dp.table(name="primeins.gold.mart_claim_performance", comment="Claim metrics for compliance & operations")
def mart_claim_performance():
    facts = spark.read.table("primeins.gold.fact_claims")
    dims_pol = spark.read.table("primeins.gold.dim_policy")
    dims_cust = spark.read.table("primeins.gold.dim_customer")
    
    mart = facts.join(dims_pol, "policy_sk", "inner") \
                .join(dims_cust, "customer_sk", "inner")
    
    return (
        mart.groupBy(
                dims_cust.region, 
                dims_pol.policy_csl,                                   # Added Policy Type
                facts.incident_severity,                               #  Added Incident Severity
                F.year(facts.incident_date).alias("incident_year"),    #  Added Time Dimension (Year)
                F.month(facts.incident_date).alias("incident_month")   #  Added Time Dimension (Month)
            )
            .agg(
                F.count("claim_id").alias("total_claims_filed"),
                F.round(F.avg("processing_time_days"), 1).alias("avg_processing_time_days"),
                F.round((F.sum("is_rejected_int") / F.count("claim_id")) * 100, 2).alias("rejection_rate_pct")
            )
    )

@dp.table(name="primeins.gold.mart_unsold_inventory", comment="Tracks revenue leakage and stagnant inventory > 60 days")
def mart_unsold_inventory():
    facts = spark.read.table("primeins.gold.fact_car_sales")
    dims_car = spark.read.table("primeins.gold.dim_car")
    
    #  Explicitly filter for vehicles sitting for MORE than 60 days
    stagnant_facts = facts.filter((F.col("is_unsold") == 1) & (F.col("days_on_market") > 60))
    mart = stagnant_facts.join(dims_car, "car_sk", "inner")
    
    return (
        mart.groupBy("model")
            .agg(
                F.count("sales_id").alias("stagnant_vehicle_count"),
                F.round(F.avg("original_selling_price"), 2).alias("avg_original_selling_price"), 
                F.round(F.avg("days_on_market"), 1).alias("avg_days_on_market"),
                F.max("days_on_market").alias("max_days_on_market")
            )
            .orderBy(F.desc("avg_days_on_market"))
    )

@dp.table(name="primeins.gold.mart_customer_metrics", comment="Unified customer demographics")
def mart_customer_metrics():
    dims_cust = spark.read.table("primeins.gold.dim_customer").filter(F.col("is_current") == True)
    
    return (
        dims_cust.groupBy("region")
                 .agg(
                     F.count("customer_sk").alias("total_active_customers"),
                     F.sum(F.when(F.col("education") == "tertiary", 1).otherwise(0)).alias("tertiary_edu_count")
                 )
                 .orderBy(F.desc("total_active_customers"))
    )