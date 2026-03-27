from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# Define the base volume path (your raw data zone)
BASE_PATH = "/Volumes/primeins/bronze/raw_data/autoinsurancedata"

# =============================================================================
# ENTITY 1: CUSTOMERS
# =============================================================================
@dp.table(
    name="primeins.bronze.customers",
    comment="Bronze layer streaming table for raw customers data. Incremental ingestion via Auto Loader."
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "[cC][uU][sS][tT][oO][mM][eE][rR][sS]*.csv")
        .load(BASE_PATH)
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# =============================================================================
# ENTITY 2: SALES
# =============================================================================
@dp.table(
    name="primeins.bronze.sales",
    comment="Bronze layer streaming table for raw sales data."
)
def bronze_sales():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "[sS][aA][lL][eE][sS]*.csv")
        .load(BASE_PATH)
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# =============================================================================
# ENTITY 3: CLAIMS (JSON Format)
# =============================================================================
@dp.table(
    name="primeins.bronze.claims",
    comment="Bronze layer streaming table for raw claims data in JSON format."
)
def bronze_claims():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "[cC][lL][aA][iI][mM][sS]*.json")
        .load(BASE_PATH)
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# =============================================================================
# ENTITY 4: CARS
# =============================================================================
@dp.table(
    name="primeins.bronze.cars",
    comment="Bronze layer streaming table for raw cars data."
)
def bronze_cars():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "[cC][aA][rR][sS]*.csv")
        .load(BASE_PATH)
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# =============================================================================
# ENTITY 5: POLICY
# =============================================================================
@dp.table(
    name="primeins.bronze.policy",
    comment="Bronze layer streaming table for raw policy data."
)
def bronze_policy():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "[pP][oO][lL][iI][cC][yY]*.csv")
        .load(BASE_PATH)
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn("_load_timestamp", current_timestamp())
    )