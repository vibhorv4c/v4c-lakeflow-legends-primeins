# Databricks notebook source
# MAGIC %md
# MAGIC ### Unit Testing

# COMMAND ----------

# MAGIC %md
# MAGIC Count check in bronze

# COMMAND ----------

# ---------------------------------------------------------
# STEP 3: Verification — Confirm all 5 Bronze tables exist and record counts
# ---------------------------------------------------------
print("=" * 60)
print("BRONZE LAYER VERIFICATION")
print("=" * 60)

entities = ["customers", "sales", "claims", "cars", "policy"]

for entity in entities:
    count = spark.table(f"primeins.bronze.{entity}").count()
    print(f"primeins.bronze.{entity}: {count:,} records")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC Datatype and count comparisn (Raw data vs Bronze)

# COMMAND ----------

import os

# ⚠️ REPLACE with your actual catalog, schema, and volume names
volume_path = "/Volumes/primeins/landing/raw_data/autoinsurancedata/"

all_files = []

# os.walk automatically goes into every subfolder recursively
for root, dirs, files in os.walk(volume_path):
    for file_name in files:
        # Combine the path and file name, then add to our list
        full_path = os.path.join(root, file_name)
        all_files.append(full_path)

print(f"✅ Found a total of {len(all_files)} files.")

# Print the first 10 files to verify
for f in all_files[:10]:
    print(f)

# COMMAND ----------

import os
from pyspark.sql.functions import col

def run_bronze_unit_test(raw_file_path, bronze_table_name, file_format):
    """
    Validates a raw file against its corresponding data in a Bronze Delta table.
    
    Args:
        raw_file_path (str): The full path to the raw file in the Volume.
        bronze_table_name (str): The name of the target Delta table (e.g., 'primeins.bronze.customers').
        file_format (str): The format of the raw file ('csv', 'json', 'parquet').
    """
    print(f"🧪 STARTING TEST: {os.path.basename(raw_file_path)} -> {bronze_table_name}")
    
    try:
        # 1. READ RAW FILE
        # We read without inferring schema to match your Bronze Auto Loader logic (everything is STRING)
        if file_format.lower() == "csv":
            raw_df = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load(raw_file_path)
        else:
            raw_df = spark.read.format(file_format).load(raw_file_path)
            
        raw_count = raw_df.count()
        raw_columns = {col_name: dtype for col_name, dtype in raw_df.dtypes}
        
        # 2. READ BRONZE TABLE (Filtered by the specific file)
        # We use endswith() because _metadata.file_name sometimes captures just the name, 
        # or the path might have a 'dbfs:/' prefix depending on the Databricks runtime.
        file_name = os.path.basename(raw_file_path)
        bronze_df = spark.table(bronze_table_name).filter(col("_source_file").endswith(file_name))
        
        bronze_count = bronze_df.count()
        
        # Filter out the metadata columns Auto Loader added so we can do a 1:1 schema comparison
        metadata_cols = ['_source_file', '_load_timestamp', '_rescued_data']
        bronze_columns = {
            col_name: dtype for col_name, dtype in bronze_df.dtypes 
            if col_name not in metadata_cols
        }

        # 3. RUN ASSERTIONS
        errors = []
        
        # Assertion A: Record Count Match
        if raw_count != bronze_count:
            errors.append(f"Count mismatch: Raw file has {raw_count} rows, but Bronze table has {bronze_count} rows.")
            
        # Assertion B: Datatype / Schema Match
        for col_name, raw_type in raw_columns.items():
            if col_name not in bronze_columns:
                errors.append(f"Missing column: '{col_name}' is in the raw file but missing from Bronze.")
            else:
                bronze_type = bronze_columns[col_name]
                if raw_type != bronze_type:
                    errors.append(f"Datatype mismatch for '{col_name}': Raw is {raw_type}, Bronze is {bronze_type}.")

        # 4. REPORT RESULTS
        if errors:
            print("❌ TEST FAILED with the following errors:")
            for error in errors:
                print(f"   - {error}")
            return False
        else:
            print(f"✅ TEST PASSED: Counts match ({raw_count} rows) and schemas align perfectly.\n")
            return True

    except Exception as e:
        print(f"⚠️ TEST ERRORED: Could not complete execution. Details: {e}")
        return False

for test_file in all_files:
    # 1. Extract just the file name from the path and make it lowercase
    file_name = os.path.basename(test_file).lower()
    
    # 2. Safely check if 'customers' is anywhere in that file name
    entities = ["customers", "sales", "claims", "cars", "policy"]
    for entity in entities:
        if entity in file_name:
            table = f"primeins.bronze.{entity}"
            
            print(f"Executing test for: {file_name}")
            
            file_format = file_name.split('.')[-1]
            # 3. Run the test
            is_successful = run_bronze_unit_test(raw_file_path=test_file, bronze_table_name=table,file_format=file_format)

            # 4. Hard fail the notebook if the test fails 
            # (Added the filename to the error so you know exactly which one broke!)
            if not is_successful:
                raise AssertionError(f"Bronze ingestion unit test failed for {file_name}!")
    
