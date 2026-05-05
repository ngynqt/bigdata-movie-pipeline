"""
Delta Lake Time Travel Demonstration
Validates data versioning and schema evolution capabilities of Delta Lake.
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys

def main():
    builder = SparkSession.builder \
        .appName("Delta-TimeTravel") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("==================================================")
    print("[INFO] EXECUTING DELTA LAKE TIME TRAVEL DEMONSTRATION")
    print("==================================================\n")

    try:
        # Accessing the inaugural snapshot (version 0) of the Bronze layer
        df_old = spark.read.format("delta").option("versionAsOf", 0).load("data/bronze/")
        
        print("[INFO] Successfully retrieved snapshot Version 0.")
        print("--- Head of Version 0 ---")
        df_old.show(5)
    except Exception as e:
        print(f"[ERROR] Time travel execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
