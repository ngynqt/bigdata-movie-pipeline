"""
Silver Layer ETL Job
Reads raw data from Bronze Delta tables, performs data cleansing (deduplication, null handling),
and writes the refined data to the Silver Delta layer.
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys

def main():
    # Initialize Spark Session with Delta Lake configurations
    builder = SparkSession.builder \
        .appName("ETL-Silver-Layer") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Extract from Bronze
        df_bronze = spark.read.format("delta").load("data/bronze/")

        # Data Cleansing: Deduplicate events and remove null records
        df_clean = df_bronze.dropDuplicates(["user_id", "movie_id", "timestamp"])
        df_clean = df_clean.na.drop()

        # Load to Silver
        df_clean.write.format("delta").mode("overwrite").save("data/silver/")
        print("[INFO] Successfully processed and written data to Silver layer.")
    except Exception as e:
        print(f"[ERROR] Failed to process Silver layer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
