"""
Spark Structured Streaming Job
Consumes real-time event streams from Kafka, parses the JSON payload, 
and persists the raw data into the Bronze Delta table.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType
from delta import configure_spark_with_delta_pip
import sys

def main():
    builder = SparkSession.builder \
        .appName("Streaming-Kafka-to-Bronze") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Include Kafka integration package
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("movie_id", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    print("[INFO] Initializing Spark Structured Streaming from Kafka...")

    try:
        # Read stream from Kafka
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "movie_ratings") \
            .option("startingOffsets", "latest") \
            .load()

        # Deserialize JSON payload
        df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        # Sink stream to Bronze Delta table
        query = df_parsed.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "data/checkpoint/") \
            .start("data/bronze/")

        print("[INFO] Streaming query actively running. Awaiting termination...")
        query.awaitTermination()
    except Exception as e:
        print(f"[ERROR] Streaming job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
