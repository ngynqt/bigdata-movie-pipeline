"""
Gold Layer ETL Job
Transforms Silver layer data into a Star Schema (Fact and Dimension tables) 
and loads it into a PostgreSQL Data Warehouse for BI consumption.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from delta import configure_spark_with_delta_pip
import random
import sys

def main():
    builder = SparkSession.builder \
        .appName("ETL-Gold-StarSchema") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Include PostgreSQL JDBC driver
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.postgresql:postgresql:42.6.0"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[INFO] Starting ETL pipeline for Gold Layer (Star Schema).")

    try:
        # Extract from Silver
        df_silver = spark.read.format("delta").load("data/silver/")

        # Database connection properties
        jdbc_url = "jdbc:postgresql://localhost:5433/datawarehouse"
        db_properties = {
            "user": "admin",
            "password": "adminpassword",
            "driver": "org.postgresql.Driver"
        }

        print("[INFO] Generating Dimension tables...")

        # 1. Movie Dimension (Static metadata mapping)
        movie_data = [
            (1, "Inception", "Sci-Fi", 2010), (2, "Interstellar", "Sci-Fi", 2014),
            (3, "The Dark Knight", "Action", 2008), (4, "Avatar", "Sci-Fi", 2009),
            (5, "The Matrix", "Sci-Fi", 1999), (6, "Avengers: Endgame", "Action", 2019),
            (7, "Joker", "Drama", 2019), (8, "Parasite", "Thriller", 2019),
            (9, "Spider-Man", "Action", 2002), (10, "Dune", "Sci-Fi", 2021),
            (11, "The Godfather", "Crime", 1972), (12, "Pulp Fiction", "Crime", 1994),
            (13, "Fight Club", "Drama", 1999), (14, "Forrest Gump", "Drama", 1994),
            (15, "Gladiator", "Action", 2000), (16, "Titanic", "Romance", 1997),
            (17, "Star Wars", "Sci-Fi", 1977), (18, "Jurassic Park", "Adventure", 1993),
            (19, "The Lion King", "Animation", 1994), (20, "The Terminator", "Sci-Fi", 1984),
            (21, "Alien", "Sci-Fi", 1979), (22, "Die Hard", "Action", 1988),
            (23, "Rocky", "Drama", 1976), (24, "Jaws", "Thriller", 1975),
            (25, "E.T.", "Sci-Fi", 1982), (26, "Braveheart", "Action", 1995),
            (27, "Goodfellas", "Crime", 1990), (28, "Se7en", "Crime", 1995),
            (29, "The Silence of the Lambs", "Thriller", 1991), (30, "Schindler's List", "Drama", 1993)
        ]
        movie_schema = StructType([
            StructField("movie_id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("release_year", IntegerType(), True)
        ])
        dim_movie = spark.createDataFrame(movie_data, movie_schema)
        
        # 2. User Dimension (Simulated profile data)
        locations = ["New York", "London", "Tokyo", "Hanoi", "Paris", "Berlin"]
        tiers = ["Basic", "Standard", "Premium"]
        user_data = [
            (i, f"User_{i}", random.randint(18, 60), random.choice(["M", "F"]), random.choice(locations), random.choice(tiers))
            for i in range(1, 151)
        ]
        user_schema = StructType([
            StructField("user_id", IntegerType(), False),
            StructField("username", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("location", StringType(), True),
            StructField("subscription_tier", StringType(), True)
        ])
        dim_user = spark.createDataFrame(user_data, user_schema)

        # 3. Date Dimension & Fact Table processing
        df_with_date = df_silver.withColumn("dt", from_unixtime(col("timestamp")))
        df_with_date = df_with_date.withColumn("date_id", date_format(col("dt"), "yyyyMMdd").cast("integer"))

        dim_date = df_with_date.select("date_id", "dt").distinct() \
            .withColumn("year", year("dt")) \
            .withColumn("month", month("dt")) \
            .withColumn("day", dayofmonth("dt")) \
            .drop("dt")

        fact_ratings = df_with_date.select("user_id", "movie_id", "date_id", "rating")

        # Load to PostgreSQL Data Warehouse
        print("[INFO] Writing Star Schema to PostgreSQL Data Warehouse...")
        
        dim_movie.write.jdbc(url=jdbc_url, table="dim_movie", mode="overwrite", properties=db_properties)
        print("       [OK] Persisted dim_movie")

        dim_user.write.jdbc(url=jdbc_url, table="dim_user", mode="overwrite", properties=db_properties)
        print("       [OK] Persisted dim_user")

        dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=db_properties)
        print("       [OK] Persisted dim_date")

        fact_ratings.write.jdbc(url=jdbc_url, table="fact_ratings", mode="overwrite", properties=db_properties)
        print("       [OK] Persisted fact_ratings")

        print("[INFO] Gold Layer ETL successfully completed.")

    except Exception as e:
        print(f"[ERROR] Failed to execute Gold Layer ETL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()