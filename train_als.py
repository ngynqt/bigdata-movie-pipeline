"""
Machine Learning Training Job
Trains an Alternating Least Squares (ALS) Collaborative Filtering model on the Silver data.
Evaluates performance via RMSE and exports recommendations to JSON.
"""
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from delta import configure_spark_with_delta_pip
import os
import sys

def main():
    builder = SparkSession.builder \
        .appName("ML-ALS-Training") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.format("delta").load("data/silver/")
        
        if df.count() == 0:
            print("[WARN] Silver layer is empty. Skipping model training.")
            sys.exit(0)

        # Split data for evaluation
        (training, test) = df.randomSplit([0.8, 0.2])

        als = ALS(
            userCol="user_id",
            itemCol="movie_id",
            ratingCol="rating",
            coldStartStrategy="drop"
        )

        # Model Evaluation phase
        print("[INFO] Training ALS model for evaluation...")
        model_test = als.fit(training)
        predictions = model_test.transform(test)
        
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        try:
            rmse = evaluator.evaluate(predictions)
            print(f"[INFO] Model Evaluation - Root Mean Square Error (RMSE): {rmse:.4f}")
        except Exception:
            print("[WARN] Insufficient test data to compute RMSE.")

        # Full Training phase
        print("[INFO] Training final ALS model on full dataset...")
        model = als.fit(df)

        # Generate top 5 recommendations per user
        rec = model.recommendForAllUsers(5)

        # Export predictions
        print("[INFO] Exporting recommendations to JSON...")
        os.makedirs("data", exist_ok=True)
        
        pandas_df = rec.toPandas()
        pandas_df.to_json("data/recommendations.json", orient="records")
        print("[INFO] Recommendations successfully exported to data/recommendations.json.")

    except Exception as e:
        print(f"[ERROR] Model training pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
