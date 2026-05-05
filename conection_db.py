"""
Database Connection Utility
Demonstrates SQLAlchemy connection to the PostgreSQL Data Warehouse 
and executes analytical BI queries.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import sys

# Define database connection URI
DB_URI = 'postgresql://admin:adminpassword@localhost:5433/datawarehouse'

def execute_query(engine, query):
    """Executes a SQL query and returns a pandas DataFrame."""
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        print(f"[ERROR] Database query execution failed: {e}")
        sys.exit(1)

def main():
    try:
        engine = create_engine(DB_URI)
        print("[INFO] Database connection established.")
    except Exception as e:
        print(f"[ERROR] Failed to connect to database: {e}")
        sys.exit(1)

    print("\n--- SAMPLE FACT RECORD ---")
    query_fact = "SELECT * FROM fact_ratings LIMIT 5;"
    df_fact = execute_query(engine, query_fact)
    print(df_fact.head())

    print("\n--- TOP 10 MOVIES BY AVERAGE RATING ---")
    query_top_10 = """
        SELECT 
            m.title AS "Movie Title", 
            ROUND(AVG(f.rating)::numeric, 2) AS "Average Rating",
            COUNT(f.rating) AS "Total Ratings"
        FROM fact_ratings f
        JOIN dim_movie m ON f.movie_id = m.movie_id
        GROUP BY m.title
        ORDER BY "Average Rating" DESC, "Total Ratings" DESC
        LIMIT 10;
    """
    df_top_10 = execute_query(engine, query_top_10)
    print(df_top_10)

if __name__ == "__main__":
    main()