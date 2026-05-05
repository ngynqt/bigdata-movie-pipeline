# Real-Time Movie Recommendation Pipeline

A production-grade Big Data pipeline that ingests real-time user rating events, processes them through a **Medallion Architecture** (Bronze → Silver → Gold), trains a Machine Learning model for personalized recommendations, and exposes results via a Netflix-style web interface and a PostgreSQL Data Warehouse.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Data Model (Star Schema)](#data-model-star-schema)
- [Pipeline Components](#pipeline-components)
- [Setup and Installation](#setup-and-installation)
- [Running the Pipeline](#running-the-pipeline)
- [Analytical Queries (BI)](#analytical-queries-bi)

---

## Architecture Overview

The system implements the **Medallion Architecture**, a multi-layered data lake pattern that progressively refines raw event data into two distinct consumption paths: **Machine Learning** and **Business Intelligence**.

```
+-------------------------+     +-------------------------+
|   Flask Web App         |     |   Data Simulator        |
|   (User Ratings UI)     |     |   (Synthetic Events)    |
+----------+--------------+     +----------+--------------+
           |                               |
           +---------------+---------------+
                           | Rating Events (JSON)
                           v
               +---------------------+
               |    Apache Kafka     |  <- Message Broker / Event Bus
               +----------+----------+
                          |
                          v
          +-------------------------------+
          |  Spark Structured Streaming   |  <- Kafka Consumer
          +-------------------------------+
                          |
                          v
          +-------------------------------+
          |   Bronze Layer (Delta Lake)   |  <- Raw, append-only events
          +-------------------------------+
                          |
                          v
          +-------------------------------+
          |   Silver Layer (Delta Lake)   |  <- Cleaned, deduplicated data
          +-------------------------------+
                 |                  |
    ML Path      |                  |   BI Path
                 v                  v
  +----------------------------+  +-------------------------------+
  |  ALS Recommendation Model |  |  Gold Layer ETL               |
  |  (Spark MLlib)            |  |  (Star Schema Construction)   |
  +----------------------------+  +-------------------------------+
                 |                              |
                 v                              v
  +----------------------------+  +-------------------------------+
  |  recommendations.json      |  |  PostgreSQL Data Warehouse    |
  |  (Top-5 per user)          |  |  (fact + dim tables)          |
  +----------------------------+  +-------------------------------+
                 |                              |
                 v                              v
  +----------------------------+  +-------------------------------+
  |  Web App Recommendations   |  |  DBeaver / BI Tools           |
  |  (REST API /api/recs)      |  |  (SQL Analytics)              |
  +----------------------------+  +-------------------------------+
```

**Key architectural decision — two independent consumption paths from Silver:**

- **ML Path:** Silver → ALS Model → JSON → Web API. Spark reads Delta Lake (columnar Parquet) directly, which is significantly faster than reading via JDBC for large-scale training workloads.
- **BI Path:** Silver → Star Schema ETL → PostgreSQL → BI Tools. The Data Warehouse is optimized for ad-hoc analytical SQL queries with proper Fact/Dimension relationships.

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| Ingestion | Apache Kafka | 3.7.0 | Distributed event streaming and message brokering |
| Processing | Apache Spark (Structured Streaming) | 3.5.1 | Batch and stream processing |
| Storage | Delta Lake | 3.1.0 | ACID-compliant Data Lake with time travel |
| Data Warehouse | PostgreSQL | 15 | Relational Star Schema for BI consumption |
| Machine Learning | Spark MLlib (ALS) | 3.5.1 | Collaborative Filtering recommendations |
| Web Application | Flask | 3.x | REST API and frontend serving |
| Infrastructure | Docker Compose | — | Container orchestration (Kafka + PostgreSQL) |
| DB Connectivity | SQLAlchemy, psycopg2-binary | — | Python-to-PostgreSQL interface |

---

## Project Structure

```
.
├── docker-compose.yml          # Infrastructure: Kafka broker + PostgreSQL DW
├── requirements.txt            # Python dependencies
├── run_demo.sh                 # One-click pipeline initialization script
│
├── data_simulator.py           # Kafka producer: synthetic high-throughput event generator
├── spark_streaming_delta.py    # Spark Streaming: Kafka → Bronze Delta table
├── batch_silver.py             # Batch ETL: Bronze → Silver (cleanse & deduplicate)
├── etl_gold_warehouse.py       # Batch ETL: Silver → Gold Star Schema → PostgreSQL
│
├── train_als.py                # ML Training: ALS Collaborative Filtering on Silver data
├── continuous_ml.py            # Daemon: orchestrates Silver → Gold → ML every 20 seconds
│
├── app.py                      # Flask: REST API (/api/rate, /api/recommendations)
├── conection_db.py             # Utility: analytical SQL queries via SQLAlchemy
├── demo_time_travel.py         # Demo: Delta Lake time travel (data versioning)
│
├── templates/
│   └── index.html              # Netflix-style frontend
├── static/
│   └── style.css               # Frontend styles
└── data/                       # Runtime directory (auto-generated, git-ignored)
    ├── bronze/                 # Raw Delta table
    ├── silver/                 # Cleaned Delta table
    ├── checkpoint/             # Spark streaming checkpoint
    └── recommendations.json    # ALS model output (served by Flask API)
```

---

## Data Model (Star Schema)

The Gold Layer (PostgreSQL) implements a **3-dimension Star Schema** optimized for BI analytical queries. The schema answers the three fundamental analytical dimensions: **Who** (user), **What** (movie), and **When** (date).

```
                  +----------------+
                  |   dim_date     |
                  |----------------|
                  | date_id  (PK)  |
                  | year           |
                  | month          |
                  | day            |
                  +-------+--------+
                          |
+-----------------+   +---+-------------+   +------------------+
|   dim_user      |   |  fact_ratings   |   |   dim_movie      |
|-----------------|   |-----------------|   |------------------|
| user_id    (PK) +---+ user_id    (FK) +---+ movie_id    (PK) |
| username        |   | movie_id   (FK) |   | title            |
| age             |   | date_id    (FK) |   | genre            |
| gender          |   | rating          |   | release_year     |
| location        |   +-----------------+   +------------------+
| subscription    |
+-----------------+
```

| Table | Type | Row Count | Description |
|---|---|---|---|
| `fact_ratings` | Fact | Dynamic | Core event table. Each row is a single rating event with FK references to all three dimension tables. |
| `dim_user` | Dimension | 150 | User profile attributes: demographics, location, subscription tier. |
| `dim_movie` | Dimension | 30 | Movie catalog metadata: title, genre, release year. |
| `dim_date` | Dimension | Dynamic | Calendar breakdown derived from event Unix timestamps. |

> **Note:** `dim_movie` stores one primary genre per title. This is a deliberate scope decision to maintain a clean Star Schema. A production extension would introduce a `bridge_movie_genre` table for the many-to-many relationship.

---

## Pipeline Components

### 1. Data Simulator (`data_simulator.py`)

Publishes synthetic rating events to Kafka at **~100 events/second** (50 events per 0.5s batch). Timestamps are randomized across the past 365 days to simulate realistic historical data distribution for time-series BI analysis.

### 2. Spark Structured Streaming — Bronze (`spark_streaming_delta.py`)

Subscribes to Kafka topic `movie_ratings`, deserializes the JSON payload, and appends raw records to the Bronze Delta table. A checkpoint directory guarantees exactly-once processing semantics across restarts.

### 3. Silver ETL Batch Job (`batch_silver.py`)

Reads from Bronze and applies two data quality transformations:
- **Deduplication:** Drops duplicate events on composite key `(user_id, movie_id, timestamp)`.
- **Null Removal:** Drops any record with null values in any column.

Writes the clean output to the Silver Delta table.

### 4. Gold ETL — Star Schema (`etl_gold_warehouse.py`)

Reads from Silver and constructs the Star Schema for PostgreSQL:
- `dim_date`: Derived from event timestamps using Spark date functions (`year`, `month`, `dayofmonth`).
- `dim_user`: Simulated profiles for 150 users (sufficient to cover all user IDs generated by the simulator).
- `dim_movie`: Static catalog of 30 movie titles with genre and release year.
- `fact_ratings`: Joins event data with the date surrogate key and selects `(user_id, movie_id, date_id, rating)`.

### 5. ALS Recommendation Model (`train_als.py`)

**Reads directly from Silver (Delta Lake)**, not from PostgreSQL. This is by design: Spark reads columnar Parquet files orders of magnitude faster than pulling data through a JDBC connection at training scale.

Training process:
1. Splits Silver data 80/20 into training and evaluation sets.
2. Trains ALS on the 80% set and computes **RMSE** on the 20% test set.
3. Retrains a final model on 100% of available data.
4. Generates top-5 recommendations per user and writes to `data/recommendations.json`.

### 6. Continuous ML Orchestrator (`continuous_ml.py`)

A long-running daemon that executes the following cycle every **20 seconds**:

```
Silver ETL → Gold ETL → ALS Training → Sleep(20s) → Repeat
```

This keeps both the Data Warehouse and the recommendation artifacts synchronized with the latest streaming data, achieving a **near-real-time update cadence**.

### 7. Flask Web Application (`app.py`)

Exposes two REST endpoints:

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/rate` | Accepts `{user_id, movie_id, rating}` and publishes to Kafka. |
| `GET` | `/api/recommendations/<user_id>` | Returns top-5 ALS recommendations for a user from the cached JSON artifact. |

### 8. Delta Lake Time Travel (`demo_time_travel.py`)

Demonstrates Delta Lake's audit and rollback capability by reading the Bronze table at `versionAsOf=0`, retrieving the exact state of the dataset at initial ingestion regardless of all subsequent writes.

---

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- Apache Spark with `SPARK_HOME` set and `spark-submit` available in `PATH`

### 1. Create Virtual Environment

```bash
python3 -m venv bdata_env
source bdata_env/bin/activate.fish   # Fish shell
# or
source bdata_env/bin/activate        # Bash / Zsh
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

### Step 1 — Initialize Infrastructure and Bootstrap Data

```bash
bash run_demo.sh
```

This script executes the full initialization sequence:
1. Purges the `data/` directory.
2. Starts Kafka and PostgreSQL via Docker Compose.
3. Launches the Data Simulator (background process).
4. Launches Spark Structured Streaming — Bronze (background process).
5. Polls until the Bronze Delta table is detected, then buffers for 10 seconds.
6. Runs sequentially: Silver ETL → Gold ETL → ALS Training → Time Travel Demo.
7. Terminates background processes on completion.

---

### Step 2 — Start the Web Application

```bash
python app.py
```

Navigate to: **http://127.0.0.1:5000**

---

### Step 3 — Start the Continuous Orchestrator

Open a separate terminal:

```bash
python continuous_ml.py
```

The orchestrator refreshes the Data Warehouse and recommendation model every 20 seconds. Cycle progress is printed to stdout.

---

### Step 4 — (Optional) Run the High-Volume Simulator

To rapidly load data for BI testing:

```bash
python data_simulator.py
```

---

## Analytical Queries (BI)

Connect any BI tool to the PostgreSQL Data Warehouse using the following credentials:

| Parameter | Value |
|---|---|
| Host | `localhost` |
| Port | `5433` |
| Database | `datawarehouse` |
| User | `admin` |
| Password | `adminpassword` |

Alternatively, run the bundled utility:

```bash
python conection_db.py
```

**Top 10 movies by average rating:**

```sql
SELECT
    m.title                          AS "Movie Title",
    ROUND(AVG(f.rating)::numeric, 2) AS "Average Rating",
    COUNT(f.rating)                  AS "Total Ratings"
FROM fact_ratings f
JOIN dim_movie m ON f.movie_id = m.movie_id
GROUP BY m.title
ORDER BY "Average Rating" DESC, "Total Ratings" DESC
LIMIT 10;
```

**Rating trend by month:**

```sql
SELECT
    d.year                           AS "Year",
    d.month                          AS "Month",
    COUNT(f.rating)                  AS "Total Ratings",
    ROUND(AVG(f.rating)::numeric, 1) AS "Average Rating"
FROM fact_ratings f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;
```

**Ratings breakdown by user location and movie genre:**

```sql
SELECT
    u.location                       AS "Country",
    m.genre                          AS "Genre",
    ROUND(AVG(f.rating)::numeric, 2) AS "Average Rating",
    COUNT(f.rating)                  AS "Total Ratings"
FROM fact_ratings f
JOIN dim_user  u ON f.user_id  = u.user_id
JOIN dim_movie m ON f.movie_id = m.movie_id
GROUP BY u.location, m.genre
ORDER BY "Total Ratings" DESC
LIMIT 10;
```
