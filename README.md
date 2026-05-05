# Real-Time Movie Recommendation Pipeline

A production-grade Big Data pipeline that ingests real-time user rating events, processes them through a Medallion Architecture (Bronze → Silver → Gold), and serves personalized movie recommendations via a Machine Learning model.

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

The system follows the **Medallion Architecture**, a layered data lake pattern that progressively refines raw event data into analytics-ready datasets.

```
[ Web App / Simulator ]
          |
          | (Rating Events)
          v
  [ Apache Kafka ]           <- Message Broker / Event Bus
          |
          v
  [ Spark Structured Streaming ]
          |
          v
  [ Bronze Layer (Delta Lake) ]  <- Raw, immutable append-only events
          |
          v
  [ Silver Layer (Delta Lake) ]  <- Cleaned, deduplicated data
          |
          v
  [ Gold Layer (PostgreSQL) ]    <- Star Schema Data Warehouse
          |
          v
  [ BI Tools / Web API ]         <- Analytics & Recommendations
```

Data flow:
1. A **Data Simulator** and a **Flask Web App** publish rating events to Kafka at high throughput.
2. **Spark Structured Streaming** consumes the Kafka topic and writes raw events to the **Bronze** Delta table.
3. A **Batch ETL job** reads Bronze, applies data cleansing rules, and writes the result to **Silver**.
4. A **Gold ETL job** constructs a Star Schema from Silver and loads it into a **PostgreSQL Data Warehouse**.
5. A **Machine Learning job** trains an ALS Collaborative Filtering model on Silver data and exports recommendations.
6. A **Continuous Orchestrator** daemon cycles through steps 3–5 every 20 seconds, enabling near-real-time updates.

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Apache Kafka 3.7 | Distributed event streaming |
| Processing | Apache Spark 3.5.1 (Structured Streaming) | Batch & stream processing |
| Storage | Delta Lake 3.1.0 | ACID-compliant data lake storage |
| Data Warehouse | PostgreSQL 15 | Analytical Star Schema storage |
| Machine Learning | Spark MLlib (ALS) | Collaborative Filtering recommendations |
| Web Application | Flask | REST API and frontend serving |
| Infrastructure | Docker Compose | Service orchestration |
| DB Client | SQLAlchemy, psycopg2 | Python-to-PostgreSQL connectivity |

---

## Project Structure

```
.
├── docker-compose.yml          # Infrastructure: Kafka broker + PostgreSQL DW
├── requirements.txt            # Python dependencies
├── run_demo.sh                 # One-click pipeline initialization script
│
├── data_simulator.py           # Kafka producer: synthetic rating event generator
├── spark_streaming_delta.py    # Spark Streaming: Kafka → Bronze Delta table
├── batch_silver.py             # Batch ETL: Bronze → Silver (cleanse & deduplicate)
├── etl_gold_warehouse.py       # Batch ETL: Silver → Gold (Star Schema → PostgreSQL)
│
├── train_als.py                # ML Training: ALS Collaborative Filtering model
├── continuous_ml.py            # Orchestrator: cyclically runs Silver→Gold→ML every 20s
│
├── app.py                      # Flask REST API and frontend server
├── conection_db.py             # Utility: analytical SQL queries via SQLAlchemy
├── demo_time_travel.py         # Demo: Delta Lake time travel (data versioning)
│
├── templates/
│   └── index.html              # Netflix-style frontend UI
├── static/
│   └── style.css               # Frontend styles
└── data/                       # Runtime data directory (auto-generated)
    ├── bronze/                 # Raw Delta table
    ├── silver/                 # Cleaned Delta table
    ├── checkpoint/             # Spark streaming checkpoint
    └── recommendations.json    # ALS model output
```

---

## Data Model (Star Schema)

The Gold Layer implements a standard Star Schema optimized for analytical workloads. All dimension tables are linked to the central fact table via foreign keys.

```
                    +---------------+
                    |   dim_date    |
                    |---------------|
                    | date_id (PK)  |
                    | year          |
                    | month         |
                    | day           |
                    +-------+-------+
                            |
+---------------+   +-------+-------+   +---------------------------+
|   dim_user    |   |  fact_ratings  |   |         dim_movie         |
|---------------|   |----------------|   |---------------------------|
| user_id (PK)  +---+ user_id (FK)   +---+ movie_id (PK)             |
| username      |   | movie_id (FK)  |   | title                     |
| age           |   | date_id (FK)   |   | genre                     |
| gender        |   | rating         |   | release_year              |
| location      |   +----------------+   +---------------------------+
| subscription  |
+---------------+
```

| Table | Type | Description |
|---|---|---|
| `fact_ratings` | Fact | Central event table. Stores each rating event with foreign keys to all dimension tables. |
| `dim_user` | Dimension | User profile attributes: demographics, location, subscription tier. |
| `dim_movie` | Dimension | Movie catalog metadata: title, genre, release year. |
| `dim_date` | Dimension | Date breakdown derived from event timestamps: year, month, day. |

---

## Pipeline Components

### 1. Data Ingestion (`data_simulator.py`)

Simulates a high-throughput event stream. Publishes 50 rating events per 0.5 seconds (approx. 100 events/second) to the Kafka topic `movie_ratings`. Timestamps are randomized across the past 365 days to simulate historical data distribution.

### 2. Spark Structured Streaming — Bronze (`spark_streaming_delta.py`)

Subscribes to the Kafka topic, deserializes the JSON payload, and appends raw records to the Bronze Delta table. Maintains a checkpoint to guarantee exactly-once processing semantics on restart.

### 3. Silver ETL Batch Job (`batch_silver.py`)

Reads from Bronze and applies two data quality rules:
- **Deduplication:** Removes duplicate events using the composite key `(user_id, movie_id, timestamp)`.
- **Null Removal:** Drops any record containing null values.

Writes the clean dataset to the Silver Delta table using `overwrite` mode.

### 4. Gold ETL — Star Schema (`etl_gold_warehouse.py`)

Constructs the Star Schema from Silver data and loads it into PostgreSQL:
- `dim_date`: Extracted from event timestamps using Spark date functions.
- `dim_user`: Simulated user profiles covering 150 users (sufficient to cover all simulator-generated user IDs).
- `dim_movie`: Static mapping of 30 movie titles with genre and release year metadata.
- `fact_ratings`: Core event table with foreign key references to all dimension tables.

### 5. ALS Recommendation Model (`train_als.py`)

Trains a Collaborative Filtering model using Spark MLlib's **Alternating Least Squares (ALS)** algorithm:
- Splits data 80/20 for training and evaluation.
- Reports **RMSE** (Root Mean Square Error) as the accuracy metric.
- Trains a final model on 100% of available data.
- Exports top-5 recommendations per user to `data/recommendations.json`.

### 6. Continuous ML Orchestrator (`continuous_ml.py`)

A long-running daemon that cyclically executes the pipeline on a configurable interval (default: 20 seconds):
```
Silver ETL → Gold ETL → ALS Training → Sleep → Repeat
```
This ensures the Data Warehouse and recommendation artifacts remain synchronized with the latest incoming events.

### 7. Flask Web Application (`app.py`)

Exposes two REST endpoints:
- `POST /api/rate` — Accepts a rating payload and publishes it to Kafka.
- `GET /api/recommendations/<user_id>` — Returns the top-5 AI-generated movie recommendations for a user from the cached JSON artifact.

### 8. Delta Lake Time Travel (`demo_time_travel.py`)

Demonstrates Delta Lake's data versioning capability by querying the Bronze table at snapshot `versionAsOf=0`, retrieving the initial state of the dataset regardless of subsequent writes.

---

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- Apache Spark (with `SPARK_HOME` configured in the environment)

### 1. Create and Activate Virtual Environment

```bash
python3 -m venv bdata_env
source bdata_env/bin/activate.fish   # Fish shell
# or
source bdata_env/bin/activate        # Bash/Zsh
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

### Step 1 — Initialize Infrastructure and Seed Data

Run the bootstrap script to start all Docker services and execute a full initial pipeline run:

```bash
bash run_demo.sh
```

This script performs the following sequence in order:
1. Purges any existing `data/` directory.
2. Starts Kafka and PostgreSQL containers via Docker Compose.
3. Launches the Data Simulator as a background process.
4. Launches Spark Streaming (Bronze ingestion) as a background process.
5. Waits for the Bronze Delta table to initialize.
6. Sequentially runs: Silver ETL → Gold ETL → ALS Training → Time Travel Demo.
7. Terminates background processes on completion.

---

### Step 2 — Start the Web Application

Open a new terminal and run:

```bash
python app.py
```

Access the UI at: **http://127.0.0.1:5000**

---

### Step 3 — Start the Continuous ML Orchestrator

Open a second terminal and run:

```bash
python continuous_ml.py
```

The orchestrator will log each cycle to stdout. The recommendation model and Data Warehouse are refreshed every 20 seconds.

---

### Step 4 — (Optional) Run High-Volume Data Simulation

To rapidly populate the Data Warehouse with large volumes of data:

```bash
python data_simulator.py
```

---

## Analytical Queries (BI)

The PostgreSQL Data Warehouse can be queried directly via any BI tool (DBeaver, Tableau, Power BI) or via the provided utility script.

**DBeaver Connection Details:**

| Parameter | Value |
|---|---|
| Host | localhost |
| Port | 5433 |
| Database | datawarehouse |
| User | admin |
| Password | adminpassword |

**Run utility queries:**

```bash
python conection_db.py
```

**Example — Top 10 movies by average rating:**

```sql
SELECT
    m.title         AS "Movie Title",
    ROUND(AVG(f.rating)::numeric, 2) AS "Average Rating",
    COUNT(f.rating) AS "Total Ratings"
FROM fact_ratings f
JOIN dim_movie m ON f.movie_id = m.movie_id
GROUP BY m.title
ORDER BY "Average Rating" DESC, "Total Ratings" DESC
LIMIT 10;
```

**Example — Rating trend by month:**

```sql
SELECT
    d.year   AS "Year",
    d.month  AS "Month",
    COUNT(f.rating)                  AS "Total Ratings",
    ROUND(AVG(f.rating)::numeric, 1) AS "Average Rating"
FROM fact_ratings f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;
```

**Example — Ratings breakdown by location and genre:**

```sql
SELECT
    u.location  AS "Country",
    m.genre     AS "Genre",
    ROUND(AVG(f.rating)::numeric, 2) AS "Average Rating",
    COUNT(f.rating)                  AS "Total Ratings"
FROM fact_ratings f
JOIN dim_user  u ON f.user_id  = u.user_id
JOIN dim_movie m ON f.movie_id = m.movie_id
GROUP BY u.location, m.genre
ORDER BY "Total Ratings" DESC
LIMIT 10;
```
