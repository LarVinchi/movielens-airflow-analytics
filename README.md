# 🎬 Monthly Movie Analytics & Reporting Pipeline

## 📖 Project Overview

This project is a **full-stack Data Engineering pipeline** orchestrated with **Apache Airflow** and containerized with **Docker**. It simulates a real-world analytics workflow where historical **MovieLens** data is ingested on a **monthly** basis, modeled into a **Star Schema**, and automatically reported via **daily email summaries**.

The goal of this project is to demonstrate **production-grade data engineering practices**, including orchestration, data modeling, idempotent pipelines, and automated reporting.

---

## 🏗️ Architecture

The pipeline is intentionally **decoupled** into two independent workflows:

### 1️⃣ Ingestion Layer (Monthly)

* **Source:** MovieLens Dataset
* **Extract:** Downloads raw CSV files
* **Transform:** Cleans strings, dates, and schema inconsistencies using **Pandas**
* **Load:** Writes data into a **PostgreSQL** Data Warehouse

#### Data Modeling

* Transforms raw data into a **Star Schema** for analytics:

  * `fact_ratings`
  * `dim_movie`
  * `dim_user`
  * `dim_date`

#### Optimization

* Builds a flattened **Gold Layer** reporting table to accelerate downstream analytical queries

---

### 2️⃣ Reporting Layer (Daily)

* **Execution:** Stateful Airflow DAG
* **Backfill Logic:** Processes historical months day-by-day
* **State Management:** Uses a database cursor to track the last processed period
* **Guarantees:** No skipped or duplicated reports

#### Output

* Automatically generates a **PDF report** containing:

  * Top 10 Movies by Rating
  * Genre Trends Over Time
  * User Engagement Metrics
* Sends reports via **Email (SMTP)**

---

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow 2.9
* **Containerization:** Docker & Docker Compose
* **Database:** PostgreSQL 15
* **Language:** Python 3.9

  * Pandas
  * SQLAlchemy
* **Infrastructure:** Infrastructure as Code (IaC) using Docker Compose

---

## 📂 Project Structure

```text
.
├── dags/
│   ├── daily_reporter_dag.py        # Reporting: Query → PDF → Email
│   └── movie_ingestion_pipeline.py  # ETL: Extract → Transform → Load
├── sql/
│   ├── analytics.sql                # Named queries for reporting
│   ├── build_reporting_table.sql    # Gold layer DDL
│   ├── clean.sql                    # Data cleaning logic
│   ├── dimensions.sql               # Dimension tables DDL
│   ├── facts.sql                    # Fact tables DDL
│   └── staging.sql                  # Staging tables DDL
├── src/
│   ├── analytics/                   # Report generation logic
│   ├── config/                      # Configuration settings
│   ├── extract/                     # Data extraction scripts
│   ├── load/                        # Data loading scripts
│   ├── transform/                   # Data transformation scripts
│   └── utils/                       # Shared utilities (DB, logging)
├── .gitignore                       # Git ignore rules
├── docker-compose.yaml              # Infrastructure definition
└── pyproject.toml                   # Python project settings
```

---

## 🚀 How to Run

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/LarVinchi/movielens-airflow-analytics.git
cd movielens-airflow-analytics
```

---

### 2️⃣ Configure Environment Variables

Create a `.env` file in the project root:

```ini
# Airflow Core
AIRFLOW_UID=50000

# PostgreSQL Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Email / SMTP Configuration (Required for Reporting DAG)
# If using Gmail with 2FA, generate an App Password
GMAIL_USER=your_email@gmail.com
GMAIL_APP_PASSWORD=your_app_password_here
```

---

### 3️⃣ Start the Infrastructure

```bash
docker-compose up -d
```

Allow a few minutes for all services (PostgreSQL, Airflow Webserver, Scheduler) to initialize.

---

### 4️⃣ Access Airflow UI

* **URL:** [http://localhost:8080](http://localhost:8080)
* **Username:** admin
* **Password:** admin

---

### 5️⃣ Trigger the Pipelines

#### Ingestion Pipeline

* Enable **`movie_ingestion_pipeline`**
* Downloads MovieLens data and populates the PostgreSQL warehouse

#### Reporting Pipeline

* Enable **`daily_historical_reporter`**
* Generates historical daily reports from the earliest available data

---

## 💡 Key Engineering Decisions

### ✅ Decoupled SQL

All SQL logic is stored in `.sql` files, keeping Python code clean and enabling analysts to modify queries without changing application logic.

### ♻️ Idempotency

The ingestion pipeline uses database transactions and safe reload patterns, allowing DAG re-runs without creating duplicates.

### 🧠 Stateful Reporting

The reporting DAG maintains a persistent cursor, enabling natural backfilling and accurate historical report generation from the 1990s to present day.

---

## 📌 Use Cases Demonstrated

* Real-world ETL orchestration with Airflow
* Star schema data modeling
* Gold-layer analytics optimization
* Automated PDF reporting
* Production-style project structuring

---

## 📄 License

This project is for educational and portfolio purposes.
