from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import sys
import os

# 1. Setup Path to import src
sys.path.append("/opt/airflow") 
from src.extract import extract_movielens
from src.load import load_to_staging
from src.transform import run_clean_layer

# 2. Path to SQL folder
SQL_PATH = "/opt/airflow/sql"

with DAG(
    dag_id="movie_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly", 
    catchup=False,
    template_searchpath=[SQL_PATH] 
) as dag:

    # Step 1: Extract (Download Zip)
    extract = PythonOperator(
        task_id="extract_movielens",
        python_callable=extract_movielens.main 
    )

    # Step 2: Load (CSV to Raw Tables)
    load = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging.main 
    )

    # Step 3: Transform (Raw to Clean Tables)
    transform = PythonOperator(
        task_id="run_clean_layer",
        python_callable=run_clean_layer.main 
    )

    # Step 4: Build Dimensions (SQL)
    # This must run clean.sql or staging.sql if needed first, then dimensions
    build_dims = PostgresOperator(
        task_id="build_dimensions",
        postgres_conn_id="postgres_default",
        sql="dimensions.sql"
    )

    # Step 5: Build Facts (SQL)
    # This creates the 'fact.fact_ratings' table used by your reporter
    build_facts = PostgresOperator(
        task_id="build_facts",
        postgres_conn_id="postgres_default",
        sql="facts.sql"
    )

    # Execution Flow
    extract >> load >> transform >> build_dims >> build_facts