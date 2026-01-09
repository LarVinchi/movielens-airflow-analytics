from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import os

from src.extract import extract_movielens
from src.load import load_to_staging
from src.transform import run_clean_layer
from src.analytics.monthly_reports import run_monthly_reports
from src.utils.db import get_engine
from src.utils.logging import get_logger

logger = get_logger(__name__)

RECIPIENT_EMAIL = os.getenv("AIRFLOW__SMTP__SMTP_USER") 
CURSOR_KEY = "movielens_report_cursor"

def get_earliest_date_from_db():
    """Query the database to find the very first rating date."""
    engine = get_engine()
    with engine.connect() as conn:
        # Adjust table name if needed based on your staging/clean schema
        result = conn.execute("SELECT MIN(rating_timestamp) FROM clean.ratings")
        min_date = result.scalar()
        if not min_date:
            # Fallback if DB is empty, default to a safe start
            return datetime(1995, 1, 1)
        return min_date

def resolve_next_window(**context):
    """
    Decides which month to process.
    PRIORITY 1: Manual Config (if you triggered it manually).
    PRIORITY 2: Cursor (Automatic daily catchup).
    """
    # 1. Check for Manual Configuration
    dag_conf = context["dag_run"].conf
    if dag_conf and "start_date" in dag_conf:
        # User entered JSON: {"start_date": "1996-05"}
        manual_date = dag_conf["start_date"]
        logger.info(f"MANUAL RUN DETECTED: {manual_date}")
        
        # Calculate end date
        dt = datetime.strptime(manual_date, "%Y-%m")
        end_dt = dt + relativedelta(months=1)
        
        return {
            "year_month": manual_date,
            "start": dt.strftime("%Y-%m-01"),
            "end": end_dt.strftime("%Y-%m-01"),
            "mode": "manual" # <--- FLAG TO SKIP CURSOR
        }

    # 2. Automatic Cursor Logic (If no manual config)
    cursor_str = Variable.get(CURSOR_KEY, default_var=None)
    
    if not cursor_str:
        # First Run
        logger.info("No cursor. Fetching DB earliest...")
        earliest = get_earliest_date_from_db()
        target_date = earliest.replace(day=1) 
    else:
        # Next Month
        last_processed = datetime.strptime(cursor_str, "%Y-%m")
        target_date = last_processed + relativedelta(months=1)

    # Check against future
    current_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if target_date >= current_month_start:
        target_date = current_month_start - relativedelta(months=1)

    return {
        "year_month": target_date.strftime("%Y-%m"),
        "start": target_date.strftime("%Y-%m-01"),
        "end": (target_date + relativedelta(months=1)).strftime("%Y-%m-01"),
        "mode": "auto"
    }

def update_cursor_task(**context):
    """Only update cursor if this was an AUTOMATIC run."""
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    
    if window.get("mode") == "manual":
        logger.info("Manual run detected. Skipping cursor update to protect history.")
        return

    # Normal update logic
    processed_month = window["year_month"]
    Variable.set(CURSOR_KEY, processed_month)
    logger.info(f"SUCCESS: Cursor updated to {processed_month}")

def run_analytics_wrapper(**context):
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    return run_monthly_reports(window["start"], window["end"])

# --- DAG Definition ---
with DAG(
    "daily_historical_reporter",
    start_date=datetime(2023, 1, 1),
    schedule="0 12 * * *", # Run Daily at 12 PM
    catchup=False
) as dag:

    # 1. Standard ETL (Always run to ensure we have fresh data if new files arrive)
    extract = PythonOperator(task_id="extract", python_callable=extract_movielens.main)
    load = PythonOperator(task_id="load", python_callable=load_to_staging.main)
    transform = PythonOperator(task_id="transform", python_callable=run_clean_layer.main)

    # 2. The Brain (Decide Date)
    resolve_window = PythonOperator(
        task_id="resolve_window",
        python_callable=resolve_next_window
    )

    # 3. Run Report
    analytics = PythonOperator(
        task_id="run_analytics",
        python_callable=run_analytics_wrapper
    )

    # 4. Email
    email = EmailOperator(
        task_id="send_email",
        to=RECIPIENT_EMAIL,
        subject="Movie Reports: {{ ti.xcom_pull(task_ids='resolve_window')['year_month'] }}",
        html_content="<h3>Historical Report Catchup</h3><p>Attached is the report for the requested month.</p>",
        files="{{ ti.xcom_pull(task_ids='run_analytics') }}"
    )

    # 5. Save Progress
    save_cursor = PythonOperator(
        task_id="save_cursor",
        python_callable=update_cursor_task
    )

    extract >> load >> transform >> resolve_window >> analytics >> email >> save_cursor