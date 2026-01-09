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
    Decides which month to process today based on the Cursor.
    """
    cursor_str = Variable.get(CURSOR_KEY, default_var=None)
    
    target_date = None

    if not cursor_str:
        # Case A: First Run ever. Find the start.
        logger.info("No cursor found. Fetching earliest date from DB...")
        # Note: We must ensure Transform runs BEFORE this check in the DAG flow
        # But for logic simplicity, we assume data exists or this task runs after transform
        earliest = get_earliest_date_from_db()
        target_date = earliest.replace(day=1) # Start of that month
        logger.info(f"Earliest data found: {target_date.strftime('%Y-%m')}")
    else:
        # Case B: We have history. Next month!
        last_processed = datetime.strptime(cursor_str, "%Y-%m")
        target_date = last_processed + relativedelta(months=1)
        logger.info(f"Cursor found ({cursor_str}). Moving to next month: {target_date.strftime('%Y-%m')}")

    # Case C: Stop if we reached the future
    # If target_date is This Month or Future, we might want to wait or just run normally
    current_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    if target_date >= current_month_start:
        logger.info("We have caught up to the present! Processing strictly last month only.")
        # Logic: If we are 'caught up', we just behave like a normal monthly reporter
        # We set target to Last Month
        target_date = current_month_start - relativedelta(months=1)
    
    # Format for Analytics
    start_str = target_date.strftime("%Y-%m-01")
    end_date = target_date + relativedelta(months=1)
    end_str = end_date.strftime("%Y-%m-01")
    
    return {
        "year_month": target_date.strftime("%Y-%m"), # For updating cursor later
        "start": start_str,
        "end": end_str
    }

def update_cursor_task(**context):
    """Updates the Airflow Variable so tomorrow we know what to do."""
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    processed_month = window["year_month"]
    
    # Safety: Don't update cursor if we are just re-running current month
    current_month = datetime.utcnow().strftime("%Y-%m")
    if processed_month < current_month:
        Variable.set(CURSOR_KEY, processed_month)
        logger.info(f"SUCCESS: Cursor updated to {processed_month}")
    else:
        logger.info("Caught up to present. Cursor remains at last completed month.")

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