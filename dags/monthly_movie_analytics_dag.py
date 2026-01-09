from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.models.param import Param 
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# --- Imports ---
from src.extract import extract_movielens
from src.load import load_to_staging
from src.transform import run_clean_layer
from src.analytics.monthly_reports import run_monthly_reports
from src.utils.db import get_engine
from src.utils.logging import get_logger
from src.utils.cursor import VARIABLE_KEY, set_cursor

logger = get_logger(__name__)

RECIPIENT_EMAIL = os.getenv("AIRFLOW__SMTP__SMTP_USER") 

def get_earliest_date_from_db():
    """Query the database to find the very first rating date."""
    engine = get_engine()
    # We query the staging table because it's the rawest source available 
    # after the load step, but clean is safer if transform runs first.
    # Since transform runs before this in our DAG, we query clean.
    query = "SELECT MIN(rating_timestamp) FROM clean.ratings"
    
    with engine.connect() as conn:
        result = conn.execute(query).scalar()
        
        if not result:
            # Fallback if DB is completely empty (shouldn't happen)
            logger.warning("No data found in DB. Defaulting to 1995-01.")
            return datetime(1995, 1, 1)
        
        return result

def resolve_next_window(**context):
    """
    Decides which month to process.
    """
    # --- 1. Check for Manual Configuration ---
    params = context["params"]
    manual_date = params.get("start_date")
    
    if manual_date:
        logger.info(f"MANUAL RUN DETECTED: {manual_date}")
        dt = datetime.strptime(manual_date, "%Y-%m")
        
        return {
            "year_month": manual_date,
            "start": dt.strftime("%Y-%m-01"),
            "end": (dt + relativedelta(months=1)).strftime("%Y-%m-01"),
            "mode": "manual"
        }

    # --- 2. Automatic Cursor Logic ---
    cursor_str = Variable.get(VARIABLE_KEY, default_var=None)
    target_date = None
    
    if not cursor_str:
        # First Run Ever
        logger.info("No cursor found. Querying DB for start date...")
        earliest = get_earliest_date_from_db()
        
        # --- FIX: STRIP TIMEZONE INFO ---
        # The DB returns a Timezone-Aware date, but utcnow() is Naive.
        # We remove the timezone to make them comparable.
        if earliest.tzinfo is not None:
            earliest = earliest.replace(tzinfo=None)
        # --------------------------------
            
        target_date = earliest.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        logger.info(f"Earliest date found: {target_date.strftime('%Y-%m')}")
    else:
        # Continue from last saved point
        last_processed = datetime.strptime(cursor_str, "%Y-%m")
        target_date = last_processed + relativedelta(months=1)
        logger.info(f"Cursor found ({cursor_str}). Next target: {target_date.strftime('%Y-%m')}")

    # --- 3. Future Guardrail ---
    current_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    if target_date >= current_month_start:
        logger.info("Caught up to present! Processing last completed month only.")
        target_date = current_month_start - relativedelta(months=1)
    
    return {
        "year_month": target_date.strftime("%Y-%m"),
        "start": target_date.strftime("%Y-%m-01"),
        "end": (target_date + relativedelta(months=1)).strftime("%Y-%m-01"),
        "mode": "auto"
    }

def update_cursor_task(**context):
    """
    Updates the Airflow Variable only if the run was Automatic.
    """
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    mode = window.get("mode")
    processed_month = window.get("year_month")
    
    if mode == "manual":
        logger.info(f"Manual run for {processed_month} completed. Cursor NOT updated.")
        return

    # Check against future again to be safe
    current_month = datetime.utcnow().strftime("%Y-%m")
    if processed_month < current_month:
        set_cursor(processed_month)
    else:
        logger.info("Already at current month. Cursor update skipped.")

def run_analytics_wrapper(**context):
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    return run_monthly_reports(window["start"], window["end"])

# --- DAG Definition ---
with DAG(
    "daily_historical_reporter",
    start_date=datetime(2023, 1, 1),
    schedule="0 12 * * *", # Daily at 12:00 PM
    catchup=False,
    tags=["movielens", "reporting"],
    # This enables the UI Form!
    params={
        "start_date": Param(
            default=None, 
            type=["null", "string"], 
            description="Format: YYYY-MM. Leave empty for automatic daily catchup."
        )
    }
) as dag:

    # 1. Standard ETL (Idempotent)
    extract = PythonOperator(task_id="extract", python_callable=extract_movielens.main)
    load = PythonOperator(task_id="load", python_callable=load_to_staging.main)
    transform = PythonOperator(task_id="transform", python_callable=run_clean_layer.main)

    # 2. Decide Date
    resolve_window = PythonOperator(
        task_id="resolve_window",
        python_callable=resolve_next_window
    )

    # 3. Generate Report
    analytics = PythonOperator(
        task_id="run_analytics",
        python_callable=run_analytics_wrapper
    )

    # 4. Email Report
    email = EmailOperator(
        task_id="send_email",
        to=RECIPIENT_EMAIL,
        subject="Movie Reports: {{ ti.xcom_pull(task_ids='resolve_window')['year_month'] }}",
        html_content="""
            <h3>MovieLens Analytics Report</h3>
            <p>Attached is the analytics report for the requested period.</p>
            <p><i>Generated by Airflow Historical Catchup</i></p>
        """,
        files="{{ ti.xcom_pull(task_ids='run_analytics') }}"
    )

    # 5. Save Progress
    save_cursor = PythonOperator(
        task_id="save_cursor",
        python_callable=update_cursor_task
    )

    # Execution Flow
    extract >> load >> transform >> resolve_window >> analytics >> email >> save_cursor