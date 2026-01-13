from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.models.param import Param 
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import sys

# --- Imports ---
sys.path.append("/opt/airflow")
from src.analytics.monthly_reports import run_monthly_reports
from src.utils.db import get_engine
from src.utils.logging import get_logger
from src.utils.cursor import VARIABLE_KEY, set_cursor

logger = get_logger(__name__)
RECIPIENT_EMAIL = os.getenv("AIRFLOW__SMTP__SMTP_USER") 

# --- Logic: Resolve Window (Preserved Exactly as you had it) ---
def get_earliest_date_from_db():
    """Query the database to find the very first rating date."""
    engine = get_engine()
    # We query the fact table now, as it's the source of truth for reports
    query = "SELECT MIN(rating_time) FROM fact.fact_ratings"
    
    with engine.connect() as conn:
        result = conn.execute(query).scalar()
        if not result:
            logger.warning("No data found in DB. Defaulting to 1995-01.")
            return datetime(1995, 1, 1)
        return result

def resolve_next_window(**context):
    """
    Decides which time window to process.
    - If 'start_date' param is provided: Runs a Manual range.
    - If no params: Runs the next month based on the database cursor.
    """
    params = context["params"]
    manual_start = params.get("start_date")
    manual_end = params.get("end_date")

    # --- 1. Manual Run Logic ---
    if manual_start:
        logger.info(f"MANUAL RUN DETECTED: Start={manual_start}, End={manual_end}")
        
        # Parse Start Date
        # Try YYYY-MM-DD first, fall back to YYYY-MM
        try:
            start_dt = datetime.strptime(manual_start, "%Y-%m-%d")
        except ValueError:
            start_dt = datetime.strptime(manual_start, "%Y-%m")

        # Parse End Date
        if manual_end:
            try:
                end_dt = datetime.strptime(manual_end, "%Y-%m-%d")
            except ValueError:
                end_dt = datetime.strptime(manual_end, "%Y-%m")
        else:
            # Default behavior: If no end date, do exactly 1 month
            end_dt = start_dt + relativedelta(months=1)

        return {
            "year_month": manual_start, # Label for email subject
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d"),
            "mode": "manual"
        }

    # --- 2. Automatic Cursor Logic (Existing) ---
    cursor_str = Variable.get(VARIABLE_KEY, default_var=None)
    target_date = None
    
    if not cursor_str:
        # First Run Ever: Check DB for earliest data
        logger.info("No cursor found. Querying DB for start date...")
        earliest = get_earliest_date_from_db()
        
        # Strip Timezone if present
        if earliest.tzinfo is not None:
            earliest = earliest.replace(tzinfo=None)
            
        target_date = earliest.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        logger.info(f"Earliest date found: {target_date.strftime('%Y-%m')}")
    else:
        # Continue from last saved point
        last_processed = datetime.strptime(cursor_str, "%Y-%m")
        target_date = last_processed + relativedelta(months=1)
        logger.info(f"Cursor found ({cursor_str}). Next target: {target_date.strftime('%Y-%m')}")

    # --- 3. Future Guardrail ---
    # Don't let the auto-runner process the current unfinished month
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
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    if window.get("mode") == "manual":
        return
    
    processed_month = window.get("year_month")
    current_month = datetime.utcnow().strftime("%Y-%m")
    
    if processed_month < current_month:
        set_cursor(processed_month)

def run_analytics_wrapper(**context):
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    # This calls your function that uses the SQL file
    return run_monthly_reports(window["start"], window["end"])

# --- DAG Definition ---
with DAG(
    "daily_historical_reporter",
    start_date=datetime(2023, 1, 1),
    schedule="0 12 * * *", 
    catchup=False,
    render_template_as_native_obj=True,
    tags=["movielens", "reporting"],
    params={
        "start_date": Param(
            default=None, 
            type=["null", "string"], 
            description="Format: YYYY-MM. Leave empty for automatic daily catchup."
        )
    }
) as dag:

    # 1. Decide Date (The Brains)
    resolve_window = PythonOperator(
        task_id="resolve_window",
        python_callable=resolve_next_window
    )

    # 2. Generate Report (The Action)
    # Note: No Extract/Load/Transform here! 
    # It assumes 'movie_ingestion_pipeline' has already populated the DB.
    analytics = PythonOperator(
        task_id="run_analytics",
        python_callable=run_analytics_wrapper
    )

    # 3. Email Report
    email = EmailOperator(
        task_id="send_email",
        to=RECIPIENT_EMAIL,
        subject="Movie Reports: {{ ti.xcom_pull(task_ids='resolve_window')['year_month'] }}",
        html_content="""
            <h3>MovieLens Analytics Report</h3>
            <p>Attached is the analytics report for the requested period.</p>
        """,
        files="{{ ti.xcom_pull(task_ids='run_analytics') }}",
        conn_id="smtp_default" # Uses our fixed SSL connection
    )

    # 4. Save Progress
    save_cursor = PythonOperator(
        task_id="save_cursor",
        python_callable=update_cursor_task
    )

    # Execution Flow
    resolve_window >> analytics >> email >> save_cursor