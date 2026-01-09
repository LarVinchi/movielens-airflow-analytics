from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

from src.extract import extract_movielens
from src.load import load_to_staging
from src.transform import run_clean_layer
from src.analytics.monthly_reports import run_monthly_reports

# Get email from environment variable (set in docker-compose)
RECIPIENT_EMAIL = os.getenv("AIRFLOW__SMTP__SMTP_USER") 

def resolve_window(**context):
    logical_date = context["data_interval_start"]
    return {
        "start": logical_date.strftime("%Y-%m-01"),
        "end": (logical_date + relativedelta(months=1)).strftime("%Y-%m-01")
    }

def run_analytics(**context):
    window = context["ti"].xcom_pull(task_ids="resolve_window")
    return run_monthly_reports(window["start"], window["end"])

with DAG(
    "monthly_movie_analytics", 
    start_date=datetime(2023, 1, 1), 
    schedule="@monthly", 
    catchup=False
) as dag:
    
    extract = PythonOperator(task_id="extract", python_callable=extract_movielens.main)
    load = PythonOperator(task_id="load", python_callable=load_to_staging.main)
    transform = PythonOperator(task_id="transform", python_callable=run_clean_layer.main)
    window = PythonOperator(task_id="resolve_window", python_callable=resolve_window)
    analytics = PythonOperator(task_id="analytics", python_callable=run_analytics)
    
    email = EmailOperator(
        task_id="send_email",
        to=RECIPIENT_EMAIL, 
        subject="Movie Reports: {{ ds }}",
        html_content="<h3>Reports Attached</h3><p>Here are your monthly movie analytics.</p>",
        files="{{ ti.xcom_pull(task_ids='analytics') }}"
    )

    extract >> load >> transform >> window >> analytics >> email