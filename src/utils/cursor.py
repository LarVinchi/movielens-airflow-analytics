from airflow.models import Variable
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from src.utils.logging import get_logger

logger = get_logger(__name__)

VARIABLE_KEY = "movielens_report_cursor"

def get_next_window():
    """
    Determines the next month to process.
    1. If no cursor exists, finds the earliest date in the DB.
    2. If cursor exists, returns cursor + 1 month.
    """
    cursor_str = Variable.get(VARIABLE_KEY, default_var=None)
    
    if cursor_str:
        # We have a history. Move to next month.
        last_date = datetime.strptime(cursor_str, "%Y-%m")
        next_date = last_date + relativedelta(months=1)
        return next_date.strftime("%Y-%m")
    
    else:
        # First Run! We need to query the DB for the earliest data.
        # Note: We return a special flag or handle this in the DAG to query DB
        return "EARLIEST_DB"

def update_cursor(processed_month: str):
    """Save the month we just finished processing."""
    Variable.set(VARIABLE_KEY, processed_month)
    logger.info(f"Cursor updated. Next run will process month after: {processed_month}")