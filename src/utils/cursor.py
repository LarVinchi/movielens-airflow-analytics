from airflow.models import Variable
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.utils.logging import get_logger

logger = get_logger(__name__)

# The unique key used in Airflow Variables to store our progress
VARIABLE_KEY = "movielens_report_cursor"

def get_next_window_logic():
    """
    Helper function to inspect the cursor without modifying it.
    Returns the next target date (YYYY-MM) or None if no cursor exists.
    """
    cursor_str = Variable.get(VARIABLE_KEY, default_var=None)
    if cursor_str:
        last_date = datetime.strptime(cursor_str, "%Y-%m")
        next_date = last_date + relativedelta(months=1)
        return next_date
    return None

def set_cursor(new_month: str):
    """
    Updates the persistent cursor to the new month.
    """
    Variable.set(VARIABLE_KEY, new_month)
    logger.info(f"Cursor successfully updated to: {new_month}")