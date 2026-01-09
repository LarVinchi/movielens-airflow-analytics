import pandas as pd
from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.sql import load_named_queries
from src.config.settings import SQL_DIR, REPORTS_DIR

def run_monthly_reports(start, end):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    queries = load_named_queries(str(SQL_DIR / "analytics.sql"))
    engine = get_engine()
    
    output_files = []
    with engine.connect() as conn:
        for name, sql in queries.items():
            df = pd.DataFrame(conn.execute(text(sql), {"start": start, "end": end}).fetchall())
            path = REPORTS_DIR / f"{name}_{start}.csv"
            df.to_csv(path, index=False)
            output_files.append(str(path))
    return output_files