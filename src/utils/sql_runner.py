from pathlib import Path
from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.logging import get_logger

logger = get_logger(__name__)

def run_sql_file(file_path: str):
    path = Path(file_path)
    if not path.exists():
        logger.error(f"SQL file not found: {path}")
        return

    logger.info(f"Executing SQL file: {path.name}")
    with open(path, "r") as f:
        sql_content = f.read()

    engine = get_engine()
    with engine.begin() as conn:
        statements = sql_content.split(";")
        for statement in statements:
            if statement.strip():
                conn.execute(text(statement))