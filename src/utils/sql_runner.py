import re
from pathlib import Path
from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.logging import get_logger

logger = get_logger(__name__)

def run_sql_file(file_path: str):
    """
    Executes an entire SQL file (splitting by ;)
    Used for creating tables (DDL).
    """
    path = Path(file_path)
    if not path.exists():
        logger.error(f"SQL file not found: {path}")
        return

    logger.info(f"Executing SQL file: {path.name}")
    with open(path, "r") as f:
        sql_content = f.read()

    engine = get_engine()
    with engine.begin() as conn:
        # Split by semicolon to handle multiple statements
        statements = sql_content.split(";")
        for statement in statements:
            if statement.strip():
                conn.execute(text(statement))

def get_query_by_name(sql_file_path: str, query_name: str) -> str:
    """
    Parses a SQL file containing '-- name: query_name' tags.
    Returns the raw SQL string for the requested query name.
    """
    path = Path(sql_file_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    with open(path, 'r') as f:
        content = f.read()

    # Regex explanation:
    # 1. Look for '-- name: <query_name>'
    # 2. Capture everything until the next '-- name:' or End of File
    pattern = rf"--\s*name:\s*{re.escape(query_name)}\s*\n(.*?)(?=\n--\s*name:|\Z)"
    
    match = re.search(pattern, content, re.DOTALL)
    
    if not match:
        raise ValueError(f"Query '{query_name}' not found in {sql_file_path}")
    
    # Return the clean SQL string
    return match.group(1).strip()