from sqlalchemy import create_engine
from src.config.settings import DB_CONNECTION_STRING

def get_engine():
    return create_engine(DB_CONNECTION_STRING)