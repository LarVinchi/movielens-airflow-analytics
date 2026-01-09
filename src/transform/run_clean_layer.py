from src.utils.sql_runner import run_sql_file
from src.config.settings import SQL_DIR

def main():
    for f in ["clean.sql", "clean_movies.sql", "clean_ratings.sql", "dimensions.sql", "facts.sql"]:
        run_sql_file(str(SQL_DIR / f))

if __name__ == "__main__":
    main()