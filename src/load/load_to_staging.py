import pandas as pd
from src.config.settings import RAW_DIR, SQL_DIR
from src.utils.db import get_engine
from src.utils.sql_runner import run_sql_file

def main():
    run_sql_file(str(SQL_DIR / "staging.sql"))
    engine = get_engine()
    
    # Load Movies
    pd.read_csv(RAW_DIR / "movies.csv").rename(
        columns={"movieId": "movie_id", "userId": "user_id"}
    ).to_sql("movies", engine.connect(), schema="staging", if_exists="replace", index=False)
    
    # Load Ratings (COPY)
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            with open(RAW_DIR / "ratings.csv", "r") as f:
                next(f)
                cur.copy_expert("COPY staging.ratings FROM STDIN WITH CSV", f)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()