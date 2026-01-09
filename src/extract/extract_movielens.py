import zipfile
import requests
from src.config.settings import RAW_DIR
from src.utils.logging import get_logger

logger = get_logger(__name__)
MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
ZIP_PATH = RAW_DIR / "ml-32m.zip"

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if not ZIP_PATH.exists():
        logger.info("Downloading...")
        r = requests.get(MOVIELENS_URL, stream=True)
        with open(ZIP_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        for member in z.namelist():
            if member.endswith(("movies.csv", "ratings.csv")):
                filename = member.split('/')[-1]
                target = RAW_DIR / filename
                if not target.exists():
                    with open(target, "wb") as f:
                        f.write(z.read(member))
                    logger.info(f"Extracted {filename}")

if __name__ == "__main__":
    main()