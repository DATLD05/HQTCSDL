from pathlib import Path

ETL_DIR = Path(__file__).resolve().parent
DATA_DIR = ETL_DIR / "data"

CONDITIONS_CSV = DATA_DIR / "conditions.csv"
ENCOUNTERS_CSV = DATA_DIR / "encounters.csv"
OBSERVATIONS_CSV = DATA_DIR / "observations.csv"
PROCEDURES_CSV = DATA_DIR / "procedures.csv"
