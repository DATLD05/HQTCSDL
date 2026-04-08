from pathlib import Path

ETL_DIR = Path(__file__).resolve().parent
DATA_DIR = ETL_DIR / "data"
# Default destination for cleaned CSV exports
CLEANED_DATA_DIR = ETL_DIR / "cleaned_data"

PATIENTS_CSV = DATA_DIR / "patients.csv"
PAYERS_CSV = DATA_DIR / "payers.csv"
PROVIDERS_CSV = DATA_DIR / "providers.csv"
ENCOUNTERS_CSV = DATA_DIR / "encounters.csv"
CONDITIONS_CSV = DATA_DIR / "conditions.csv"
OBSERVATIONS_CSV = DATA_DIR / "observations.csv"
PROCEDURES_CSV = DATA_DIR / "procedures.csv"
MEDICATIONS_CSV = DATA_DIR / "medications.csv"

# Cleaned outputs: single CSV per table, written under CLEANED_DATA_DIR by default
CLEANED_SUFFIX = "_cleaned"


def cleaned_csv_path(raw_path: Path) -> Path:
    """Return path like cleaned_data/patients_cleaned.csv from data/patients.csv."""
    stem = raw_path.stem
    return CLEANED_DATA_DIR / f"{stem}{CLEANED_SUFFIX}.csv"
