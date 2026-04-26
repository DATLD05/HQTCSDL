from pathlib import Path

ETL_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = ETL_DIR / "data" / "raw"
# Default destination for cleaned CSV exports
CLEANED_DATA_DIR = ETL_DIR / "data" / "clean"

PATIENTS_CSV = RAW_DATA_DIR / "patients.csv"
PAYERS_CSV = RAW_DATA_DIR / "payers.csv"
PROVIDERS_CSV = RAW_DATA_DIR / "providers.csv"
ENCOUNTERS_CSV = RAW_DATA_DIR / "encounters.csv"
CONDITIONS_CSV = RAW_DATA_DIR / "conditions.csv"
OBSERVATIONS_CSV = RAW_DATA_DIR / "observations.csv"
PROCEDURES_CSV = RAW_DATA_DIR / "procedures.csv"
MEDICATIONS_CSV = RAW_DATA_DIR / "medications.csv"

# Cleaned outputs: single CSV per table, written under CLEANED_DATA_DIR by default
CLEANED_SUFFIX = "_cleaned"
STAR_DATA_DIR = ETL_DIR / "data" / "star"


def cleaned_csv_path(raw_path: Path) -> Path:
    """Return path like data/clean/patients_cleaned.csv from data/raw/patients.csv."""
    stem = raw_path.stem
    return CLEANED_DATA_DIR / f"{stem}{CLEANED_SUFFIX}.csv"
