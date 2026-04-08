from pathlib import Path
from typing import TypedDict

from pyspark.sql import DataFrame, SparkSession

from src.etl.extract.extract_conditions import extract as extract_conditions
from src.etl.extract.extract_encounters import extract as extract_encounters
from src.etl.extract.extract_medications import extract as extract_medications
from src.etl.extract.extract_observations import extract as extract_observations
from src.etl.extract.extract_patients import extract as extract_patients
from src.etl.extract.extract_payers import extract as extract_payers
from src.etl.extract.extract_providers import extract as extract_providers
from src.etl.extract.extract_procedures import extract as extract_procedures


class RawCsvPaths(TypedDict, total=False):
    patients: Path | None
    payers: Path | None
    providers: Path | None
    encounters: Path | None
    conditions: Path | None
    observations: Path | None
    procedures: Path | None
    medications: Path | None


def extract_all(
    spark: SparkSession,
    paths: RawCsvPaths | None = None,
) -> dict[str, DataFrame]:
    """Load all eight source CSVs as raw DataFrames (column names trimmed)."""
    p = paths or {}
    return {
        "patients": extract_patients(spark, p.get("patients")),
        "payers": extract_payers(spark, p.get("payers")),
        "providers": extract_providers(spark, p.get("providers")),
        "encounters": extract_encounters(spark, p.get("encounters")),
        "conditions": extract_conditions(spark, p.get("conditions")),
        "observations": extract_observations(spark, p.get("observations")),
        "procedures": extract_procedures(spark, p.get("procedures")),
        "medications": extract_medications(spark, p.get("medications")),
    }
