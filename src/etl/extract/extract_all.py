from pathlib import Path
from typing_extensions import TypedDict

from pyspark.sql import DataFrame, SparkSession

from src.etl.extract.extract_csv import extract_csv
from src.etl.extract.extract_db import extract_db
from src.etl.paths import (
    PATIENTS_CSV,
    PAYERS_CSV,
    PROVIDERS_CSV,
    ENCOUNTERS_CSV,
    CONDITIONS_CSV,
    OBSERVATIONS_CSV,
    PROCEDURES_CSV,
    MEDICATIONS_CSV,
)


class RawCsvPaths(TypedDict, total=False):
    patients: Path | None
    payers: Path | None
    providers: Path | None
    encounters: Path | None
    conditions: Path | None
    observations: Path | None
    procedures: Path | None
    medications: Path | None


CSV_FILES = {
    "patients": PATIENTS_CSV,
    "payers": PAYERS_CSV,
    "providers": PROVIDERS_CSV,
    "encounters": ENCOUNTERS_CSV,
    "conditions": CONDITIONS_CSV,
    "observations": OBSERVATIONS_CSV,
    "procedures": PROCEDURES_CSV,
    "medications": MEDICATIONS_CSV,
}


def extract_all_csv(
    spark: SparkSession,
    paths: RawCsvPaths | None = None,
) -> dict[str, DataFrame]:
    p = paths or {}

    return {
        name: extract_csv(
            spark,
            default_path,
            p.get(name),
        )
        for name, default_path in CSV_FILES.items()
    }


def extract_all_db(
    spark: SparkSession,
) -> dict[str, DataFrame]:
    return {
        table: extract_db(spark, table)
        for table in CSV_FILES.keys()
    }