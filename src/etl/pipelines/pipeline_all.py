"""
End-to-end ETL: read 8 raw CSVs → clean with PySpark → 8 DataFrames + optional CSV writes.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.etl.csv_utils import write_single_csv
from src.etl.extract import RawCsvPaths, extract_all
from src.etl.paths import (
    CONDITIONS_CSV,
    ENCOUNTERS_CSV,
    MEDICATIONS_CSV,
    OBSERVATIONS_CSV,
    PATIENTS_CSV,
    PAYERS_CSV,
    PROCEDURES_CSV,
    PROVIDERS_CSV,
    cleaned_csv_path,
)
from src.etl.clean import clean_all
from src.etl.load import load_star_to_bigquery
from src.etl.transform import transform_star

_TABLE_ORDER = (
    "patients",
    "payers",
    "providers",
    "encounters",
    "conditions",
    "observations",
    "procedures",
    "medications",
)

_CSV_BY_KEY: dict[str, Path] = {
    "patients": PATIENTS_CSV,
    "payers": PAYERS_CSV,
    "providers": PROVIDERS_CSV,
    "encounters": ENCOUNTERS_CSV,
    "conditions": CONDITIONS_CSV,
    "observations": OBSERVATIONS_CSV,
    "procedures": PROCEDURES_CSV,
    "medications": MEDICATIONS_CSV,
}


def _output_path_for_table(key: str, output_dir: Path | None) -> Path:
    raw = _CSV_BY_KEY[key]
    if output_dir is None:
        return cleaned_csv_path(raw)
    return (Path(output_dir) / f"{raw.stem}_cleaned").with_suffix(".csv")


def run(
    spark: SparkSession,
    *,
    raw_paths: RawCsvPaths | None = None,
    write_csv: bool = False,
    output_dir: Path | None = None,
    write_star_csv: bool = True,
    star_output_dir: Path | None = None,
    load_to_cloud: bool = False,
    bq_project_id: str | None = None,
    bq_dataset_id: str | None = None,
    bq_credentials_path: str | None = None,
    bq_write_disposition: str = "WRITE_TRUNCATE",
) -> dict[str, DataFrame]:
    """
    Returns eight cleaned DataFrames keyed by table name.
    If write_csv is True, writes one CSV per table with suffix ``_cleaned.csv``.
    If write_star_csv is True, runs star transform and writes star CSV outputs.
    If load_to_cloud is True, loads star CSV outputs to BigQuery.
    """
    raw = extract_all(spark, raw_paths)
    cleaned = clean_all(raw)
    transform_star(cleaned, write_csv=write_star_csv, output_dir=star_output_dir)
    if load_to_cloud:
        load_star_to_bigquery(
            project_id=bq_project_id,
            dataset_id=bq_dataset_id,
            credentials_path=bq_credentials_path,
            star_dir=star_output_dir,
            write_disposition=bq_write_disposition,
        )

    if write_csv:
        for key in _TABLE_ORDER:
            write_single_csv(cleaned[key], _output_path_for_table(key, output_dir))
        missing = [
            str(_output_path_for_table(key, output_dir))
            for key in _TABLE_ORDER
            if not _output_path_for_table(key, output_dir).exists()
        ]
        if missing:
            raise RuntimeError(
                "Missing cleaned CSV outputs after write: " + ", ".join(missing)
            )

    return cleaned


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL_All").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(
            spark,
            write_csv=True,
            write_star_csv=True,
            load_to_cloud=True,
        )
    except Exception as exc:
        msg = str(exc).splitlines()[0]
        print(f"ETL pipeline failed: {exc.__class__.__name__}: {msg}")
    finally:
        spark.stop()

