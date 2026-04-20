"""
End-to-end ETL: read 8 raw CSVs → clean with PySpark → 8 DataFrames + optional CSV writes.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.etl.csv_utils import write_single_csv
from src.etl.extract import RawCsvPaths, extract_all
from src.etl.integrity_audit import build_integrity_report, has_integrity_violations
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
from src.etl.transform import transform_all, transform_star

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
    write_star_csv: bool = False,
    star_output_dir: Path | None = None,
    fail_on_integrity_error: bool = False,
) -> dict[str, DataFrame]:
    """
    Returns eight cleaned DataFrames keyed by table name.
    If write_csv is True, writes one CSV per table with suffix ``_cleaned.csv``.
    If write_star_csv is True, writes 4 star-schema CSV tables with suffix ``_star.csv``.
    """
    raw = extract_all(spark, raw_paths)
    cleaned = transform_all(raw)
    report = build_integrity_report(raw, cleaned)
    print("=== Integrity Report ===")
    for key in sorted(report.keys()):
        print(f"{key}: {report[key]}")
    if fail_on_integrity_error and has_integrity_violations(report):
        raise RuntimeError("PK/FK integrity violations detected. See Integrity Report above.")

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

    if write_star_csv:
        transform_star(cleaned, write_csv=True, output_dir=star_output_dir)

    return cleaned


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL_All").getOrCreate()
    try:
        run(spark, write_csv=True)
    finally:
        spark.stop()
