from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.etl.csv_utils import read_csv


def extract_csv(
    spark: SparkSession,
    default_path: Path,
    csv_path: Path | None = None,
) -> DataFrame:
    return read_csv(spark, csv_path or default_path)