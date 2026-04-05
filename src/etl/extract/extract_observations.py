from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.etl.csv_utils import normalize_column_names
from src.etl.paths import OBSERVATIONS_CSV


def extract(spark: SparkSession, csv_path: Path | None = None) -> DataFrame:
    path = str(csv_path or OBSERVATIONS_CSV)
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    return normalize_column_names(df)
