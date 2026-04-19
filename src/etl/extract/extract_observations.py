from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.etl.csv_utils import read_csv
from src.etl.paths import OBSERVATIONS_CSV


def extract(spark: SparkSession, csv_path: Path | None = None) -> DataFrame:
    return read_csv(spark, csv_path or OBSERVATIONS_CSV)
