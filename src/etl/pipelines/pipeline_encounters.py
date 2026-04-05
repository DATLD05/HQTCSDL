from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.etl.extract import extract_encounters
from src.etl.transform import transform_encounters


def run(
    spark: SparkSession, encounters_csv: Path | None = None
) -> DataFrame:
    raw = extract_encounters(spark, encounters_csv)
    return transform_encounters(raw)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    df = run(spark)

    df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("src/etl/data/encounters_transformed")

    spark.stop()