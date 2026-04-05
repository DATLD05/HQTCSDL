from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.etl.extract import extract_observations
from src.etl.transform import transform_observations

from src.etl.pipelines.pipeline_encounters import run as run_encounters


def run(
    spark: SparkSession,
    encounters_transformed: DataFrame | None = None,
    observations_csv: Path | None = None,
    encounters_csv: Path | None = None,
) -> DataFrame:
    enc = (
        encounters_transformed
        if encounters_transformed is not None
        else run_encounters(spark, encounters_csv)
    )
    raw = extract_observations(spark, observations_csv)
    return transform_observations(raw, enc)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    df = run(spark)

    df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("src/etl/data/observations_transformed")

    spark.stop()