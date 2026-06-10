# src/etl/extract_db.py

from pyspark.sql import DataFrame, SparkSession

from credentials.db.db_config import (
    DB_URL,
    DB_USER,
    DB_PASSWORD,
    DB_DRIVER,
)


def extract_db(
    spark: SparkSession,
    table: str,
) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", DB_DRIVER)
        .load()
    )