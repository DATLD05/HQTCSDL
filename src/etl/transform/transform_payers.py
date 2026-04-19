from pyspark.sql import DataFrame

from src.etl.transform.common import base_payers_keys


def transform(payers_raw: DataFrame) -> DataFrame:
    return base_payers_keys(payers_raw)
