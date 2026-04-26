from pyspark.sql import DataFrame

from src.etl.clean.common import base_payers_keys


def clean(payers_raw: DataFrame) -> DataFrame:
    return base_payers_keys(payers_raw)

