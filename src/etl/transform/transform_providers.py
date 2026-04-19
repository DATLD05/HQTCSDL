from pyspark.sql import DataFrame

from src.etl.transform.common import base_providers_keys


def transform(providers_raw: DataFrame) -> DataFrame:
    return base_providers_keys(providers_raw)
