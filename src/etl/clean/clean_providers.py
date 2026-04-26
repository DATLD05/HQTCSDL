from pyspark.sql import DataFrame

from src.etl.clean.common import base_providers_keys


def clean(providers_raw: DataFrame) -> DataFrame:
    return base_providers_keys(providers_raw)

