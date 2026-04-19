from pyspark.sql import DataFrame

from src.etl.transform.common import base_patient_keys


def transform(patients_raw: DataFrame) -> DataFrame:
    return base_patient_keys(patients_raw)
