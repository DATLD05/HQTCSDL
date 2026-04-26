from pyspark.sql import DataFrame

from src.etl.clean.common import base_patient_keys


def clean(patients_raw: DataFrame) -> DataFrame:
    return base_patient_keys(patients_raw)

