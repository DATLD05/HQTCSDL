from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.csv_utils import (
    collapse_whitespace,
    fix_stop_after_start,
    normalize_key_uuid,
    normalize_long_code,
    parse_ts_any,
    trim_empty_to_null,
)
from src.etl.transform.common import filter_by_patient_life_ts


def transform(
    procedures_raw: DataFrame,
    encounters: DataFrame,
    patients: DataFrame,
) -> DataFrame:
    enc = encounters.select(
        F.col("Id").alias("ENCOUNTER"),
        F.col("PATIENT").alias("_e_pat"),
        F.col("START").alias("_e_start"),
        F.col("STOP").alias("_e_stop"),
    ).distinct()

    string_cols = [
        "PATIENT",
        "ENCOUNTER",
        "DESCRIPTION",
        "REASONCODE",
        "REASONDESCRIPTION",
    ]
    df = trim_empty_to_null(procedures_raw, [c for c in string_cols if c in procedures_raw.columns])
    for c in ("PATIENT", "ENCOUNTER"):
        if c in df.columns:
            df = df.withColumn(c, normalize_key_uuid(c))
    df = df.filter(F.col("ENCOUNTER").isNotNull() & F.col("PATIENT").isNotNull())

    df = df.join(enc, on="ENCOUNTER", how="inner")
    df = df.filter(F.col("PATIENT") == F.col("_e_pat"))

    df = (
        df.withColumn("START", parse_ts_any(F.trim(F.col("START").cast("string"))))
        .withColumn("STOP", parse_ts_any(F.trim(F.col("STOP").cast("string"))))
    )
    s, e = fix_stop_after_start("START", "STOP")
    df = df.withColumn("START", s).withColumn("STOP", e)

    df = df.filter(
        F.col("START").isNotNull()
        & (F.col("START") >= F.col("_e_start"))
        & (F.col("START") <= F.col("_e_stop"))
    ).drop("_e_pat", "_e_start", "_e_stop")

    if "CODE" in df.columns:
        df = df.withColumn("CODE", normalize_long_code("CODE"))
    if "DESCRIPTION" in df.columns:
        df = df.withColumn(
            "DESCRIPTION",
            collapse_whitespace(F.col("DESCRIPTION").cast("string")),
        )
    if "REASONCODE" in df.columns:
        df = df.withColumn(
            "REASONCODE",
            F.when(
                F.trim(F.col("REASONCODE").cast("string")) == "",
                F.lit(None),
            ).otherwise(normalize_long_code("REASONCODE")),
        )
    if "REASONDESCRIPTION" in df.columns:
        df = df.withColumn(
            "REASONDESCRIPTION",
            collapse_whitespace(F.col("REASONDESCRIPTION").cast("string")),
        )
    if "BASE_COST" in df.columns:
        df = df.withColumn("BASE_COST", F.col("BASE_COST").cast("decimal(18,4)"))

    df = filter_by_patient_life_ts(df, patient_col="PATIENT", ts_col="START", patients=patients)

    return df.dropDuplicates(["ENCOUNTER", "CODE", "START", "STOP", "PATIENT"])
