from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.csv_utils import (
    collapse_whitespace,
    normalize_key_uuid,
    normalize_long_code,
    parse_date_any,
    trim_empty_to_null,
)
from src.etl.transform.common import filter_by_patient_life_date


def transform(
    conditions_raw: DataFrame,
    encounters: DataFrame,
    patients: DataFrame,
) -> DataFrame:
    enc = encounters.select(
        F.col("Id").alias("ENCOUNTER"),
        F.col("PATIENT").alias("_e_pat"),
        F.col("START").alias("_e_start"),
        F.col("STOP").alias("_e_stop"),
    ).distinct()

    string_cols = ["PATIENT", "ENCOUNTER", "DESCRIPTION"]
    df = trim_empty_to_null(conditions_raw, [c for c in string_cols if c in conditions_raw.columns])
    for c in ("PATIENT", "ENCOUNTER"):
        if c in df.columns:
            df = df.withColumn(c, normalize_key_uuid(c))
    df = df.filter(F.col("ENCOUNTER").isNotNull() & F.col("PATIENT").isNotNull())

    df = df.join(enc, on="ENCOUNTER", how="inner")
    df = df.filter(F.col("PATIENT") == F.col("_e_pat"))

    df = (
        df.withColumn("START", parse_date_any(F.col("START").cast("string")))
        .withColumn(
            "STOP",
            F.when(
                F.col("STOP").isNull()
                | (F.trim(F.col("STOP").cast("string")) == ""),
                F.lit(None),
            ).otherwise(parse_date_any(F.col("STOP").cast("string"))),
        )
    )
    df = df.withColumn(
        "STOP",
        F.when(
            F.col("STOP").isNotNull()
            & F.col("START").isNotNull()
            & (F.col("STOP") < F.col("START")),
            F.lit(None),
        ).otherwise(F.col("STOP")),
    )

    enc_start = F.to_date(F.col("_e_start"))
    enc_stop = F.to_date(F.col("_e_stop"))
    cond_start = F.col("START")
    in_enc_window = (
        cond_start.isNotNull()
        & enc_start.isNotNull()
        & (cond_start >= enc_start)
        & (cond_start <= enc_stop)
    )
    df = df.filter(in_enc_window).drop("_e_pat", "_e_start", "_e_stop")

    if "CODE" in df.columns:
        df = df.withColumn("CODE", normalize_long_code("CODE"))
    if "DESCRIPTION" in df.columns:
        df = df.withColumn(
            "DESCRIPTION",
            collapse_whitespace(F.col("DESCRIPTION").cast("string")),
        )

    df = filter_by_patient_life_date(df, patient_col="PATIENT", date_col="START", patients=patients)

    return df.dropDuplicates(["ENCOUNTER", "CODE", "START", "PATIENT"])
