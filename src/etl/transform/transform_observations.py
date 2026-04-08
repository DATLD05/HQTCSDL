from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.csv_utils import collapse_whitespace, normalize_key_uuid, parse_ts_any, trim_empty_to_null
from src.etl.transform.common import filter_by_patient_life_ts


def transform(
    observations_raw: DataFrame,
    encounters: DataFrame,
    patients: DataFrame,
) -> DataFrame:
    enc = encounters.select(
        F.col("Id").alias("ENCOUNTER"),
        F.col("PATIENT").alias("_e_pat"),
        F.col("START").alias("_e_start"),
        F.col("STOP").alias("_e_stop"),
    ).distinct()

    string_cols = ["PATIENT", "ENCOUNTER", "CATEGORY", "CODE", "DESCRIPTION", "UNITS", "TYPE", "VALUE"]
    df = trim_empty_to_null(
        observations_raw,
        [c for c in string_cols if c in observations_raw.columns],
    )
    for c in ("PATIENT", "ENCOUNTER"):
        if c in df.columns:
            df = df.withColumn(c, normalize_key_uuid(c))
    df = df.filter(F.col("ENCOUNTER").isNotNull() & F.col("PATIENT").isNotNull())

    df = df.join(enc, on="ENCOUNTER", how="inner")
    df = df.filter(F.col("PATIENT") == F.col("_e_pat"))

    df = df.withColumn("DATE", parse_ts_any(F.trim(F.col("DATE").cast("string"))))
    df = df.filter(
        F.col("DATE").isNotNull()
        & (F.col("DATE") >= F.col("_e_start"))
        & (F.col("DATE") <= F.col("_e_stop"))
    ).drop("_e_pat", "_e_start", "_e_stop")

    if "CATEGORY" in df.columns:
        df = df.withColumn(
            "CATEGORY",
            F.lower(F.trim(F.col("CATEGORY").cast("string"))),
        )
    if "TYPE" in df.columns:
        df = df.withColumn(
            "TYPE",
            F.lower(F.trim(F.col("TYPE").cast("string"))),
        )
    if "CODE" in df.columns:
        df = df.withColumn("CODE", F.trim(F.col("CODE").cast("string")))
    if "DESCRIPTION" in df.columns:
        df = df.withColumn(
            "DESCRIPTION",
            collapse_whitespace(F.col("DESCRIPTION").cast("string")),
        )
    if "UNITS" in df.columns:
        df = df.withColumn("UNITS", F.lower(F.trim(F.col("UNITS").cast("string"))))

    df = df.withColumn(
        "VALUE_NUMERIC",
        F.when(F.col("TYPE") == "numeric", F.col("VALUE").cast("double")).otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "VALUE_TEXT",
        F.when(F.col("TYPE") == "text", F.col("VALUE").cast("string")).otherwise(F.lit(None)),
    )

    df = filter_by_patient_life_ts(df, patient_col="PATIENT", ts_col="DATE", patients=patients)

    return df.dropDuplicates(
        ["DATE", "ENCOUNTER", "CODE", "DESCRIPTION", "CATEGORY", "PATIENT", "TYPE"]
    )
