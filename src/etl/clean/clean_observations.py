from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.csv_utils import (
    normalize_category_mapped,
    normalize_code_canonical,
    normalize_key_uuid,
    normalize_unit_symbol,
    parse_ts_any,
    sanitize_text,
    trim_empty_to_null,
)
from src.etl.clean.common import filter_by_patient_life_ts


def clean(
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
            normalize_category_mapped(
                "CATEGORY",
                {
                    "vital-signs": "vital-signs",
                    "vital signs": "vital-signs",
                    "laboratory": "laboratory",
                    "exam": "exam",
                    "survey": "survey",
                    "social-history": "social-history",
                    "social history": "social-history",
                },
            ),
        )
    if "TYPE" in df.columns:
        df = df.withColumn(
            "TYPE",
            normalize_category_mapped(
                "TYPE",
                {
                    "numeric": "numeric",
                    "number": "numeric",
                    "text": "text",
                    "string": "text",
                },
            ),
        )
    if "CODE" in df.columns:
        df = df.withColumn("CODE", normalize_code_canonical("CODE"))
    if "DESCRIPTION" in df.columns:
        df = df.withColumn("DESCRIPTION", sanitize_text("DESCRIPTION"))
    if "UNITS" in df.columns:
        df = df.withColumn("UNITS", normalize_unit_symbol("UNITS"))

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

