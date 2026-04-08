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
    medications_raw: DataFrame,
    encounters: DataFrame,
    patients: DataFrame,
    payers: DataFrame,
) -> DataFrame:
    enc = encounters.select(
        F.col("Id").alias("ENCOUNTER"),
        F.col("PATIENT").alias("_e_pat"),
        F.col("START").alias("_e_start"),
        F.col("STOP").alias("_e_stop"),
    ).distinct()

    payer_keys = payers.select(F.col("Id").alias("_valid_payer")).distinct()

    string_cols = [
        "PATIENT",
        "PAYER",
        "ENCOUNTER",
        "DESCRIPTION",
        "REASONCODE",
        "REASONDESCRIPTION",
    ]
    df = trim_empty_to_null(medications_raw, [c for c in string_cols if c in medications_raw.columns])
    for c in ("PATIENT", "PAYER", "ENCOUNTER"):
        if c in df.columns:
            df = df.withColumn(c, normalize_key_uuid(c))
    df = df.filter(
        F.col("ENCOUNTER").isNotNull()
        & F.col("PATIENT").isNotNull()
        & F.col("PAYER").isNotNull()
    )

    df = df.join(payer_keys, df["PAYER"] == payer_keys["_valid_payer"], "inner").drop("_valid_payer")

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

    for c in ("BASE_COST", "PAYER_COVERAGE", "TOTALCOST"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("decimal(18,4)"))
    if "DISPENSES" in df.columns:
        df = df.withColumn("DISPENSES", F.col("DISPENSES").cast("long"))

    df = filter_by_patient_life_ts(df, patient_col="PATIENT", ts_col="START", patients=patients)

    return df.dropDuplicates(
        ["ENCOUNTER", "CODE", "START", "PATIENT", "PAYER", "DISPENSES"]
    )
