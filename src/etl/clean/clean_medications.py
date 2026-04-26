from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.csv_utils import (
    fix_stop_after_start,
    normalize_code_canonical,
    normalize_key_uuid,
    normalize_money_decimal,
    parse_ts_any,
    sanitize_text,
    trim_empty_to_null,
)
from src.etl.clean.common import filter_by_patient_life_ts


def clean(
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
        df = df.withColumn("CODE", normalize_code_canonical("CODE"))
    if "DESCRIPTION" in df.columns:
        df = df.withColumn("DESCRIPTION", sanitize_text("DESCRIPTION"))
    if "REASONCODE" in df.columns:
        df = df.withColumn(
            "REASONCODE",
            F.when(
                F.trim(F.col("REASONCODE").cast("string")) == "",
                F.lit(None),
            ).otherwise(normalize_code_canonical("REASONCODE")),
        )
    if "REASONDESCRIPTION" in df.columns:
        df = df.withColumn("REASONDESCRIPTION", sanitize_text("REASONDESCRIPTION"))

    for c in ("BASE_COST", "PAYER_COVERAGE", "TOTALCOST"):
        if c in df.columns:
            df = df.withColumn(c, normalize_money_decimal(c))
    if "DISPENSES" in df.columns:
        df = df.withColumn("DISPENSES", F.col("DISPENSES").cast("long"))

    df = filter_by_patient_life_ts(df, patient_col="PATIENT", ts_col="START", patients=patients)

    return df.dropDuplicates(
        ["ENCOUNTER", "CODE", "START", "PATIENT", "PAYER", "DISPENSES"]
    )

