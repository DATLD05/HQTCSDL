

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.etl.csv_utils import (
    fix_stop_after_start,
    normalize_category_mapped,
    normalize_code_canonical,
    normalize_key_uuid,
    normalize_money_decimal,
    parse_ts_any,
    sanitize_text,
    trim_empty_to_null,
)
from src.etl.clean.common import filter_by_patient_life_ts


def clean(
    encounters_raw: DataFrame,
    patients: DataFrame,
    payers: DataFrame,
    providers: DataFrame,
) -> DataFrame:
    string_cols = [
        "Id",
        "PATIENT",
        "ORGANIZATION",
        "PROVIDER",
        "PAYER",
        "ENCOUNTERCLASS",
        "CODE",
        "DESCRIPTION",
        "REASONCODE",
        "REASONDESCRIPTION",
    ]
    df = trim_empty_to_null(encounters_raw, [c for c in string_cols if c in encounters_raw.columns])
    df = df.withColumn("Id", normalize_key_uuid("Id"))
    for c in ("PATIENT", "ORGANIZATION", "PROVIDER", "PAYER"):
        if c in df.columns:
            df = df.withColumn(c, normalize_key_uuid(c))
    df = df.filter(F.col("Id").isNotNull() & F.col("PATIENT").isNotNull())

    pid = patients.select(F.col("Id").alias("_valid_patient")).distinct()
    pr = providers.select(F.col("Id").alias("_valid_provider")).distinct()
    py = payers.select(F.col("Id").alias("_valid_payer")).distinct()
    df = df.join(pid, df["PATIENT"] == pid["_valid_patient"], "inner").drop("_valid_patient")
    df = df.join(pr, df["PROVIDER"] == pr["_valid_provider"], "inner").drop("_valid_provider")
    df = df.join(py, df["PAYER"] == py["_valid_payer"], "inner").drop("_valid_payer")

    df = (
        df.withColumn("_start_raw", F.trim(F.col("START").cast("string")))
        .withColumn("_stop_raw", F.trim(F.col("STOP").cast("string")))
        .withColumn("START", parse_ts_any(F.col("_start_raw")))
        .withColumn("STOP", parse_ts_any(F.col("_stop_raw")))
        .drop("_start_raw", "_stop_raw")
    )
    s, e = fix_stop_after_start("START", "STOP")
    df = df.withColumn("START", s).withColumn("STOP", e)

    if "ENCOUNTERCLASS" in df.columns:
        df = df.withColumn(
            "ENCOUNTERCLASS",
            normalize_category_mapped(
                "ENCOUNTERCLASS",
                {
                    "wellness": "wellness",
                    "inpatient": "inpatient",
                    "ambulatory": "ambulatory",
                    "outpatient": "outpatient",
                    "emergency": "emergency",
                    "urgentcare": "urgentcare",
                    "urgent_care": "urgentcare",
                },
            ),
        )
    if "CODE" in df.columns:
        df = df.withColumn("CODE", normalize_code_canonical("CODE"))
    if "DESCRIPTION" in df.columns:
        df = df.withColumn("DESCRIPTION", sanitize_text("DESCRIPTION"))
    if "REASONDESCRIPTION" in df.columns:
        df = df.withColumn("REASONDESCRIPTION", sanitize_text("REASONDESCRIPTION"))
    if "REASONCODE" in df.columns:
        df = df.withColumn(
            "REASONCODE",
            F.when(
                F.trim(F.col("REASONCODE").cast("string")) == "",
                F.lit(None),
            ).otherwise(normalize_code_canonical("REASONCODE")),
        )

    for c in ("BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE"):
        if c in df.columns:
            df = df.withColumn(c, normalize_money_decimal(c))

    df = filter_by_patient_life_ts(df, patient_col="PATIENT", ts_col="START", patients=patients)

    # Remove non-essential organization reference from cleaned output
    if "ORGANIZATION" in df.columns:
        df = df.drop("ORGANIZATION")

    w = Window.partitionBy("Id").orderBy(
        F.col("START").asc_nulls_last(),
        F.col("STOP").asc_nulls_last(),
        F.col("TOTAL_CLAIM_COST").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

