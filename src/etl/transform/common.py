"""Shared transform helpers: patient lifecycle, encounter windows, typing."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.etl.csv_utils import collapse_whitespace, normalize_key_uuid, trim_empty_to_null


def base_patient_keys(patients: DataFrame) -> DataFrame:
    """Id as normalized key + birth/death dates for constraints."""
    cols = [
        "Id",
        "BIRTHDATE",
        "DEATHDATE",
        "SSN",
        "DRIVERS",
        "PASSPORT",
        "PREFIX",
        "FIRST",
        "LAST",
        "SUFFIX",
        "MAIDEN",
        "MARITAL",
        "RACE",
        "ETHNICITY",
        "GENDER",
        "BIRTHPLACE",
        "ADDRESS",
        "CITY",
        "STATE",
        "COUNTY",
        "ZIP",
        "LAT",
        "LON",
        "HEALTHCARE_EXPENSES",
        "HEALTHCARE_COVERAGE",
    ]
    present = [c for c in cols if c in patients.columns]
    df = patients.select(*[F.col(c) for c in present])
    df = trim_empty_to_null(
        df,
        [
            c
            for c in [
                "Id",
                "SSN",
                "DRIVERS",
                "PASSPORT",
                "PREFIX",
                "FIRST",
                "LAST",
                "SUFFIX",
                "MAIDEN",
                "MARITAL",
                "RACE",
                "ETHNICITY",
                "GENDER",
                "BIRTHPLACE",
                "ADDRESS",
                "CITY",
                "STATE",
                "COUNTY",
                "ZIP",
            ]
            if c in df.columns
        ],
    )
    df = df.withColumn("Id", normalize_key_uuid("Id"))
    df = df.filter(F.col("Id").isNotNull())
    if "BIRTHDATE" in df.columns:
        df = df.withColumn("BIRTHDATE", F.to_date(F.col("BIRTHDATE").cast("string")))
    if "DEATHDATE" in df.columns:
        df = df.withColumn(
            "DEATHDATE",
            F.when(
                F.trim(F.col("DEATHDATE").cast("string")) == "",
                F.lit(None),
            ).otherwise(F.to_date(F.col("DEATHDATE").cast("string"))),
        )
    # Lifecycle: if death before birth, clear death (data repair)
    if "BIRTHDATE" in df.columns and "DEATHDATE" in df.columns:
        df = df.withColumn(
            "DEATHDATE",
            F.when(
                F.col("DEATHDATE").isNotNull()
                & F.col("BIRTHDATE").isNotNull()
                & (F.col("DEATHDATE") < F.col("BIRTHDATE")),
                F.lit(None),
            ).otherwise(F.col("DEATHDATE")),
        )
    # Costs: preserve 0 as meaningful
    for money in ("HEALTHCARE_EXPENSES", "HEALTHCARE_COVERAGE"):
        if money in df.columns:
            df = df.withColumn(money, F.col(money).cast("decimal(18,4)"))
    # Text cleanup: collapse whitespace; title-like fields
    for text in ("FIRST", "LAST", "MAIDEN", "ADDRESS", "CITY", "BIRTHPLACE"):
        if text in df.columns:
            df = df.withColumn(text, collapse_whitespace(F.col(text).cast("string")))
    for cat in ("RACE", "ETHNICITY", "MARITAL"):
        if cat in df.columns:
            df = df.withColumn(
                cat,
                F.initcap(F.lower(collapse_whitespace(F.col(cat).cast("string")))),
            )
    if "GENDER" in df.columns:
        df = df.withColumn(
            "GENDER",
            F.upper(F.substring(F.trim(F.col("GENDER").cast("string")), 1, 1)),
        )
    if "PREFIX" in df.columns:
        df = df.withColumn(
            "PREFIX",
            F.initcap(F.lower(F.trim(F.col("PREFIX").cast("string")))),
        )
    # Build normalised full name from first/last
    if "FIRST" in df.columns and "LAST" in df.columns:
        df = df.withColumn(
            "NAME",
            F.initcap(
                F.lower(
                    collapse_whitespace(
                        F.concat_ws(
                            " ",
                            F.col("FIRST").cast("string"),
                            F.col("LAST").cast("string"),
                        )
                    )
                )
            ),
        )
    if "STATE" in df.columns:
        # Patient addresses use full state names (e.g. Massachusetts)
        df = df.withColumn(
            "STATE",
            F.initcap(F.lower(collapse_whitespace(F.col("STATE").cast("string")))),
        )
    if "ZIP" in df.columns:
        df = df.withColumn("ZIP", F.trim(F.col("ZIP").cast("string")))
    if "LAT" in df.columns:
        df = df.withColumn("LAT", F.col("LAT").cast("double"))
    if "LON" in df.columns:
        df = df.withColumn("LON", F.col("LON").cast("double"))

    w = Window.partitionBy("Id").orderBy(F.col("BIRTHDATE").asc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # Drop non-essential / quasi-identifying columns per requirement
    drop_cols = [
        "SSN",
        "DRIVERS",
        "PASSPORT",
        "PREFIX",
        "FIRST",
        "LAST",
        "SUFFIX",
        "MAIDEN",
        "BIRTHPLACE",
        "ADDRESS",
        "CITY",
        "STATE",
        "COUNTY",
        "ZIP",
        "LAT",
        "LON",
    ]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)
    return df


def base_payers_keys(payers: DataFrame) -> DataFrame:
    df = payers
    df = trim_empty_to_null(df, [c for c in ["Id", "NAME", "PHONE"] if c in df.columns])
    df = df.withColumn("Id", normalize_key_uuid("Id")).filter(F.col("Id").isNotNull())
    if "NAME" in df.columns:
        df = df.withColumn("NAME", collapse_whitespace(F.col("NAME").cast("string")))
    money_cols = [
        "AMOUNT_COVERED",
        "AMOUNT_UNCOVERED",
        "REVENUE",
    ]
    for c in money_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("decimal(18,4)"))
    int_cols = [
        "COVERED_ENCOUNTERS",
        "UNCOVERED_ENCOUNTERS",
        "COVERED_MEDICATIONS",
        "UNCOVERED_MEDICATIONS",
        "COVERED_PROCEDURES",
        "UNCOVERED_PROCEDURES",
        "COVERED_IMMUNIZATIONS",
        "UNCOVERED_IMMUNIZATIONS",
        "UNIQUE_CUSTOMERS",
        "MEMBER_MONTHS",
    ]
    for c in int_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("long"))
    if "QOLS_AVG" in df.columns:
        df = df.withColumn("QOLS_AVG", F.col("QOLS_AVG").cast("double"))
    # Keep location/phone only in raw; drop from cleaned output per requirement
    drop_cols = ["ADDRESS", "CITY", "STATE_HEADQUARTERED", "ZIP", "PHONE"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)
    w = Window.partitionBy("Id").orderBy(F.col("NAME").asc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def base_providers_keys(providers: DataFrame) -> DataFrame:
    df = providers
    strings = [
        "Id",
        "ORGANIZATION",
        "NAME",
        "GENDER",
        "SPECIALITY",
        "ADDRESS",
        "CITY",
        "STATE",
        "ZIP",
    ]
    df = trim_empty_to_null(df, [c for c in strings if c in df.columns])
    df = df.withColumn("Id", normalize_key_uuid("Id")).filter(F.col("Id").isNotNull())
    if "ORGANIZATION" in df.columns:
        df = df.withColumn("ORGANIZATION", normalize_key_uuid("ORGANIZATION"))
    if "NAME" in df.columns:
        df = df.withColumn("NAME", collapse_whitespace(F.col("NAME").cast("string")))
    if "SPECIALITY" in df.columns:
        df = df.withColumn(
            "SPECIALITY",
            F.upper(collapse_whitespace(F.col("SPECIALITY").cast("string"))),
        )
    if "GENDER" in df.columns:
        df = df.withColumn(
            "GENDER",
            F.upper(F.substring(F.trim(F.col("GENDER").cast("string")), 1, 1)),
        )
    # Normalise textual/location fields (then drop some per requirement below)
    for c in ("ADDRESS", "CITY"):
        if c in df.columns:
            df = df.withColumn(c, collapse_whitespace(F.col(c).cast("string")))
    if "STATE" in df.columns:
        df = df.withColumn("STATE", F.upper(F.trim(F.col("STATE").cast("string"))))
    if "ZIP" in df.columns:
        df = df.withColumn("ZIP", F.trim(F.col("ZIP").cast("string")))
    if "LAT" in df.columns:
        df = df.withColumn("LAT", F.col("LAT").cast("double"))
    if "LON" in df.columns:
        df = df.withColumn("LON", F.col("LON").cast("double"))
    if "UTILIZATION" in df.columns:
        df = df.withColumn("UTILIZATION", F.col("UTILIZATION").cast("long"))
    w = Window.partitionBy("Id").orderBy(F.col("ORGANIZATION").asc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # Drop non-essential provider columns per requirement
    drop_cols = ["ORGANIZATION", "ADDRESS", "CITY", "STATE", "ZIP", "LAT", "LON"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)
    return df


def filter_by_patient_life_ts(
    df: DataFrame,
    *,
    patient_col: str,
    ts_col: str,
    patients: DataFrame,
) -> DataFrame:
    p = patients.select(
        F.col("Id").alias("_p_id"),
        F.col("BIRTHDATE").alias("_birth"),
        F.col("DEATHDATE").alias("_death"),
    )
    out = df.join(p, F.col(patient_col) == F.col("_p_id"), "inner")
    event_date = F.to_date(F.col(ts_col))
    ok_birth = F.col("_birth").isNull() | (event_date >= F.col("_birth"))
    ok_death = F.col("_death").isNull() | (event_date <= F.col("_death"))
    return out.filter(ok_birth & ok_death).drop("_p_id", "_birth", "_death")


def filter_by_patient_life_date(
    df: DataFrame,
    *,
    patient_col: str,
    date_col: str,
    patients: DataFrame,
) -> DataFrame:
    p = patients.select(
        F.col("Id").alias("_p_id"),
        F.col("BIRTHDATE").alias("_birth"),
        F.col("DEATHDATE").alias("_death"),
    )
    out = df.join(p, F.col(patient_col) == F.col("_p_id"), "inner")
    d = F.col(date_col)
    ok_birth = F.col("_birth").isNull() | (d >= F.col("_birth"))
    ok_death = F.col("_death").isNull() | (d <= F.col("_death"))
    return out.filter(ok_birth & ok_death).drop("_p_id", "_birth", "_death")
