"""
Build Data Warehouse tables for procedures domain (PySpark):
- Dim_Procedure
- Dim_Time
- Fact_Procedures

This pipeline reads cleaned CSV outputs from the ETL layer and writes
single CSV files under ``src/etl/transform_data`` by default.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.etl.csv_utils import parse_ts_any, read_csv, write_single_csv
from src.etl.paths import (
    ENCOUNTERS_CSV,
    ETL_DIR,
    PROCEDURES_CSV,
    cleaned_csv_path,
)


def _time_bucket(hour_col: F.Column) -> F.Column:
    return (
        F.when((hour_col >= 0) & (hour_col <= 5), F.lit("Night"))
        .when((hour_col >= 6) & (hour_col <= 11), F.lit("Morning"))
        .when((hour_col >= 12) & (hour_col <= 17), F.lit("Afternoon"))
        .otherwise(F.lit("Evening"))
    )


def build_dim_time(spark: SparkSession) -> DataFrame:
    """Create a reusable 24-hour time dimension."""
    hours = spark.range(0, 24).select(F.col("id").cast("int").alias("Hour"))
    return (
        hours.withColumn("Time_Key", F.col("Hour"))
        .withColumn("Time_Bucket", _time_bucket(F.col("Hour")))
        .select("Time_Key", "Hour", "Time_Bucket")
    )


def _procedure_category(description_col: F.Column, *, category_mode: str) -> F.Column:
    """
    Procedure category strategy.

    - unknown: production-safe fallback (no heuristic classification)
    - keyword: lightweight keyword-based heuristic (easy to revise in team reviews)
    """
    if category_mode == "unknown":
        return F.lit("Unknown")

    d = F.lower(F.trim(description_col.cast("string")))
    return (
        F.when(d.rlike("assessment|screen|examination|exam"), F.lit("Assessment"))
        .when(d.rlike("extraction|surgery|repair|implant|procedure"), F.lit("Intervention"))
        .when(d.rlike("education|counsel|advice|follow-up"), F.lit("Counseling/FollowUp"))
        .otherwise(F.lit("General"))
    )


def build_dim_procedure(procedures: DataFrame, *, category_mode: str = "unknown") -> DataFrame:
    """
    Build procedure dimension:
    - Code (PK)
    - Description
    - Procedure_Category
    - Is_Top_Pareto (top 80% by frequency)
    """
    base = (
        procedures.withColumn("_Code", F.trim(F.col("CODE").cast("string")))
        .withColumn("_Description", F.trim(F.col("DESCRIPTION").cast("string")))
        .where(F.col("_Code").isNotNull() & (F.col("_Code") != ""))
    )

    # Pick one representative description per code (most frequent description).
    desc_freq = (
        base.groupBy("_Code", "_Description")
        .agg(F.count("*").alias("_desc_freq"))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("_Code").orderBy(
                    F.col("_desc_freq").desc(),
                    F.col("_Description").asc_nulls_last(),
                )
            ),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn", "_desc_freq")
    )

    code_freq = base.groupBy("_Code").agg(F.count("*").alias("_freq"))

    total_w = Window.partitionBy()
    pareto_w = Window.orderBy(F.col("_freq").desc(), F.col("_Code").asc())
    pareto = (
        code_freq.withColumn("_total", F.sum("_freq").over(total_w))
        .withColumn("_cum", F.sum("_freq").over(pareto_w))
        .withColumn("_rank", F.row_number().over(pareto_w))
        .withColumn(
            "Is_Top_Pareto",
            (F.col("_cum") <= F.col("_total") * F.lit(0.8)) | (F.col("_rank") == 1),
        )
        .select("_Code", "Is_Top_Pareto")
    )

    out = (
        desc_freq.join(pareto, on="_Code", how="left")
        .withColumn(
            "Procedure_Category",
            _procedure_category(F.col("_Description"), category_mode=category_mode),
        )
        .select(
            F.col("_Code").alias("Code"),
            F.col("_Description").alias("Description"),
            F.col("Procedure_Category"),
            F.coalesce(F.col("Is_Top_Pareto"), F.lit(False)).alias("Is_Top_Pareto"),
        )
        .dropDuplicates(["Code"])
    )
    return out


def build_fact_procedures(
    procedures: DataFrame,
    encounters: DataFrame,
    dim_time: DataFrame,
    *,
    keep_orphan_encounters: bool = False,
    id_mode: str = "deterministic",
    unclaimed_mode: str = "base_cost",
) -> DataFrame:
    """
    Build fact table for procedures.

        Unclaimed_Cost strategy:
        - base_cost: Unclaimed = Base_Cost (safe phase-1 default)
        - encounter_gap_ratio: infer by encounter-level claim gap ratio
            max(TOTAL_CLAIM_COST - PAYER_COVERAGE, 0) / TOTAL_CLAIM_COST
    """
    proc = (
        procedures.withColumn("_encounter", F.trim(F.col("ENCOUNTER").cast("string")))
        .withColumn("_patient", F.trim(F.col("PATIENT").cast("string")))
        .withColumn("_code", F.trim(F.col("CODE").cast("string")))
        .withColumn("_start_ts", parse_ts_any(F.col("START")))
        .withColumn("_stop_ts", parse_ts_any(F.col("STOP")))
        .withColumn("_base_cost", F.col("BASE_COST").cast("double"))
        .where(F.col("_start_ts").isNotNull())
        .where(F.col("_code").isNotNull() & (F.col("_code") != ""))
        .where(F.col("_encounter").isNotNull() & (F.col("_encounter") != ""))
    )

    proc = proc.dropDuplicates(["_encounter", "_code", "_start_ts", "_patient"])

    enc = encounters.select(
        F.col("Id").alias("_enc_id"),
        F.col("PROVIDER").alias("_provider"),
        F.col("PAYER").alias("_payer"),
        F.col("TOTAL_CLAIM_COST").cast("double").alias("_enc_total_claim"),
        F.col("PAYER_COVERAGE").cast("double").alias("_enc_payer_coverage"),
    ).dropDuplicates(["_enc_id"])

    join_mode = "left" if keep_orphan_encounters else "inner"
    fact = proc.join(enc, proc["_encounter"] == enc["_enc_id"], join_mode).drop("_enc_id")

    if keep_orphan_encounters:
        fact = fact.withColumn("_provider", F.coalesce(F.col("_provider"), F.lit("Unknown")))
        fact = fact.withColumn("_payer", F.coalesce(F.col("_payer"), F.lit("Unknown")))

    fact = fact.withColumn("_hour", F.hour(F.col("_start_ts")))
    fact = fact.join(dim_time, fact["_hour"] == dim_time["Hour"], "left")

    duration_minutes = F.greatest(
        F.floor(
            (
                F.coalesce(F.unix_timestamp(F.col("_stop_ts")), F.unix_timestamp(F.col("_start_ts")))
                - F.unix_timestamp(F.col("_start_ts"))
            )
            / F.lit(60)
        ).cast("int"),
        F.lit(0),
    )

    if id_mode == "uuid":
        fact_id = F.expr("uuid()")
    else:
        fact_id = F.sha2(
            F.concat_ws(
                "||",
                F.col("_encounter"),
                F.col("_code"),
                F.date_format(F.col("_start_ts"), "yyyy-MM-dd HH:mm:ss"),
                F.coalesce(F.col("_patient"), F.lit("")),
            ),
            256,
        )

    encounter_gap_ratio = F.when(
        F.col("_enc_total_claim").isNotNull() & (F.col("_enc_total_claim") > 0),
        F.greatest(F.col("_enc_total_claim") - F.coalesce(F.col("_enc_payer_coverage"), F.lit(0.0)), F.lit(0.0))
        / F.col("_enc_total_claim"),
    ).otherwise(F.lit(None))

    if unclaimed_mode == "encounter_gap_ratio":
        unclaimed_cost = F.greatest(
            F.coalesce(F.col("_base_cost"), F.lit(0.0)) * F.coalesce(encounter_gap_ratio, F.lit(1.0)),
            F.lit(0.0),
        )
    else:
        # Phase-1 default (simple + stable): no covered-cost at procedure grain.
        unclaimed_cost = F.coalesce(F.col("_base_cost"), F.lit(0.0))

    return fact.select(
        fact_id.alias("Id"),
        F.col("_encounter").alias("Encounter_Id"),
        F.col("_code").alias("Procedure_Code"),
        F.date_format(F.col("_start_ts"), "yyyyMMdd").cast("int").alias("Start_Date_Key"),
        F.col("Time_Key").cast("int").alias("Start_Time_Key"),
        F.col("_patient").alias("Patient_Id"),
        F.col("_provider").alias("Provider_Id"),
        F.col("_payer").alias("Payer_Id"),
        duration_minutes.alias("Procedure_Duration_Minutes"),
        F.col("_base_cost").cast("double").alias("Base_Cost"),
        unclaimed_cost.cast("double").alias("Unclaimed_Cost"),
    )


def run(
    spark: SparkSession,
    *,
    procedures_path: Path | None = None,
    encounters_path: Path | None = None,
    output_dir: Path | None = None,
    keep_orphan_encounters: bool = False,
    id_mode: str = "deterministic",
    category_mode: str = "unknown",
    unclaimed_mode: str = "base_cost",
) -> dict[str, DataFrame]:
    procedures_src = procedures_path or cleaned_csv_path(PROCEDURES_CSV)
    encounters_src = encounters_path or cleaned_csv_path(ENCOUNTERS_CSV)
    out_dir = Path(output_dir) if output_dir is not None else (ETL_DIR / "transform_data")

    procedures = read_csv(spark, procedures_src)
    encounters = read_csv(spark, encounters_src)

    dim_time = build_dim_time(spark)
    dim_procedure = build_dim_procedure(procedures, category_mode=category_mode)
    fact_procedures = build_fact_procedures(
        procedures,
        encounters,
        dim_time,
        keep_orphan_encounters=keep_orphan_encounters,
        id_mode=id_mode,
        unclaimed_mode=unclaimed_mode,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    write_single_csv(dim_procedure, out_dir / "Dim_Procedure.csv")
    write_single_csv(dim_time, out_dir / "Dim_Time.csv")
    write_single_csv(fact_procedures, out_dir / "Fact_Procedures.csv")

    return {
        "Dim_Procedure": dim_procedure,
        "Dim_Time": dim_time,
        "Fact_Procedures": fact_procedures,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Dim_Procedure, Dim_Time, Fact_Procedures")
    parser.add_argument("--procedures", type=Path, default=None, help="Path to procedures_cleaned.csv")
    parser.add_argument("--encounters", type=Path, default=None, help="Path to encounters_cleaned.csv")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory for DW CSV files")
    parser.add_argument(
        "--keep-orphan-encounters",
        action="store_true",
        help="Keep rows without encounter match and fill Provider/Payer as 'Unknown'",
    )
    parser.add_argument(
        "--id-mode",
        choices=("deterministic", "uuid"),
        default="deterministic",
        help="Fact ID strategy",
    )
    parser.add_argument(
        "--category-mode",
        choices=("unknown", "keyword"),
        default="unknown",
        help="Procedure category strategy for Dim_Procedure",
    )
    parser.add_argument(
        "--unclaimed-mode",
        choices=("base_cost", "encounter_gap_ratio"),
        default="base_cost",
        help="Unclaimed_Cost strategy in Fact_Procedures",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark = SparkSession.builder.appName("DW_Procedures_Pipeline").getOrCreate()
    try:
        outputs = run(
            spark,
            procedures_path=args.procedures,
            encounters_path=args.encounters,
            output_dir=args.output_dir,
            keep_orphan_encounters=args.keep_orphan_encounters,
            id_mode=args.id_mode,
            category_mode=args.category_mode,
            unclaimed_mode=args.unclaimed_mode,
        )
        print("=== DW tables built ===")
        for name, df in outputs.items():
            print(f"{name}: {df.count()} rows")
    finally:
        spark.stop()
