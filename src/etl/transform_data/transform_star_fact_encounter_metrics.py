from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _ensure_columns(df: DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {', '.join(missing)}")


def build_fact_encounter_metrics(encounters: DataFrame, patients: DataFrame) -> DataFrame:
    _ensure_columns(
        encounters,
        [
            "Id",
            "START",
            "STOP",
            "PATIENT",
            "PROVIDER",
            "PAYER",
            "ENCOUNTERCLASS",
            "BASE_ENCOUNTER_COST",
            "TOTAL_CLAIM_COST",
            "PAYER_COVERAGE",
        ],
        "encounters cleaned dataframe",
    )
    _ensure_columns(patients, ["Id", "BIRTHDATE", "DEATHDATE"], "patients cleaned dataframe")

    p = patients.select(
        F.col("Id").alias("_patient_id"),
        F.col("BIRTHDATE").alias("_birthdate"),
        F.col("DEATHDATE").alias("_deathdate"),
    )
    e = encounters.alias("e").join(p, F.col("e.PATIENT") == F.col("_patient_id"), "left")
    w = Window.partitionBy(F.col("e.PATIENT")).orderBy(F.col("e.START").asc_nulls_last())

    prev_anchor = F.coalesce(
        F.to_date(F.lag(F.col("e.STOP")).over(w)),
        F.to_date(F.lag(F.col("e.START")).over(w)),
    )
    current_start_date = F.to_date(F.col("e.START"))
    current_anchor = F.coalesce(F.to_date(F.col("e.STOP")), current_start_date)
    duration_minutes_raw = (
        F.unix_timestamp(F.col("e.STOP")) - F.unix_timestamp(F.col("e.START"))
    ) / F.lit(60.0)

    return (
        e.select(
            F.col("e.Id").alias("Id"),
            F.col("e.Id").alias("Encounter_Id"),
            F.col("e.PATIENT").alias("Patient_Id"),
            F.col("e.PROVIDER").alias("Provider_Id"),
            F.col("e.PAYER").alias("Payer_Id"),
            F.date_format(F.to_date(F.col("e.START")), "yyyyMMdd").cast("int").alias("Start_Date_Key"),
            F.hour(F.col("e.START")).alias("Start_Time_Key"),
            F.date_format(F.to_date(F.col("e.STOP")), "yyyyMMdd").cast("int").alias("Stop_Date_Key"),
            F.hour(F.col("e.STOP")).alias("Stop_Time_Key"),
            F.floor(F.datediff(current_start_date, F.col("_birthdate")) / F.lit(365.25))
            .cast("int")
            .alias("Patient_Age"),
            F.when(F.col("e.START").isNull() | F.col("e.STOP").isNull(), F.lit(None))
            .otherwise(F.greatest(duration_minutes_raw, F.lit(0.0)))
            .cast("int")
            .alias("Duration_Minutes"),
            F.when(F.col("e.START").isNull() | F.col("e.STOP").isNull(), F.lit(None))
            .otherwise(
                F.greatest(
                    F.datediff(F.to_date(F.col("e.STOP")), F.to_date(F.col("e.START"))),
                    F.lit(0),
                )
            )
            .cast("int")
            .alias("Length_Of_Stay_Days"),
            F.col("e.BASE_ENCOUNTER_COST").cast("double").alias("Base_Encounter_Cost"),
            F.col("e.TOTAL_CLAIM_COST").cast("double").alias("Total_Claim_Cost"),
            F.col("e.PAYER_COVERAGE").cast("double").alias("Payer_Coverage"),
            F.greatest(
                F.coalesce(F.col("e.TOTAL_CLAIM_COST").cast("double"), F.lit(0.0))
                - F.coalesce(F.col("e.PAYER_COVERAGE").cast("double"), F.lit(0.0)),
                F.lit(0.0),
            ).alias("Out_Of_Pocket_Cost"),
            F.when(F.col("e.ENCOUNTERCLASS") == F.lit("inpatient"), F.lit(1))
            .otherwise(F.lit(0))
            .alias("Is_Admitted"),
            F.when(
                prev_anchor.isNotNull()
                & current_start_date.isNotNull()
                & (F.datediff(current_start_date, prev_anchor) <= 30),
                F.lit(1),
            )
            .otherwise(F.lit(0))
            .alias("Is_Readmission_30D"),
            F.when(
                F.col("_deathdate").isNotNull()
                & current_anchor.isNotNull()
                & (F.col("_deathdate") >= current_start_date)
                & (F.col("_deathdate") <= F.date_add(current_anchor, 30)),
                F.lit(1),
            )
            .otherwise(F.lit(0))
            .alias("Is_Death_30D"),
        )
        .filter(F.col("Encounter_Id").isNotNull())
        .dropDuplicates(["Encounter_Id"])
    )
