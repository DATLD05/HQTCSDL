from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.etl.transform._star_common import date_key, time_key


def build_fact_encounter_metrics(encounters: DataFrame, patients: DataFrame) -> DataFrame:
    p = patients.select(
        F.col("Id").alias("_pid"),
        F.to_date(F.col("BIRTHDATE")).alias("_birth"),
        F.to_date(F.col("DEATHDATE")).alias("_death"),
    )
    e = encounters.join(p, encounters["PATIENT"] == p["_pid"], "left")
    patient_w = Window.partitionBy("PATIENT").orderBy(F.col("START").asc())
    prev_stop = F.lag("STOP").over(patient_w)
    readmit_days = F.datediff(F.to_date(F.col("START")), F.to_date(prev_stop))
    age = F.floor(F.months_between(F.to_date(F.col("START")), F.col("_birth")) / F.lit(12)).cast("int")
    duration_minutes = F.floor((F.unix_timestamp(F.col("STOP")) - F.unix_timestamp(F.col("START"))) / F.lit(60)).cast(
        "int"
    )
    los_days = F.datediff(F.to_date(F.col("STOP")), F.to_date(F.col("START"))).cast("int")
    oop = F.greatest(
        F.coalesce(F.col("TOTAL_CLAIM_COST").cast("double"), F.lit(0.0))
        - F.coalesce(F.col("PAYER_COVERAGE").cast("double"), F.lit(0.0)),
        F.lit(0.0),
    )
    return (
        e.select(
            F.sha2(F.concat_ws("||", F.col("Id"), F.col("PATIENT")), 256).alias("Id"),
            F.col("Id").alias("Encounter_Id"),
            F.col("PATIENT").alias("Patient_Id"),
            F.col("PROVIDER").alias("Provider_Id"),
            F.col("PAYER").alias("Payer_Id"),
            date_key(F.col("START")).alias("Start_Date_Key"),
            time_key(F.col("START")).alias("Start_Time_Key"),
            date_key(F.col("STOP")).alias("Stop_Date_Key"),
            time_key(F.col("STOP")).alias("Stop_Time_Key"),
            age.alias("Patient_Age"),
            duration_minutes.alias("Duration_Minutes"),
            los_days.alias("Length_Of_Stay_Days"),
            F.col("BASE_ENCOUNTER_COST").cast("double").alias("Base_Encounter_Cost"),
            F.col("TOTAL_CLAIM_COST").cast("double").alias("Total_Claim_Cost"),
            F.col("PAYER_COVERAGE").cast("double").alias("Payer_Coverage"),
            oop.alias("Out_Of_Pocket_Cost"),
            F.when(F.lower(F.col("ENCOUNTERCLASS")) == F.lit("inpatient"), F.lit(1)).otherwise(F.lit(0)).alias(
                "Is_Admitted"
            ),
            F.when(readmit_days.between(0, 30), F.lit(1)).otherwise(F.lit(0)).alias("Is_Readmission_30D"),
            F.when(
                F.col("_death").isNotNull()
                & F.datediff(F.col("_death"), F.to_date(F.col("STOP"))).between(0, 30),
                F.lit(1),
            ).otherwise(F.lit(0)).alias("Is_Death_30D"),
        )
        .dropDuplicates(["Encounter_Id"])
    )

