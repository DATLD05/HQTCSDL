from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_encounter(encounters: DataFrame, patients: DataFrame) -> DataFrame:
    p = patients.select(F.col("Id").alias("_pid"), F.to_date(F.col("BIRTHDATE")).alias("_birth"))
    e = encounters.join(p, encounters["PATIENT"] == p["_pid"], "left")
    age = F.floor(F.months_between(F.to_date(F.col("START")), F.col("_birth")) / F.lit(12))
    duration_min = F.floor((F.unix_timestamp(F.col("STOP")) - F.unix_timestamp(F.col("START"))) / F.lit(60))
    return (
        e.withColumn(
            "Age_Group",
            F.when(age < 18, F.lit("0-17"))
            .when(age < 35, F.lit("18-34"))
            .when(age < 50, F.lit("35-49"))
            .when(age < 65, F.lit("50-64"))
            .otherwise(F.lit("65+")),
        )
        .withColumn(
            "Duration_Bucket",
            F.when(duration_min < 30, F.lit("<30m"))
            .when(duration_min < 120, F.lit("30-119m"))
            .when(duration_min < 1440, F.lit("2-24h"))
            .otherwise(F.lit(">=24h")),
        )
        .select(
            F.col("Id").alias("Id"),
            F.col("ENCOUNTERCLASS").alias("EncounterClass"),
            "Age_Group",
            "Duration_Bucket",
        )
        .dropDuplicates(["Id"])
    )

