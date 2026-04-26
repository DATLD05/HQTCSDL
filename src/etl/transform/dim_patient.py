from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_patient(patients: DataFrame) -> DataFrame:
    return patients.select(
        F.col("Id").alias("Id"),
        F.to_date(F.col("BIRTHDATE")).alias("BirthDate"),
        F.to_date(F.col("DEATHDATE")).alias("DeathDate"),
        F.col("GENDER").alias("Gender"),
        F.col("RACE").alias("Race"),
    ).dropDuplicates(["Id"])

