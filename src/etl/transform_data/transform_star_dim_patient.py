from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _ensure_columns(df: DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {', '.join(missing)}")


def build_dim_patient(patients: DataFrame) -> DataFrame:
    _ensure_columns(
        patients,
        ["Id", "BIRTHDATE", "DEATHDATE", "GENDER", "RACE"],
        "patients cleaned dataframe",
    )
    return (
        patients.select(
            F.col("Id").alias("Id"),
            F.col("BIRTHDATE").alias("BirthDate"),
            F.col("DEATHDATE").alias("DeathDate"),
            F.col("GENDER").alias("Gender"),
            F.col("RACE").alias("Race"),
        )
        .filter(F.col("Id").isNotNull())
        .dropDuplicates(["Id"])
    )
