from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _ensure_columns(df: DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {', '.join(missing)}")


def build_dim_provider(providers: DataFrame) -> DataFrame:
    _ensure_columns(
        providers,
        ["Id", "NAME", "SPECIALITY"],
        "providers cleaned dataframe",
    )
    return (
        providers.select(
            F.col("Id").alias("Id"),
            F.col("NAME").alias("Name"),
            F.col("SPECIALITY").alias("Speciality"),
        )
        .filter(F.col("Id").isNotNull())
        .dropDuplicates(["Id"])
    )
