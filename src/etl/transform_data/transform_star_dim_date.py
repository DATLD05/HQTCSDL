from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _ensure_columns(df: DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {', '.join(missing)}")


def build_dim_date(encounters: DataFrame) -> DataFrame:
    _ensure_columns(encounters, ["START", "STOP"], "encounters cleaned dataframe")

    start_dates = encounters.select(F.to_date("START").alias("Date"))
    stop_dates = encounters.select(F.to_date("STOP").alias("Date"))
    all_dates = (
        start_dates.unionByName(stop_dates)
        .filter(F.col("Date").isNotNull())
        .distinct()
    )
    return all_dates.select(
        F.date_format("Date", "yyyyMMdd").cast("int").alias("Date_Key"),
        F.col("Date"),
        F.year("Date").alias("Year"),
        F.month("Date").alias("Month"),
        F.date_format("Date", "E").alias("DayOfWeek"),
    )
