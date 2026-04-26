from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.transform._star_common import date_key


def build_dim_date(cleaned: dict[str, DataFrame]) -> DataFrame:
    dates = []
    for key, col_name in [
        ("encounters", "START"),
        ("encounters", "STOP"),
        ("conditions", "START"),
        ("procedures", "START"),
        ("medications", "START"),
        ("medications", "STOP"),
    ]:
        dates.append(
            cleaned[key]
            .select(F.to_date(F.col(col_name)).alias("Date"))
            .where(F.col("Date").isNotNull())
        )
    d = dates[0]
    for other in dates[1:]:
        d = d.unionByName(other)
    return (
        d.dropDuplicates(["Date"])
        .withColumn("Date_Key", date_key(F.col("Date")))
        .withColumn("Year", F.year(F.col("Date")))
        .withColumn("Month", F.month(F.col("Date")))
        .withColumn("DayOfWeek", F.date_format(F.col("Date"), "EEEE"))
        .select("Date_Key", "Date", "Year", "Month", "DayOfWeek")
    )

