from pyspark.sql import functions as F


def date_key(col: F.Column) -> F.Column:
    return F.date_format(col, "yyyyMMdd").cast("int")


def time_key(col: F.Column) -> F.Column:
    return F.hour(col).cast("int")


def derive_time_bucket(hour_col: F.Column) -> F.Column:
    return (
        F.when((hour_col >= 0) & (hour_col <= 5), F.lit("Night"))
        .when((hour_col >= 6) & (hour_col <= 11), F.lit("Morning"))
        .when((hour_col >= 12) & (hour_col <= 17), F.lit("Afternoon"))
        .otherwise(F.lit("Evening"))
    )

