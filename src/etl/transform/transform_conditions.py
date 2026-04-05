from pyspark.sql import DataFrame, functions as F


def _trim_empty_to_null(df: DataFrame, columns: list[str]) -> DataFrame:
    out = df
    for name in columns:
        if name in out.columns:
            out = out.withColumn(
                name, F.nullif(F.trim(F.col(name).cast("string")), F.lit(""))
            )
    return out


def _normalize_snomed_code(column: str = "CODE") -> F.Column:
    c = F.col(column)
    return F.when(c.isNull(), F.lit(None)).otherwise(
        F.format_string("%.0f", c.cast("double"))
    )


def transform(conditions_raw: DataFrame, encounters_transformed: DataFrame) -> DataFrame:
    keys = encounters_transformed.select(
        F.col("Id").alias("ENCOUNTER")
    ).where(F.col("ENCOUNTER").isNotNull()).distinct()
    string_cols = ["PATIENT", "ENCOUNTER", "DESCRIPTION"]
    df = _trim_empty_to_null(conditions_raw, string_cols)
    df = df.filter(F.col("ENCOUNTER").isNotNull())
    df = df.join(keys, on="ENCOUNTER", how="inner")
    df = (
        df.withColumn(
            "START",
            F.try_to_date(F.trim(F.col("START").cast("string")), "M/d/yyyy"),
        )
        .withColumn(
            "STOP",
            F.try_to_date(F.trim(F.col("STOP").cast("string")), "M/d/yyyy"),
        )
        .withColumn("CODE", _normalize_snomed_code("CODE"))
    )
    return df.dropDuplicates(["ENCOUNTER", "CODE", "START", "PATIENT"])
