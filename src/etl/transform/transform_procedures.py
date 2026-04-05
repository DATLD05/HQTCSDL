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


def transform(
    procedures_raw: DataFrame, encounters_transformed: DataFrame
) -> DataFrame:
    keys = encounters_transformed.select(
        F.col("Id").alias("ENCOUNTER")
    ).where(F.col("ENCOUNTER").isNotNull()).distinct()
    string_cols = ["PATIENT", "ENCOUNTER", "DESCRIPTION", "REASONCODE", "REASONDESCRIPTION"]
    df = _trim_empty_to_null(procedures_raw, string_cols)
    df = df.filter(F.col("ENCOUNTER").isNotNull())
    df = df.join(keys, on="ENCOUNTER", how="inner")
    df = (
        df.withColumn("START", F.to_timestamp(F.col("START")))
        .withColumn("STOP", F.to_timestamp(F.col("STOP")))
        .withColumn("CODE", _normalize_snomed_code("CODE"))
        .withColumn("BASE_COST", F.col("BASE_COST").cast("decimal(14,2)"))
    )
    return df.dropDuplicates(
        ["ENCOUNTER", "CODE", "START", "STOP", "PATIENT"]
    )
