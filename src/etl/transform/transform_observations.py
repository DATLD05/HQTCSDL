from pyspark.sql import DataFrame, functions as F


def _trim_empty_to_null(df: DataFrame, columns: list[str]) -> DataFrame:
    out = df
    for name in columns:
        if name in out.columns:
            out = out.withColumn(
                name, F.nullif(F.trim(F.col(name).cast("string")), F.lit(""))
            )
    return out


def transform(
    observations_raw: DataFrame, encounters_transformed: DataFrame
) -> DataFrame:
    keys = encounters_transformed.select(
        F.col("Id").alias("ENCOUNTER")
    ).where(F.col("ENCOUNTER").isNotNull()).distinct()
    string_cols = [
        "PATIENT",
        "ENCOUNTER",
        "CATEGORY",
        "CODE",
        "DESCRIPTION",
        "UNITS",
        "TYPE",
    ]
    df = _trim_empty_to_null(observations_raw, string_cols)
    df = df.filter(F.col("ENCOUNTER").isNotNull())
    df = df.join(keys, on="ENCOUNTER", how="inner")
    df = df.withColumn("DATE", F.to_timestamp(F.col("DATE")))
    df = df.withColumn(
        "VALUE_NUMERIC",
        F.when(F.lower(F.col("TYPE")) == "numeric", F.col("VALUE").cast("double")).otherwise(
            F.lit(None)
        ),
    )
    df = df.withColumn("CODE", F.col("CODE").cast("string"))
    return df.dropDuplicates(
        ["DATE", "ENCOUNTER", "CODE", "DESCRIPTION", "CATEGORY", "PATIENT"]
    )
