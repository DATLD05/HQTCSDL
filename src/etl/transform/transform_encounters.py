from pyspark.sql import DataFrame, functions as F


def _trim_empty_to_null(df: DataFrame, columns: list[str]) -> DataFrame:
    out = df
    for name in columns:
        if name in out.columns:
            out = out.withColumn(
                name, F.nullif(F.trim(F.col(name).cast("string")), F.lit(""))
            )
    return out


def transform(raw: DataFrame) -> DataFrame:
    string_cols = [
        "Id",
        "PATIENT",
        "PROVIDER",
        "PAYER",
        "ENCOUNTERCLASS",
        "CODE",
        "DESCRIPTION",
        "REASONCODE",
        "REASONDESCRIPTION",
    ]
    df = _trim_empty_to_null(raw, string_cols)
    df = df.drop("ORGANIZATION")
    df = df.filter(F.col("Id").isNotNull())
    df = (
        df.withColumn("START", F.to_timestamp(F.col("START")))
        .withColumn("STOP", F.to_timestamp(F.col("STOP")))
        .withColumn(
            "CODE",
            F.when(F.col("CODE").isNull(), F.lit(None)).otherwise(
                F.format_string("%.0f", F.col("CODE").cast("double"))
            ),
        )
        .withColumn("BASE_ENCOUNTER_COST", F.col("BASE_ENCOUNTER_COST").cast("decimal(14,2)"))
        .withColumn("TOTAL_CLAIM_COST", F.col("TOTAL_CLAIM_COST").cast("decimal(14,2)"))
        .withColumn("PAYER_COVERAGE", F.col("PAYER_COVERAGE").cast("decimal(14,2)"))
        .withColumn("REASONCODE", F.col("REASONCODE").cast("string"))
    )
    return df.dropDuplicates(["Id"])
