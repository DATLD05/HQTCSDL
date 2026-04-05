from pyspark.sql import DataFrame


def normalize_column_names(df: DataFrame) -> DataFrame:
    out = df
    for name in df.columns:
        stripped = name.strip()
        if name != stripped:
            out = out.withColumnRenamed(name, stripped)
    return out
