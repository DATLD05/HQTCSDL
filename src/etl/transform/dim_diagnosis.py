from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_diagnosis(conditions: DataFrame) -> DataFrame:
    desc = F.lower(F.coalesce(F.col("Description"), F.lit("")))
    return (
        conditions.select(
            F.col("CODE").cast("string").alias("Code"),
            F.col("DESCRIPTION").cast("string").alias("Description"),
        )
        .where(F.col("Code").isNotNull() & (F.trim(F.col("Code")) != ""))
        .withColumn(
            "Diagnosis_Group",
            F.when(desc.rlike("diabetes"), F.lit("Diabetes"))
            .when(desc.rlike("hypertension|blood pressure"), F.lit("Cardiovascular"))
            .when(desc.rlike("pregnan"), F.lit("Pregnancy"))
            .otherwise(F.lit("General")),
        )
        .dropDuplicates(["Code"])
        .select("Code", "Description", "Diagnosis_Group")
    )

