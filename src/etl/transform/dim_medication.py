from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_medication(medications: DataFrame) -> DataFrame:
    return (
        medications.select(
            F.col("CODE").cast("string").alias("Code"),
            F.col("DESCRIPTION").cast("string").alias("Name"),
        )
        .where(F.col("Code").isNotNull() & (F.trim(F.col("Code")) != ""))
        .withColumn(
            "Drug_Class",
            F.when(F.lower(F.coalesce(F.col("Name"), F.lit(""))).rlike("cillin|mycin"), F.lit("Antibiotic"))
            .when(
                F.lower(F.coalesce(F.col("Name"), F.lit(""))).rlike("ibuprofen|acetaminophen|analges"),
                F.lit("Analgesic"),
            )
            .otherwise(F.lit("General")),
        )
        .withColumn(
            "Form",
            F.when(F.lower(F.coalesce(F.col("Name"), F.lit(""))).rlike("tablet|tab"), F.lit("Tablet"))
            .when(F.lower(F.coalesce(F.col("Name"), F.lit(""))).rlike("inject"), F.lit("Injection"))
            .otherwise(F.lit("Other")),
        )
        .withColumn(
            "Strength",
            F.regexp_extract(F.coalesce(F.col("Name"), F.lit("")), r"(\d+(\.\d+)?\s?(mg|g|ml|UNT/ML))", 1),
        )
        .withColumn(
            "Strength",
            F.when(F.col("Strength") == "", F.lit(None)).otherwise(F.col("Strength")),
        )
        .dropDuplicates(["Code"])
        .select("Code", "Name", "Drug_Class", "Form", "Strength")
    )

