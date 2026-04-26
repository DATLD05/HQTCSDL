from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_dim_procedure(procedures: DataFrame) -> DataFrame:
    base = (
        procedures.select(
            F.col("CODE").cast("string").alias("Code"),
            F.col("DESCRIPTION").cast("string").alias("Description"),
        )
        .where(F.col("Code").isNotNull() & (F.trim(F.col("Code")) != ""))
        .dropDuplicates(["Code", "Description"])
    )
    freq = procedures.groupBy(F.col("CODE").cast("string").alias("Code")).agg(
        F.count("*").alias("_freq")
    )
    total_w = Window.partitionBy()
    rank_w = Window.orderBy(F.col("_freq").desc(), F.col("Code").asc())
    pareto = (
        freq.withColumn("_total", F.sum("_freq").over(total_w))
        .withColumn("_cum", F.sum("_freq").over(rank_w))
        .withColumn("Is_Top_Pareto", F.col("_cum") <= F.col("_total") * F.lit(0.8))
        .select("Code", "Is_Top_Pareto")
    )
    desc = F.lower(F.coalesce(F.col("Description"), F.lit("")))
    return (
        base.join(pareto, on="Code", how="left")
        .withColumn(
            "Procedure_Category",
            F.when(desc.rlike("exam|assessment|screen"), F.lit("Assessment"))
            .when(desc.rlike("repair|surgery|implant|extraction"), F.lit("Intervention"))
            .when(desc.rlike("education|counsel|follow-up"), F.lit("Counseling/FollowUp"))
            .otherwise(F.lit("General")),
        )
        .select("Code", "Description", "Procedure_Category", "Is_Top_Pareto")
        .dropDuplicates(["Code"])
    )

