from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.transform._star_common import derive_time_bucket


def build_dim_time(spark_df: DataFrame) -> DataFrame:
    spark = spark_df.sparkSession
    t = spark.range(0, 24).select(F.col("id").cast("int").alias("Hour"))
    return (
        t.withColumn("Time_Key", F.col("Hour"))
        .withColumn("Time_Bucket", derive_time_bucket(F.col("Hour")))
        .select("Time_Key", "Hour", "Time_Bucket")
    )

