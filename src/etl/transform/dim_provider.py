from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_provider(providers: DataFrame) -> DataFrame:
    return providers.select(
        F.col("Id").alias("Id"),
        F.col("NAME").alias("Name"),
        F.col("SPECIALITY").alias("Speciality"),
    ).dropDuplicates(["Id"])

