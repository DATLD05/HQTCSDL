from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_payer(payers: DataFrame) -> DataFrame:
    return payers.select(
        F.col("Id").alias("Id"),
        F.col("NAME").alias("Name"),
        F.col("STATE_HEADQUARTERED").alias("State_Headquartered"),
    ).dropDuplicates(["Id"])

