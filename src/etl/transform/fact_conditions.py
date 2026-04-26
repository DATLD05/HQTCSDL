from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.transform._star_common import date_key


def build_fact_conditions(conditions: DataFrame, encounters: DataFrame) -> DataFrame:
    enc = encounters.select(
        F.col("Id").alias("_enc"),
        F.col("PROVIDER").alias("_provider"),
        F.col("PAYER").alias("_payer"),
    )
    c = conditions.join(enc, conditions["ENCOUNTER"] == enc["_enc"], "left")
    return c.select(
        F.sha2(F.concat_ws("||", F.col("ENCOUNTER"), F.col("CODE"), F.col("START")), 256).alias("Id"),
        F.col("ENCOUNTER").alias("Encounter_Id"),
        F.col("CODE").cast("string").alias("Condition_Code"),
        date_key(F.to_timestamp(F.col("START"))).alias("Start_Date_Key"),
        F.col("PATIENT").alias("Patient_Id"),
        F.col("_provider").alias("Provider_Id"),
        F.col("_payer").alias("Payer_Id"),
    ).dropDuplicates(["Id"])

