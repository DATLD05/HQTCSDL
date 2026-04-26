from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.transform._star_common import date_key, time_key


def build_fact_procedures(procedures: DataFrame, encounters: DataFrame) -> DataFrame:
    enc = encounters.select(
        F.col("Id").alias("_enc"),
        F.col("PROVIDER").alias("_provider"),
        F.col("PAYER").alias("_payer"),
    )
    p = procedures.join(enc, procedures["ENCOUNTER"] == enc["_enc"], "left")
    return p.select(
        F.sha2(
            F.concat_ws("||", F.col("ENCOUNTER"), F.col("CODE"), F.date_format(F.col("START"), "yyyy-MM-dd HH:mm:ss")),
            256,
        ).alias("Id"),
        F.col("ENCOUNTER").alias("Encounter_Id"),
        F.col("CODE").cast("string").alias("Procedure_Code"),
        date_key(F.col("START")).alias("Start_Date_Key"),
        time_key(F.col("START")).alias("Start_Time_Key"),
        F.col("PATIENT").alias("Patient_Id"),
        F.col("_provider").alias("Provider_Id"),
        F.col("_payer").alias("Payer_Id"),
        F.floor((F.unix_timestamp(F.col("STOP")) - F.unix_timestamp(F.col("START"))) / F.lit(60))
        .cast("int")
        .alias("Procedure_Duration_Minutes"),
        F.col("BASE_COST").cast("double").alias("Base_Cost"),
        F.coalesce(F.col("BASE_COST").cast("double"), F.lit(0.0)).alias("Unclaimed_Cost"),
    ).dropDuplicates(["Id"])

