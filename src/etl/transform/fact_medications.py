from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.transform._star_common import date_key


def build_fact_medications(medications: DataFrame, encounters: DataFrame) -> DataFrame:
    enc = encounters.select(
        F.col("Id").alias("_enc"),
        F.col("PROVIDER").alias("_provider"),
        F.col("PAYER").alias("_payer"),
    )
    m = medications.join(enc, medications["ENCOUNTER"] == enc["_enc"], "left")
    return m.select(
        F.sha2(
            F.concat_ws("||", F.col("ENCOUNTER"), F.col("CODE"), F.date_format(F.col("START"), "yyyy-MM-dd HH:mm:ss")),
            256,
        ).alias("Id"),
        F.col("ENCOUNTER").alias("Encounter_Id"),
        F.col("CODE").cast("string").alias("Medication_Code"),
        F.col("PATIENT").alias("Patient_Id"),
        F.col("_provider").alias("Provider_Id"),
        F.coalesce(F.col("PAYER"), F.col("_payer")).alias("Payer_Id"),
        date_key(F.col("START")).alias("Start_Date_Key"),
        date_key(F.col("STOP")).alias("End_Date_Key"),
        F.lit(None).cast("double").alias("Dosage"),
        F.lit(None).cast("string").alias("Frequency"),
        F.datediff(F.to_date(F.col("STOP")), F.to_date(F.col("START"))).cast("int").alias("Duration_Days"),
        F.col("BASE_COST").cast("double").alias("Base_Cost"),
        F.col("PAYER_COVERAGE").cast("double").alias("Covered_Cost"),
    ).dropDuplicates(["Id"])

