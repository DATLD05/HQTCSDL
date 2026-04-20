from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

def run_dim_encounter(spark: SparkSession, raw_bucket: str, gold_bucket: str):
    print("Đang xử lý Dim_Encounter...")
    
    df_raw = spark.read.parquet(f"{raw_bucket}/encounters/")
    
    dim_encounter = df_raw.select(
        col("Id"),
        col("ENCOUNTERCLASS").alias("EncounterClass")
    ).dropDuplicates(["Id"])
    
    dim_encounter_final = dim_encounter.select(
        trim(col("Id")).alias("Id"),
        trim(col("EncounterClass")).alias("EncounterClass")
    )
    
    
    print(f"Đã xử lý xong Dim_Encounter. Số dòng: {dim_encounter_final.count()}")
    return dim_encounter_final

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Build_Dim_Encounter").getOrCreate()
    run_dim_encounter(spark, "gs://raw_data", "gs://gold_data")