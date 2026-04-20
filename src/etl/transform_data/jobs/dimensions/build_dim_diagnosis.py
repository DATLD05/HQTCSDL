from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, trim

def run_dim_diagnosis(spark: SparkSession, raw_bucket: str, gold_bucket: str):
    print("Đang xử lý Dim_Diagnosis...")
    
    df_raw = spark.read.parquet(f"{raw_bucket}/conditions/")
    
    dim_diagnosis = df_raw.select(
        col("CODE").alias("Code"),
        col("DESCRIPTION").alias("Description")
    ).dropDuplicates(["Code"])
    
    dim_diagnosis_final = dim_diagnosis.select(
        trim(col("Code")).alias("Code"),
        trim(col("Description")).alias("Description"),
        col("Description").alias("Diagnosis_Group") 
    )
    
    print(f"Đã xử lý xong Dim_Diagnosis. Số dòng: {dim_diagnosis_final.count()}")
    return dim_diagnosis_final

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Build_Dim_Diagnosis").getOrCreate()
    run_dim_diagnosis(spark, "gs://raw_data", "gs://gold_data")