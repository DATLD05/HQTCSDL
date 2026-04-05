import os
import sys
from pyspark.sql import SparkSession

# 1. Định hướng Python cho Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    # 2. Khởi tạo với cấu hình bypass lỗi Java cao
    spark = SparkSession.builder \
        .appName("TestSpark") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/javax.security.auth=ALL-UNNAMED") \
        .getOrCreate()

    try:
        print("--- Spark Session khoi tao thanh cong ---")
        data = [("Benh An A", 101), ("Benh An B", 102)]
        df = spark.createDataFrame(data, ["TenHoSo", "MaID"])
        df.show()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()