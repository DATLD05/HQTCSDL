import os
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from src.etl.csv_utils import normalize_column_names

# Tải biến môi trường từ file .env
load_dotenv()

def get_jdbc_url() -> str:
    """Tạo chuỗi kết nối JDBC từ biến môi trường."""
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "")
    db_name = os.environ.get("DB_NAME", "")
    instance = os.environ.get("DB_INSTANCE", "")
    
    # Nếu có port thì dùng port, không thì dùng host mặc định
    base_host = f"{host}:{port}" if port else host
    
    url = f"jdbc:sqlserver://{base_host};databaseName={db_name};encrypt=true;trustServerCertificate=true;"
    
    # Bổ sung Named Instance nếu có (ví dụ: SQLEXPRESS)
    if instance:
        url += f"instanceName={instance};"
        
    return url

def get_jdbc_properties() -> dict:
    """Lấy thông tin đăng nhập JDBC từ biến môi trường."""
    return {
        "user": os.environ.get("DB_USER", ""),
        "password": os.environ.get("DB_PASSWORD", ""),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }



def extract_table(spark: SparkSession, schema: str, table_name: str) -> DataFrame:
    """Trích xuất một bảng cụ thể từ SQL Server."""
    jdbc_url = get_jdbc_url()
    properties = get_jdbc_properties()
    full_table_name = f"{schema}.{table_name}"
    
    # Sử dụng PySpark JDBC để đọc bảng
    # Lưu ý: PySpark phải được cấu hình với thư viện mssql-jdbc
    df = spark.read.jdbc(url=jdbc_url, table=full_table_name, properties=properties)
    return normalize_column_names(df)

def extract_all_sql_server(spark: SparkSession) -> dict[str, DataFrame]:
    """
    Load tất cả 8 bảng từ SQL Server (schema 'raw') như các DataFrame gốc.
    Các bảng: patients, payers, providers, encounters, conditions, observations, procedures, medications
    """
    tables = [
        "patients",
        "payers",
        "providers",
        "encounters",
        "conditions",
        "observations",
        "procedures",
        "medications",
    ]
    
    schema = "raw"
    dataframes = {}
    
    for table in tables:
        # Nếu lỗi (ví dụ sai thông tin đăng nhập hoặc bảng không tồn tại),
        # lỗi sẽ được raise ra để dừng pipeline (giống với hành vi đọc CSV).
        dataframes[table] = extract_table(spark, schema, table)
        print(f"Đã trích xuất thành công bảng {schema}.{table}")
            
    return dataframes
