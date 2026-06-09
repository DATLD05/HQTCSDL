# HQTCSDL - Healthcare ETL Data Warehouse

Dự án này xây dựng pipeline ETL cho dữ liệu hồ sơ bệnh án/bệnh viện. Pipeline đọc dữ liệu nguồn, làm sạch bằng PySpark, chuyển đổi sang mô hình star schema và có thể nạp kết quả lên BigQuery.

Luồng tổng quát:

```text
raw CSV / SQL Server
-> extract
-> clean
-> transform star schema
-> write CSV
-> optional load to BigQuery
```

## Mục Tiêu

- Chuẩn hóa dữ liệu y tế từ nhiều bảng nguồn.
- Loại bỏ bản ghi sai quan hệ hoặc sai thời gian nghiệp vụ.
- Tạo các bảng dimension và fact phục vụ phân tích.
- Xuất dữ liệu star schema ra CSV.
- Hỗ trợ load dữ liệu star schema lên Google BigQuery.

## Cấu Trúc Dự Án

```text
.
├── main.py                         # Script kiểm tra Spark đơn giản
├── pyproject.toml                  # Cấu hình project và toàn bộ dependency
├── requirements.txt                # File tương thích, trỏ về pyproject.toml
├── src/etl/
│   ├── paths.py                    # Đường dẫn dữ liệu raw/clean/star
│   ├── csv_utils.py                # Helper đọc/ghi CSV và chuẩn hóa dữ liệu
│   ├── extract/                    # Đọc dữ liệu từ CSV hoặc SQL Server
│   ├── clean/                      # Làm sạch và kiểm tra dữ liệu
│   ├── transform/                  # Tạo star schema
│   ├── load/                       # Load star CSV lên BigQuery
│   ├── pipelines/pipeline_all.py   # Pipeline ETL end-to-end
│   ├── integrity_audit.py          # Helper audit ràng buộc dữ liệu
│   ├── data/raw/                   # Dữ liệu nguồn
│   ├── data/clean/                 # Dữ liệu sau clean
│   └── data/star/                  # Dữ liệu star schema
└── warehouse/
    ├── ddl/dimensions/             # DDL tạo bảng dimension trên BigQuery
    ├── ddl/facts/                  # DDL tạo bảng fact trên BigQuery
    └── scripts/main.py             # Script chạy DDL BigQuery
```

## Yêu Cầu Môi Trường

Dự án yêu cầu Python `>=3.10,<3.12`. Nên dùng Python 3.10 hoặc 3.11.

PySpark cũng cần Java được cài sẵn. Nếu Spark không khởi động được, hãy kiểm tra:

```bash
java -version
python --version
```

## Cài Đặt

Từ thư mục dự án:

```bash
cd HQTCSDL
```

Tạo virtual environment:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
```

Cài dependency:

```bash
pip install -e .
```

Nếu máy không có `python3.10`, có thể dùng `python3.11`. Không nên dùng Python 3.12 vì project đang khai báo `<3.12`.

Toàn bộ dependency chính được khai báo trong `pyproject.toml`. File `requirements.txt` chỉ được giữ để tương thích với workflow cũ; nếu chạy `pip install -r requirements.txt` thì pip cũng sẽ cài project thông qua `-e .`.

## Dữ Liệu Đầu Vào

Pipeline mặc định đọc 8 file CSV trong `src/etl/data/raw/`:

```text
patients.csv
payers.csv
providers.csv
encounters.csv
conditions.csv
observations.csv
procedures.csv
medications.csv
```

Các file này được đọc qua `src/etl/extract/extract_all.py`.

Ngoài CSV, dự án có module đọc SQL Server ở `src/etl/extract/extract_db.py`. Module này dùng các biến môi trường:

```env
DB_HOST=
DB_PORT=
DB_NAME=
DB_INSTANCE=
DB_USER=
DB_PASSWORD=
```

Lưu ý: extract SQL Server hiện chưa được nối vào pipeline mặc định. Pipeline mặc định vẫn dùng CSV.

## Chạy ETL Local

Lệnh dưới đây chạy pipeline local, ghi lại dữ liệu clean và star CSV, không load lên BigQuery:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
from src.etl.pipelines.pipeline_all import run

spark = (
    SparkSession.builder
    .appName("ETL_All_Local")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    run(
        spark,
        write_csv=True,
        write_star_csv=True,
        load_to_cloud=False,
    )
finally:
    spark.stop()
PY
```

Kết quả được ghi vào:

```text
src/etl/data/clean/
src/etl/data/star/
```

Không nên chạy trực tiếp `python src/etl/pipelines/pipeline_all.py` nếu chưa cấu hình BigQuery, vì phần `__main__` hiện đang gọi `load_to_cloud=True`.

## Star Schema Đầu Ra

Pipeline tạo 9 bảng dimension:

```text
Dim_Patient
Dim_Payer
Dim_Provider
Dim_Procedure
Dim_Diagnosis
Dim_Medication
Dim_Date
Dim_Time
Dim_Encounter
```

Và 4 bảng fact:

```text
Fact_Encounter_Metrics
Fact_Procedures
Fact_Conditions
Fact_Medications
```

Ý nghĩa chính:

- `Fact_Encounter_Metrics`: chỉ số lượt khám, chi phí, tuổi bệnh nhân, readmission 30 ngày, death 30 ngày.
- `Fact_Procedures`: chi tiết thủ thuật theo encounter, patient, provider, payer.
- `Fact_Conditions`: chẩn đoán/bệnh theo encounter.
- `Fact_Medications`: thuốc, thời gian dùng thuốc và chi phí.

## Load Lên BigQuery

Tạo file `.env` ở thư mục project:

```env
PROJECT_ID=your-gcp-project
DATASET_ID=healthcare_core
GOOGLE_APPLICATION_CREDENTIALS=.credentials/gcp/bigquery-loader.sa.dev.json
```

Sau đó chạy pipeline với `load_to_cloud=True`:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
from src.etl.pipelines.pipeline_all import run

spark = (
    SparkSession.builder
    .appName("ETL_All_BigQuery")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    run(
        spark,
        write_csv=True,
        write_star_csv=True,
        load_to_cloud=True,
    )
finally:
    spark.stop()
PY
```

Module load chính là `src/etl/load/load_star_to_bigquery.py`. Module này load các CSV trong `src/etl/data/star/` lên BigQuery với schema được định nghĩa trong Python.

## Chạy DDL BigQuery

DDL nằm trong:

```text
warehouse/ddl/dimensions/
warehouse/ddl/facts/
```

Script chạy DDL:

```bash
python warehouse/scripts/main.py
```

Lưu ý: các file DDL hiện đang hard-code project/dataset trong câu `CREATE TABLE`. Nếu muốn dùng project khác, cần chỉnh lại DDL hoặc cập nhật script để thay thế project/dataset đúng cách.

## Kiểm Tra Nhanh

Kiểm tra cú pháp Python:

```bash
python -m compileall src main.py warehouse/scripts/main.py
```

Kiểm tra dữ liệu star CSV bằng Python thường:

```bash
python - <<'PY'
import csv
from pathlib import Path

for path in sorted(Path("src/etl/data/star").glob("*.csv")):
    with path.open(newline="", encoding="utf-8") as f:
        rows = sum(1 for _ in csv.DictReader(f))
    print(f"{path.name}: {rows}")
PY
```

## Ghi Chú Quan Trọng

- `README.md` này mô tả pipeline hiện tại trong `src/etl`.
- Dependency chính đã được gom về `pyproject.toml`; `requirements.txt` chỉ là file tương thích.
- Dữ liệu `procedures` sau clean có thể giảm mạnh do rule lọc theo encounter window và quan hệ patient/encounter. Khi làm báo cáo, nên giải thích rõ logic này.
- Không commit file credential thật. Thư mục `.credentials/` đã nằm trong `.gitignore`.
