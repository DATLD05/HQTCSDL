# HQTCSDL - Healthcare ETL Data Warehouse

Dự án này xây dựng pipeline ETL cho dữ liệu hồ sơ bệnh án/bệnh viện. Pipeline đọc dữ liệu nguồn, làm sạch bằng PySpark, chuyển đổi sang mô hình star schema, sau đó nạp dữ liệu lên Google BigQuery.

Nguyên tắc thiết kế hiện tại:

```text
warehouse/ddl  -> tạo dataset và bảng trên BigQuery
src/etl/load   -> chỉ nạp dữ liệu CSV vào bảng đã tồn tại
```

Vì vậy, phần `load` không còn chịu trách nhiệm tạo bảng. Nếu dataset hoặc table chưa tồn tại trên BigQuery, bước load sẽ báo lỗi và yêu cầu chạy DDL trong `warehouse` trước.

## Luồng Xử Lý

```text
raw CSV / SQL Server
-> extract
-> clean
-> transform star schema
-> ghi star CSV
-> warehouse tạo bảng BigQuery
-> load star CSV vào bảng BigQuery đã có sẵn
```

Thứ tự chạy khuyến nghị:

```text
1. Cài môi trường Python
2. Cấu hình .env và credential Google Cloud
3. Chạy warehouse/scripts/main.py để tạo dataset và bảng BigQuery
4. Chạy ETL để tạo dữ liệu star CSV
5. Chạy load để nạp CSV vào các bảng đã tạo
```

## Mục Tiêu

- Chuẩn hóa dữ liệu y tế từ nhiều bảng nguồn.
- Làm sạch dữ liệu bằng PySpark.
- Tạo dữ liệu đầu ra theo mô hình star schema.
- Tạo bảng BigQuery bằng SQL DDL trong thư mục `warehouse`.
- Nạp dữ liệu star CSV lên BigQuery sau khi bảng đã được tạo.
- Tách rõ phần thiết kế kho dữ liệu và phần nạp dữ liệu.

## Cấu Trúc Dự Án

```text
.
├── main.py                         # Script kiểm tra Spark đơn giản
├── pyproject.toml                  # Cấu hình project và dependency chính
├── requirements.txt                # File tương thích, trỏ về pyproject.toml
├── .env.example                    # Mẫu biến môi trường
├── src/etl/
│   ├── paths.py                    # Đường dẫn dữ liệu raw/clean/star
│   ├── csv_utils.py                # Helper đọc/ghi CSV
│   ├── extract/                    # Đọc dữ liệu từ CSV hoặc SQL Server
│   ├── clean/                      # Làm sạch dữ liệu
│   ├── transform/                  # Tạo star schema
│   ├── load/                       # Chỉ load star CSV lên BigQuery
│   ├── pipelines/pipeline_all.py   # Pipeline ETL end-to-end
│   ├── integrity_audit.py          # Helper audit ràng buộc dữ liệu
│   ├── data/raw/                   # Dữ liệu nguồn
│   ├── data/clean/                 # Dữ liệu sau clean
│   └── data/star/                  # Dữ liệu star schema
└── warehouse/
    ├── ddl/
    │   ├── dimensions/             # SQL tạo bảng dimension
    │   └── facts/                  # SQL tạo bảng fact
    └── scripts/
        └── main.py                 # Script chạy toàn bộ DDL lên BigQuery
```

## Cấu Trúc Warehouse

```text
warehouse/
├── ddl/
│   ├── dimensions/
│   │   ├── 01_dim_patient.sql
│   │   ├── 02_dim_payer.sql
│   │   ├── 03_dim_provider.sql
│   │   ├── 04_dim_procedure.sql
│   │   ├── 05_dim_diagnosis.sql
│   │   ├── 06_dim_medication.sql
│   │   ├── 07_dim_date.sql
│   │   ├── 08_dim_time.sql
│   │   └── 09_dim_encounter.sql
│   └── facts/
│       ├── 01_fact_encounter_metrics.sql
│       ├── 02_fact_procedures.sql
│       ├── 03_fact_conditions.sql
│       └── 04_fact_medications.sql
└── scripts/
    └── main.py
```

Vai trò của `warehouse`:

- Chứa toàn bộ SQL DDL tạo bảng BigQuery.
- Tạo các bảng dimension và fact theo đúng schema.
- Cấu hình partitioning và clustering cho bảng fact.
- Là nơi quản lý cấu trúc kho dữ liệu.

Vai trò của `src/etl/load`:

- Đọc các file CSV trong `src/etl/data/star/`.
- Kiểm tra dataset và table đã tồn tại trên BigQuery.
- Load dữ liệu vào bảng có sẵn.
- Không tạo dataset.
- Không tạo table.
- Không định nghĩa schema bảng BigQuery.

## Yêu Cầu Môi Trường

Dự án yêu cầu Python `>=3.10,<3.12`. Nên dùng Python 3.10 hoặc 3.11.

PySpark cần Java. Kiểm tra nhanh:

```bash
java -version
python --version
```

## Cài Đặt

Từ thư mục chứa project:

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

Toàn bộ dependency chính nằm trong `pyproject.toml`. File `requirements.txt` chỉ giữ để tương thích với workflow cũ.

Nếu vẫn muốn cài bằng `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Cấu Hình BigQuery

Tạo file `.env` từ file mẫu:

```bash
cp .env.example .env
```

Nội dung `.env` cần có:

```env
PROJECT_ID=your-gcp-project-id
DATASET_ID=healthcare_core
BIGQUERY_LOCATION=US
GOOGLE_APPLICATION_CREDENTIALS=.credentials/gcp/bigquery-loader.sa.dev.json
```

Ý nghĩa:

- `PROJECT_ID`: ID project trên Google Cloud.
- `DATASET_ID`: tên dataset BigQuery.
- `BIGQUERY_LOCATION`: vùng tạo dataset, ví dụ `US`.
- `GOOGLE_APPLICATION_CREDENTIALS`: đường dẫn tới file JSON service account.

Không commit file `.env` hoặc credential thật lên GitHub.

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

Các file này được đọc qua:

```text
src/etl/extract/extract_all.py
```

Dự án cũng có module đọc SQL Server:

```text
src/etl/extract/extract_db.py
```

Module SQL Server dùng các biến môi trường:

```env
DB_HOST=
DB_PORT=
DB_NAME=
DB_INSTANCE=
DB_USER=
DB_PASSWORD=
```

Hiện tại pipeline mặc định vẫn dùng CSV.

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

Các file CSV tương ứng được ghi vào:

```text
src/etl/data/star/
```

## Bước 1: Tạo Bảng BigQuery Bằng Warehouse

Chạy script DDL:

```bash
python warehouse/scripts/main.py
```

Script này sẽ:

- Đọc biến môi trường từ `.env`.
- Kết nối BigQuery bằng service account.
- Tạo dataset nếu chưa có.
- Chạy DDL trong `warehouse/ddl/dimensions/`.
- Chạy DDL trong `warehouse/ddl/facts/`.

Các file DDL dùng placeholder:

```text
{PROJECT_ID}
{DATASET_ID}
```

Khi chạy, `warehouse/scripts/main.py` sẽ tự thay bằng giá trị trong `.env`.

Lưu ý: DDL đang dùng `CREATE TABLE IF NOT EXISTS`. Nếu bảng đã tồn tại, BigQuery sẽ không tự thay đổi schema cũ. Nếu muốn đổi cấu trúc bảng đã có, cần dùng `ALTER TABLE`, `CREATE OR REPLACE TABLE`, hoặc xóa bảng rồi tạo lại tùy yêu cầu.

## Bước 2: Chạy ETL Local Để Tạo Star CSV

Lệnh dưới đây chạy ETL local, ghi dữ liệu clean và star CSV, chưa load lên BigQuery:

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

Kết quả:

```text
src/etl/data/clean/
src/etl/data/star/
```

## Bước 3: Load Dữ Liệu Lên BigQuery

Sau khi đã tạo bảng bằng `warehouse`, có thể load dữ liệu bằng pipeline:

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

Hoặc nếu đã có sẵn CSV trong `src/etl/data/star/`, chỉ chạy riêng phần load:

```bash
python - <<'PY'
from src.etl.load import load_star_to_bigquery

load_star_to_bigquery()
PY
```

Phần load hiện tại:

- Không tạo dataset.
- Không tạo table.
- Không dùng schema Python để tạo bảng.
- Chỉ nạp CSV vào bảng đã tồn tại.
- Dùng schema thật của bảng BigQuery đã tạo bởi `warehouse/ddl`.

Mặc định load dùng:

```text
WRITE_TRUNCATE
```

Nghĩa là mỗi lần load sẽ ghi đè dữ liệu trong bảng đích. Nếu muốn append dữ liệu:

```bash
python - <<'PY'
from src.etl.load import load_star_to_bigquery

load_star_to_bigquery(write_disposition="WRITE_APPEND")
PY
```

## Load Và Warehouse Khác Nhau Như Thế Nào

```text
warehouse/ddl
```

- Là nơi thiết kế cấu trúc bảng.
- Có SQL `CREATE TABLE`.
- Có kiểu dữ liệu, ràng buộc `NOT NULL`, description.
- Có partitioning và clustering cho bảng fact.
- Chạy trước khi load dữ liệu.

```text
src/etl/load
```

- Là nơi nạp dữ liệu.
- Không chứa logic tạo bảng.
- Không quyết định schema bảng.
- Không tạo bảng nếu bảng chưa có.
- Nếu thiếu bảng, chương trình báo lỗi.

## Tối Ưu Chi Phí BigQuery

Các bảng fact trong `warehouse/ddl/facts/` đã được thiết kế để giảm chi phí query:

- Partition theo `Start_Date_Key`.
- Cluster theo các cột thường dùng để lọc hoặc join như `Patient_Id`, `Provider_Id`, `Payer_Id`, `Procedure_Code`, `Condition_Code`, `Medication_Code`.
- Bật `require_partition_filter = TRUE` để hạn chế query quét toàn bộ bảng fact.

Khi query bảng fact, nên luôn lọc theo khoảng thời gian:

```sql
WHERE Start_Date_Key BETWEEN 20240101 AND 20241231
```

Nếu không có điều kiện partition, BigQuery có thể từ chối query do bảng fact đang bật `require_partition_filter`.

## Kiểm Tra Nhanh

Kiểm tra cú pháp Python:

```bash
python -m compileall src main.py warehouse/scripts/main.py
```

Kiểm tra số dòng star CSV:

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

## Lỗi Thường Gặp

Nếu load báo lỗi dataset không tồn tại:

```text
BigQuery dataset does not exist
```

Hãy chạy:

```bash
python warehouse/scripts/main.py
```

Nếu load báo lỗi table không tồn tại:

```text
BigQuery table does not exist
```

Nghĩa là bảng chưa được tạo trong BigQuery. Cần kiểm tra lại file DDL trong `warehouse/ddl` và chạy lại script warehouse.

Nếu pipeline chạy trực tiếp bằng lệnh dưới đây:

```bash
python src/etl/pipelines/pipeline_all.py
```

thì phần `__main__` đang gọi `load_to_cloud=True`. Vì vậy cần cấu hình BigQuery và tạo bảng trước, hoặc nên dùng lệnh Python ở trên để chủ động đặt `load_to_cloud=False` khi chỉ muốn chạy local.

## Ghi Chú Quan Trọng

- `warehouse` là nơi tạo bảng BigQuery.
- `load` chỉ nạp dữ liệu vào bảng có sẵn.
- Muốn thay đổi schema BigQuery thì sửa SQL trong `warehouse/ddl`.
- Không commit credential thật.
- `.env` là file cấu hình local, không nên push lên GitHub.
- `.env.example` là file mẫu an toàn để push.
