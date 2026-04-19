# Huong dan su dung cac ham PySpark (chi tiet)

Tai lieu nay duoc viet cho du an nay (dang dung `pyspark>=4.1.1`), theo huong thuc hanh: moi ham co muc dich, tham so, kieu du lieu, cach goi, vi du, va loi hay gap.

---

## 1) Khoi tao SparkSession

### Ham: `SparkSession.builder...getOrCreate()`

- **Muc dich**: Tao SparkSession (diem vao chinh de lam viec voi DataFrame API).
- **Kieu tra ve**: `SparkSession`

### Cac ham/thuoc tinh builder hay dung

1. `appName(name: str)`
- **Tham so**:
  - `name` (`str`): Ten ung dung Spark hien tren UI/log.
- **Vi du**:
  - `SparkSession.builder.appName("ETL-Conditions")`

2. `master(master: str)`
- **Tham so**:
  - `master` (`str`): Che do chay (`"local[*]"`, `"local[4]"`, hoac URL cluster).

3. `config(key: str, value: str)`
- **Tham so**:
  - `key` (`str`): Ten cau hinh Spark.
  - `value` (`str`): Gia tri cau hinh.
- **Vi du**:
  - `.config("spark.sql.shuffle.partitions", "8")`

4. `getOrCreate()`
- **Khong tham so**.
- Tra ve session hien co neu da ton tai; neu chua co thi tao moi.

### Mau day du

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ETL-Conditions")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
```

---

## 2) Doc du lieu CSV

### Ham: `spark.read.csv(...)`

- **Muc dich**: Doc file CSV thanh `DataFrame`.
- **Kieu tra ve**: `DataFrame`

### Chu ky hay dung

```python
DataFrameReader.csv(
    path: str | list[str],
    schema: StructType | str | None = None,
    sep: str | None = None,
    header: bool | None = None,
    inferSchema: bool | None = None,
    nullValue: str | None = None,
    dateFormat: str | None = None,
    timestampFormat: str | None = None,
    mode: str | None = None,
    ...
)
```

### Tham so quan trong

- `path` (`str | list[str]`): Duong dan 1 file hoac nhieu file.
- `header` (`bool`): Dong dau la ten cot hay khong.
- `inferSchema` (`bool`): Spark tu suy luan kieu du lieu.
- `schema` (`StructType | str`): Tu khai bao schema (nen dung cho ETL production).
- `sep` (`str`): Ky tu tach cot, vd `","`, `";"`.
- `nullValue` (`str`): Gia tri nao se duoc coi la null.

### Vi du

```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("src/etl/extract/conditions.csv")
)
```

> Goi y: voi cot ma dinh danh nhu `CODE`, `PATIENT`, `ENCOUNTER`, nen de dang `string` de tranh mat dinh dang.

---

## 3) Xem nhanh du lieu

### `df.printSchema()`
- **Muc dich**: In cau truc schema.
- **Tra ve**: `None`

### `df.show(n: int = 20, truncate: bool | int = True, vertical: bool = False)`
- `n` (`int`): So dong hien thi.
- `truncate` (`bool | int`): Cat ngan chuoi hay khong.
- `vertical` (`bool`): In theo chieu doc.

### `df.count()`
- **Tra ve**: `int` (tong so dong).
- **Luu y**: la action, se trigger job.

---

## 4) Chon cot, doi ten cot, them cot

### `select(*cols)`
- **Tham so**: danh sach cot (`str` hoac `Column`).
- **Tra ve**: `DataFrame`

### `withColumn(colName: str, col: Column)`
- **Muc dich**: Them cot moi hoac de cot cu.
- `colName` (`str`): Ten cot.
- `col` (`Column`): Bieu thuc cot.

### `withColumnRenamed(existing: str, new: str)`
- Doi ten cot.

### Vi du

```python
from pyspark.sql import functions as F

df2 = (
    df.select("START", "STOP", "PATIENT", "CODE")
      .withColumn("start_dt", F.to_date("START", "yyyy-MM-dd"))
      .withColumn("stop_dt", F.to_date("STOP", "yyyy-MM-dd"))
)
```

---

## 5) Loc du lieu va dieu kien

### `filter(condition)` / `where(condition)`
- **Tham so**:
  - `condition` (`Column | str`): Dieu kien loc.
- **Tra ve**: `DataFrame`

### Vi du

```python
active_df = df2.filter(F.col("STOP").isNull())
chronic_df = df2.where(F.col("DESCRIPTION").contains("Chronic"))
```

### Cac ham dieu kien hay dung (`pyspark.sql.functions`)

- `col(name: str) -> Column`
- `lit(value: Any) -> Column`
- `when(condition: Column, value: Any) -> Column`
- `coalesce(*cols) -> Column`
- `isnull(col)`, `isnan(col)`

Vi du tao cot dieu kien:

```python
df3 = df2.withColumn(
    "duration_days",
    F.when(F.col("stop_dt").isNotNull(), F.datediff("stop_dt", "start_dt"))
     .otherwise(F.lit(None).cast("int"))
)
```

---

## 6) GroupBy/Aggregate

### `groupBy(*cols)`
- **Tham so**: cot nhom (`str` hoac `Column`).
- **Tra ve**: `GroupedData`

### `agg(*exprs)`
- **Tham so**: bieu thuc tong hop (`Column`).
- **Tra ve**: `DataFrame`

### Ham tong hop pho bien

- `F.count(col)` -> `Column`
- `F.countDistinct(*cols)` -> `Column`
- `F.sum(col)`, `F.avg(col)`, `F.min(col)`, `F.max(col)`

### Vi du

```python
per_patient = df3.groupBy("PATIENT").agg(
    F.count("*").alias("visit_count"),
    F.countDistinct("CODE").alias("distinct_codes"),
    F.avg("duration_days").alias("avg_condition_days"),
)
```

---

## 7) Sap xep, gioi han, lay mau

### `orderBy(*cols, ascending=True)` / `sort(...)`
- Sap xep theo cot.

### `limit(num: int)`
- Lay N dong dau.

### `sample(withReplacement: bool | None = None, fraction: float | None = None, seed: int | None = None)`
- Lay mau ngau nhien.

### Vi du

```python
top_codes = (
    df3.groupBy("CODE", "DESCRIPTION")
       .agg(F.count("*").alias("freq"))
       .orderBy(F.desc("freq"))
       .limit(50)
)
```

---

## 8) Join DataFrame

### `join(other, on=None, how=None)`

- **Tham so**:
  - `other` (`DataFrame`): Bang ben phai.
  - `on` (`str | list[str] | Column | list[Column] | None`): Dieu kien join.
  - `how` (`str | None`): Kieu join (`inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, ...).
- **Tra ve**: `DataFrame`

### Vi du

```python
enriched = df3.join(per_patient, on="PATIENT", how="inner")
```

### Tinh nang toi uu

- `F.broadcast(df_small)` de ep broadcast join khi bang phai nho.

```python
enriched = df3.join(F.broadcast(per_patient), on="PATIENT", how="inner")
```

---

## 9) Window functions

### `Window.partitionBy(...).orderBy(...)`

- **Muc dich**: Tinh toan theo cua so (theo nhom + thu tu).
- **Kieu tra ve**: `WindowSpec`

### Ham hay dung voi window

- `F.row_number()`
- `F.rank()`
- `F.dense_rank()`
- `F.lag(col, offset=1, default=None)`
- `F.lead(col, offset=1, default=None)`

### Vi du

```python
from pyspark.sql.window import Window

w = Window.partitionBy("PATIENT").orderBy("start_dt", "ENCOUNTER")
ranked = df3.withColumn("visit_order", F.row_number().over(w))
```

---

## 10) Xu ly null, ngay thang, chuoi

### Ngay thang
- `F.to_date(col, format=None)`
- `F.to_timestamp(col, format=None)`
- `F.datediff(end, start)`
- `F.date_add(col, days)` / `F.date_sub(col, days)`

### Chuoi
- `F.trim(col)`, `F.lower(col)`, `F.upper(col)`
- `F.regexp_replace(col, pattern, replacement)`
- `F.substring(col, pos, len)`

### Null
- `df.na.fill(value | dict)`
- `df.na.drop(how="any"|"all", subset=[...])`
- `df.na.replace(...)`

Vi du chuan hoa:

```python
clean = (
    df.withColumn("PATIENT", F.lower(F.trim(F.col("PATIENT"))))
      .withColumn("ENCOUNTER", F.lower(F.trim(F.col("ENCOUNTER"))))
      .withColumn("CODE", F.trim(F.col("CODE").cast("string")))
      .withColumn("DESCRIPTION", F.regexp_replace(F.trim("DESCRIPTION"), r"\\s+", " "))
)
```

---

## 11) Action vs Transformation (rat quan trong)

### Transformation (lazy)
Khong chay ngay, chi tao ke hoach:
- `select`, `filter`, `withColumn`, `join`, `groupBy().agg()`, `orderBy`, ...

### Action (trigger job)
Khi goi action Spark moi thuc thi:
- `show`, `count`, `collect`, `first`, `take`, `toPandas`, `write...`

> Neu thay code chua chay ma da chain nhieu buoc la dung binh thuong vi Spark lazy evaluation.

---

## 12) Ghi du lieu

### `df.write` (DataFrameWriter)

Cac ham pho bien:
- `mode(saveMode: str)`
  - `"overwrite"`, `"append"`, `"ignore"`, `"error"` (`"errorifexists"`)
- `format(source: str)`
- `option(key: str, value: Any)`
- `csv(path: str)`
- `parquet(path: str)`
- `json(path: str)`

### Vi du

```python
(
    enriched.write
    .mode("overwrite")
    .option("header", True)
    .csv("output/enriched_conditions")
)
```

---

## 13) Kieu du lieu PySpark SQL thuong gap

Trong `pyspark.sql.types`:
- `StringType()`
- `IntegerType()`
- `LongType()`
- `DoubleType()`
- `BooleanType()`
- `DateType()`
- `TimestampType()`
- `DecimalType(precision, scale)`
- `ArrayType(elementType)`
- `StructType([...])`
- `StructField(name, dataType, nullable=True)`

### Vi du schema tuong minh cho `conditions.csv`

```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("START", StringType(), True),
    StructField("STOP", StringType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
])
```

---

## 14) Ham dung nhieu trong ETL benh an (goi y)

1. Doc + parse date:
```python
raw = spark.read.option("header", True).schema(schema).csv("...")
df = raw.withColumn("start_dt", F.to_date("START", "yyyy-MM-dd"))
```

2. Tinh duration:
```python
df = df.withColumn(
    "duration_days",
    F.when(F.col("STOP").isNotNull(), F.datediff(F.to_date("STOP"), F.to_date("START")))
)
```

3. Top ma benh:
```python
top = df.groupBy("CODE", "DESCRIPTION").agg(F.count("*").alias("freq")).orderBy(F.desc("freq"))
```

4. Thu tu lan kham:
```python
w = Window.partitionBy("PATIENT").orderBy("start_dt", "ENCOUNTER")
df = df.withColumn("visit_order", F.row_number().over(w))
```

---

## 15) Loi hay gap va cach tranh

1. **`toPandas()` tren du lieu lon**
- Co the tran RAM.
- Cach tranh: chi `limit`, hoac giu xu ly tren Spark.

2. **Schema suy luan sai (`inferSchema`)**
- Ma dinh danh bi doi thanh so.
- Cach tranh: khai bao schema tuong minh.

3. **Null date**
- `datediff` cho ra null neu 1 trong 2 dau vao null.
- Dung `when(...).otherwise(...)` ro rang.

4. **Join bi nhan ban ghi**
- Kiem tra khoa join co duy nhat khong.
- Dung `dropDuplicates` truoc join neu can.

5. **Hieu nham lazy evaluation**
- Them `count()/show()` o moc can kiem tra de trigger va debug.

---

## 16) Mau chuong trinh ETL nho (end-to-end)

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("demo").master("local[*]").getOrCreate()

raw = spark.read.option("header", True).option("inferSchema", True).csv("src/etl/extract/conditions.csv")

df = (
    raw.withColumn("start_dt", F.to_date("START", "yyyy-MM-dd"))
       .withColumn("stop_dt", F.to_date("STOP", "yyyy-MM-dd"))
       .withColumn("duration_days", F.when(F.col("stop_dt").isNotNull(), F.datediff("stop_dt", "start_dt")))
)

per_patient = df.groupBy("PATIENT").agg(
    F.count("*").alias("visit_count"),
    F.countDistinct("CODE").alias("distinct_codes"),
)

w = Window.partitionBy("PATIENT").orderBy("start_dt", "ENCOUNTER")
ranked = df.withColumn("visit_order", F.row_number().over(w))

result = ranked.join(F.broadcast(per_patient), on="PATIENT", how="inner")
result.show(20, truncate=False)

spark.stop()
```

---

## 17) Tai lieu chinh thuc nen doc them

- PySpark API reference: https://spark.apache.org/docs/latest/api/python/reference/index.html
- SQL functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
- DataFrame API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- Window API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html

---

Neu ban muon, buoc tiep theo co the la viet them **1 file cheat sheet rieng cho du lieu benh an** (cac ham uu tien + mau query hay dung), ngan gon 1-2 trang de tra cuu nhanh khi code.
