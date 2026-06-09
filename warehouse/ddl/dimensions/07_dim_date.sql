CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Date` (
  Date_Key INT64 NOT NULL,
  Date DATE,
  Year INT64,
  Month INT64,
  DayOfWeek STRING
) OPTIONS(description="Bảng thời gian chuẩn (Dim Date)");
