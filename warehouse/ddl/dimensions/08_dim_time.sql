CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Time` (
  Time_Key INT64 NOT NULL,
  Hour INT64,
  Time_Bucket STRING
) OPTIONS(description="Bảng giờ trong ngày");
