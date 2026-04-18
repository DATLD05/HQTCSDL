CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Dim_Time` (
  Time_Key INT64 NOT NULL,
  Hour INT64,
  Time_Bucket STRING
) OPTIONS(description="Bảng giờ trong ngày");