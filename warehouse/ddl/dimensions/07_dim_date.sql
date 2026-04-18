CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Dim_Date` (
  Date_Key INT64 NOT NULL,
  Date DATE,
  Year INT64,
  Month INT64,
  DayOfWeek STRING
) OPTIONS(description="Bảng thời gian chuẩn (Dim Date)");