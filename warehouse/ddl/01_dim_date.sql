CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_date` (
  date_key INT64,
  full_date DATE,
  day_of_month INT64,
  day_name STRING,
  day_of_week INT64,
  week_of_year INT64,
  month INT64,
  month_name STRING,
  quarter INT64,
  year INT64,
  is_weekend BOOL
)
CLUSTER BY year, month;