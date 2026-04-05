CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_condition` (
  condition_key INT64,
  condition_code STRING,
  condition_description STRING,
  condition_category STRING
)
CLUSTER BY condition_code;