CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_medication` (
  medication_key INT64,
  medication_code STRING,
  medication_name STRING,
  medication_category STRING,
  base_cost NUMERIC,
  generic_name STRING,
  route STRING
)
CLUSTER BY medication_code;