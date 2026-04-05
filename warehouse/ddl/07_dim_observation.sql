CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_observation` (
  observation_key INT64,
  observation_code STRING,
  observation_description STRING,
  observation_category STRING,
  default_unit STRING,
  value_type STRING
)
CLUSTER BY observation_code;