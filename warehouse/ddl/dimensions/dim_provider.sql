CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_provider` (
  provider_key INT64,
  provider_id STRING,
  provider_name STRING,
  specialty STRING,
  gender STRING,
  city STRING,
  state STRING,
  organization STRING
)
CLUSTER BY provider_id;