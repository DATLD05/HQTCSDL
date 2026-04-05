CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_payer` (
  payer_key INT64,
  payer_id STRING,
  payer_name STRING,
  ownership STRING,
  payer_city STRING,
  payer_state STRING
)
CLUSTER BY payer_id;