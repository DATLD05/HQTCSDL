CREATE TABLE IF NOT EXISTS `your_project.your_dataset.fact_observations` (
  observation_fact_key INT64,

  observation_id STRING,
  encounter_id STRING,
  patient_id STRING,

  patient_key INT64,
  provider_key INT64,
  observation_key INT64,
  condition_key INT64,
  date_key INT64,

  observation_count INT64,
  observation_value_numeric NUMERIC,

  value_text STRING,
  unit_source STRING,
  interpretation STRING,

  observation_timestamp TIMESTAMP
)
PARTITION BY DATE(observation_timestamp)
CLUSTER BY patient_key, observation_key, condition_key;