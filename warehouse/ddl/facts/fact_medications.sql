CREATE TABLE IF NOT EXISTS `your_project.your_dataset.fact_medications` (
  medication_fact_key INT64,

  medication_event_id STRING,
  encounter_id STRING,
  patient_id STRING,

  patient_key INT64,
  provider_key INT64,
  payer_key INT64,
  medication_key INT64,
  condition_key INT64,
  start_date_key INT64,
  stop_date_key INT64,

  medication_count INT64,
  dispenses INT64,
  quantity NUMERIC,
  base_cost NUMERIC,
  total_cost NUMERIC,

  reason_description STRING,
  medication_start_timestamp TIMESTAMP,
  medication_stop_timestamp TIMESTAMP
)
PARTITION BY DATE(medication_start_timestamp)
CLUSTER BY patient_key, medication_key, condition_key;