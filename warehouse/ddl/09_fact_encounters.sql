CREATE TABLE IF NOT EXISTS `your_project.your_dataset.fact_encounters` (
  encounter_fact_key INT64,
  encounter_id STRING,

  patient_key INT64,
  provider_key INT64,
  payer_key INT64,
  encounter_type_key INT64,
  condition_key INT64,
  start_date_key INT64,
  end_date_key INT64,

  encounter_count INT64,
  base_encounter_cost NUMERIC,
  total_claim_cost NUMERIC,
  payer_coverage NUMERIC,

  encounter_start_timestamp TIMESTAMP,
  encounter_end_timestamp TIMESTAMP
)
PARTITION BY DATE(encounter_start_timestamp)
CLUSTER BY patient_key, provider_key, encounter_type_key;