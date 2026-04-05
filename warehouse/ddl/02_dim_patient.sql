CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_patient` (
  patient_key INT64,
  patient_id STRING,
  first_name STRING,
  last_name STRING,
  birth_date DATE,
  death_date DATE,
  gender STRING,
  race STRING,
  ethnicity STRING,
  marital_status STRING,
  city STRING,
  state STRING,
  county STRING,
  zip_code STRING,
  age_group STRING
)
CLUSTER BY patient_id;