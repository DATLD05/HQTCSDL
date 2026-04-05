CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dim_encounter_type` (
  encounter_type_key INT64,
  encounter_class STRING,
  encounter_description STRING
)
CLUSTER BY encounter_class;