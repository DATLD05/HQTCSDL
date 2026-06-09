CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Fact_Medications` (
  Id STRING NOT NULL,
  Encounter_Id STRING,
  Medication_Code STRING,
  
  Patient_Id STRING,
  Provider_Id STRING,
  Payer_Id STRING,
  
  Start_Date_Key INT64,
  End_Date_Key INT64,
  
  Dosage FLOAT64,
  Frequency STRING,
  Duration_Days INT64,
  
  Base_Cost FLOAT64,
  Covered_Cost FLOAT64
)
PARTITION BY RANGE_BUCKET(Start_Date_Key, GENERATE_ARRAY(20240101, 20270101, 100))
CLUSTER BY Medication_Code, Patient_Id, Payer_Id
OPTIONS(
  require_partition_filter = TRUE,
  description="Bảng Fact chi tiết đơn thuốc"
);
