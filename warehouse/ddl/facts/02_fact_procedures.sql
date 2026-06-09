CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Fact_Procedures` (
  Id STRING NOT NULL,
  Encounter_Id STRING,
  Procedure_Code STRING,
  
  Start_Date_Key INT64,
  Start_Time_Key INT64,
  
  Patient_Id STRING,
  Provider_Id STRING,
  Payer_Id STRING,
  
  Procedure_Duration_Minutes INT64,
  Base_Cost FLOAT64,
  Unclaimed_Cost FLOAT64
)
PARTITION BY RANGE_BUCKET(Start_Date_Key, GENERATE_ARRAY(20240101, 20270101, 100))
CLUSTER BY Procedure_Code, Patient_Id, Provider_Id
OPTIONS(
  require_partition_filter = TRUE,
  description="Bảng Fact chi tiết thủ thuật y tế"
);
