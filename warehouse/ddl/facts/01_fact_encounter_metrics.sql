CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Fact_Encounter_Metrics` (
  Id STRING NOT NULL,
  Encounter_Id STRING,
  Patient_Id STRING,
  Provider_Id STRING,
  Payer_Id STRING,
  
  Start_Date_Key INT64,
  Start_Time_Key INT64,
  Stop_Date_Key INT64,
  Stop_Time_Key INT64,
  
  Patient_Age INT64,
  Duration_Minutes INT64,
  Length_Of_Stay_Days INT64,
  
  Base_Encounter_Cost FLOAT64,
  Total_Claim_Cost FLOAT64,
  Payer_Coverage FLOAT64,
  Out_Of_Pocket_Cost FLOAT64,

  Is_Admitted INT64,
  Is_Readmission_30D INT64,
  Is_Death_30D INT64
)
PARTITION BY RANGE_BUCKET(Start_Date_Key, GENERATE_ARRAY(20240101, 20270101, 100))
CLUSTER BY Patient_Id, Provider_Id, Payer_Id
OPTIONS(
  require_partition_filter = TRUE,
  description="Bảng Fact trung tâm lưu trữ chỉ số lượt khám"
);
