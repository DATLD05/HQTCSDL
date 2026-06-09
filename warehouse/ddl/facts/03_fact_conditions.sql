CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Fact_Conditions` (
  Id STRING NOT NULL,
  Encounter_Id STRING,
  Condition_Code STRING,
  
  Start_Date_Key INT64,
  
  Patient_Id STRING,
  Provider_Id STRING,
  Payer_Id STRING
)
PARTITION BY RANGE_BUCKET(Start_Date_Key, GENERATE_ARRAY(20240101, 20270101, 100))
CLUSTER BY Condition_Code, Patient_Id, Provider_Id
OPTIONS(
  require_partition_filter = TRUE,
  description="Bảng Fact chi tiết chẩn đoán bệnh"
);
