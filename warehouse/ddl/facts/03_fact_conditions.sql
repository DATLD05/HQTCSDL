CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Fact_Conditions` (
  Id STRING NOT NULL,
  Encounter_Id STRING,
  Condition_Code STRING,
  
  Start_Date_Key INT64,
  
  Patient_Id STRING,
  Provider_Id STRING,
  Payer_Id STRING
)
CLUSTER BY Patient_Id, Condition_Code
OPTIONS(description="Bảng Fact chi tiết chẩn đoán bệnh");
