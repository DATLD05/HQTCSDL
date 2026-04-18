CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Fact_Encounter_Metrics` (
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
CLUSTER BY Patient_Id, Provider_Id
OPTIONS(
  description="Bảng Fact trung tâm lưu trữ chỉ số lượt khám"
);