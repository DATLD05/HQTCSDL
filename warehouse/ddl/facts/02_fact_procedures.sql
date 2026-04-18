CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Fact_Procedures` (
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
CLUSTER BY Patient_Id, Procedure_Code
OPTIONS(
  description="Bảng Fact chi tiết thủ thuật y tế"
);
