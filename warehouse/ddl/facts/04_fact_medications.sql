CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Fact_Medications` (
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
CLUSTER BY Patient_Id, Medication_Code
OPTIONS(description="Bảng Fact chi tiết đơn thuốc");
