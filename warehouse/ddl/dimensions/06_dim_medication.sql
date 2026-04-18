CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Dim_Medication` (
  Code STRING NOT NULL,
  Name STRING,
  Drug_Class STRING,
  Form STRING,
  Strength STRING
) OPTIONS(description="Danh mục thuốc");