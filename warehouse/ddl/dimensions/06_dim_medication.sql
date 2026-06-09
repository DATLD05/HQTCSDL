CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Medication` (
  Code STRING NOT NULL,
  Name STRING,
  Drug_Class STRING,
  Form STRING,
  Strength STRING
) OPTIONS(description="Danh mục thuốc");
