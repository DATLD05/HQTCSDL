CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Diagnosis` (
  Code STRING NOT NULL,
  Description STRING,
  Diagnosis_Group STRING
) OPTIONS(description="Danh mục chẩn đoán bệnh");
