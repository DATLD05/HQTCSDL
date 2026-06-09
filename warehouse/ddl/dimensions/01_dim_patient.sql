CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Patient` (
  Id STRING NOT NULL,
  BirthDate DATE,
  DeathDate DATE,
  Gender STRING,
  Race STRING
) OPTIONS(description="Bảng thông tin bệnh nhân");
