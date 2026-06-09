CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Encounter` (
  Id STRING NOT NULL,
  EncounterClass STRING,
  Age_Group STRING,
  Duration_Bucket STRING
) OPTIONS(description="Bảng phân loại và phân nhóm lượt khám");
