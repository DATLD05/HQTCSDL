CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Dim_Encounter` (
  Id STRING NOT NULL,
  EncounterClass STRING,
  Age_Group STRING,
  Duration_Bucket STRING
) OPTIONS(description="Bảng phân loại và phân nhóm lượt khám");