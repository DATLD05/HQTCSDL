CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Dim_Patient` (
  Id STRING NOT NULL,
  BirthDate DATE,
  DeathDate DATE,
  Gender STRING,
  Race STRING
) OPTIONS(description="Bảng thông tin bệnh nhân");