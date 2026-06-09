CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Provider` (
  Id STRING NOT NULL,
  Name STRING,
  Speciality STRING
) OPTIONS(description="Bảng thông tin bác sĩ / cơ sở y tế");
