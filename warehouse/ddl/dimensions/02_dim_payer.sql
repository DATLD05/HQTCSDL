CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Payer` (
  Id STRING NOT NULL,
  Name STRING,
  State_Headquartered STRING
) OPTIONS(description="Bảng thông tin tổ chức thanh toán / Bảo hiểm");
