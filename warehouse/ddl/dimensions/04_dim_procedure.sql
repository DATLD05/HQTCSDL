CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.Dim_Procedure` (
  Code STRING NOT NULL,
  Description STRING,
  Procedure_Category STRING,
  Is_Top_Pareto BOOL
) OPTIONS(description="Danh mục các thủ thuật y tế");
