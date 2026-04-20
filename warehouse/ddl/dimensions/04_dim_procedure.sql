CREATE TABLE IF NOT EXISTS `project-0bccb5ce-036b-493e-9c0.healthcare_core.Dim_Procedure` (
  Code STRING NOT NULL,
  Description STRING,
  Procedure_Category STRING,
  Is_Top_Pareto BOOL
) OPTIONS(description="Danh mục các thủ thuật y tế");