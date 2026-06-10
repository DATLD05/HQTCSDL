

from pathlib import Path

from pyspark.sql import DataFrame

from src.etl.csv_utils import write_single_csv
from src.etl.paths import STAR_DATA_DIR
from src.etl.transform.dim_date import build_dim_date
from src.etl.transform.dim_diagnosis import build_dim_diagnosis
from src.etl.transform.dim_encounter import build_dim_encounter
from src.etl.transform.dim_medication import build_dim_medication
from src.etl.transform.dim_patient import build_dim_patient
from src.etl.transform.dim_payer import build_dim_payer
from src.etl.transform.dim_procedure import build_dim_procedure
from src.etl.transform.dim_provider import build_dim_provider
from src.etl.transform.dim_time import build_dim_time
from src.etl.transform.fact_conditions import build_fact_conditions
from src.etl.transform.fact_encounter_metrics import build_fact_encounter_metrics
from src.etl.transform.fact_medications import build_fact_medications
from src.etl.transform.fact_procedures import build_fact_procedures


def _star_output_path(table_name: str, output_dir: Path | None) -> Path:
    base = STAR_DATA_DIR if output_dir is None else Path(output_dir)
    return base / f"{table_name}.csv"


def transform_star(
    cleaned: dict[str, DataFrame],
    *,
    write_csv: bool = False,
    output_dir: Path | None = None,
) -> dict[str, DataFrame]:
    dim_patient = build_dim_patient(cleaned["patients"])
    dim_payer = build_dim_payer(cleaned["payers"])
    dim_provider = build_dim_provider(cleaned["providers"])
    dim_procedure = build_dim_procedure(cleaned["procedures"])
    dim_diagnosis = build_dim_diagnosis(cleaned["conditions"])
    dim_medication = build_dim_medication(cleaned["medications"])
    dim_date = build_dim_date(cleaned)
    dim_time = build_dim_time(cleaned["encounters"])
    dim_encounter = build_dim_encounter(cleaned["encounters"], cleaned["patients"])

    fact_encounter_metrics = build_fact_encounter_metrics(cleaned["encounters"], cleaned["patients"])
    fact_procedures = build_fact_procedures(cleaned["procedures"], cleaned["encounters"])
    fact_conditions = build_fact_conditions(cleaned["conditions"], cleaned["encounters"])
    fact_medications = build_fact_medications(cleaned["medications"], cleaned["encounters"])

    out = {
        "Dim_Patient": dim_patient,
        "Dim_Payer": dim_payer,
        "Dim_Provider": dim_provider,
        "Dim_Procedure": dim_procedure,
        "Dim_Diagnosis": dim_diagnosis,
        "Dim_Medication": dim_medication,
        "Dim_Date": dim_date,
        "Dim_Time": dim_time,
        "Dim_Encounter": dim_encounter,
        "Fact_Encounter_Metrics": fact_encounter_metrics,
        "Fact_Procedures": fact_procedures,
        "Fact_Conditions": fact_conditions,
        "Fact_Medications": fact_medications,
    }

    if write_csv:
        for key, df in out.items():
            write_single_csv(df, _star_output_path(key, output_dir))

    return out

