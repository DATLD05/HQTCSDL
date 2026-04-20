from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from src.etl.csv_utils import write_single_csv
from src.etl.paths import ETL_DIR
from src.etl.transform.transform_star_dim_date import build_dim_date
from src.etl.transform.transform_star_dim_patient import build_dim_patient
from src.etl.transform.transform_star_dim_provider import build_dim_provider
from src.etl.transform.transform_star_fact_encounter_metrics import (
    build_fact_encounter_metrics,
)

STAR_DATA_DIR = ETL_DIR / "star_data"
STAR_SUFFIX = "_star"


def _star_output_path(table_name: str, output_dir: Path | None) -> Path:
    target_dir = STAR_DATA_DIR if output_dir is None else Path(output_dir)
    return target_dir / f"{table_name}{STAR_SUFFIX}.csv"


def transform_star(
    cleaned: dict[str, DataFrame],
    *,
    write_csv: bool = False,
    output_dir: Path | None = None,
) -> dict[str, DataFrame]:
    dim_patient = build_dim_patient(cleaned["patients"])
    dim_provider = build_dim_provider(cleaned["providers"])
    dim_date = build_dim_date(cleaned["encounters"])
    fact_encounter_metrics = build_fact_encounter_metrics(
        cleaned["encounters"],
        cleaned["patients"],
    )
    out = {
        "dim_patient": dim_patient,
        "dim_provider": dim_provider,
        "dim_date": dim_date,
        "fact_encounter_metrics": fact_encounter_metrics,
    }

    if write_csv:
        for key, df in out.items():
            write_single_csv(df, _star_output_path(key, output_dir))

    return out
