from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

from src.etl.paths import STAR_DATA_DIR

load_dotenv()

_STAR_TABLES = (
    "Dim_Patient",
    "Dim_Payer",
    "Dim_Provider",
    "Dim_Procedure",
    "Dim_Diagnosis",
    "Dim_Medication",
    "Dim_Date",
    "Dim_Time",
    "Dim_Encounter",
    "Fact_Encounter_Metrics",
    "Fact_Procedures",
    "Fact_Conditions",
    "Fact_Medications",
)


def _env_or(value: str | None, env_name: str) -> str:
    out = value or os.getenv(env_name)
    if not out:
        raise RuntimeError(f"Missing required config: {env_name}")
    return out


def _resolve_credentials_path(credentials_path: str | None) -> str:
    candidate = (
        credentials_path
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or str(Path(".credentials/gcp/bigquery-loader.sa.dev.json").resolve())
    )
    if not Path(candidate).exists():
        raise RuntimeError(
            "Missing service-account key file. Set GOOGLE_APPLICATION_CREDENTIALS "
            "or place key at .credentials/gcp/bigquery-loader.sa.dev.json"
        )
    return candidate


def _project_id_from_service_account(credentials_path: str) -> str | None:
    try:
        with open(credentials_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        project_id = data.get("project_id")
        return project_id if isinstance(project_id, str) and project_id else None
    except Exception:
        return None


def _default_dataset_id(project_id: str) -> str:
    normalized = project_id.replace("-", "_")
    return f"{normalized}_dw"


def _ensure_dataset_exists(client: bigquery.Client, project_id: str, dataset_id: str) -> None:
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_ref}")


def _get_client(*, project_id: str, credentials_path: str) -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    return bigquery.Client(credentials=credentials, project=project_id)


def _csv_path_for_table(star_dir: Path, table_name: str) -> Path:
    return star_dir / f"{table_name}.csv"


def run(
    *,
    project_id: str | None = None,
    dataset_id: str | None = None,
    credentials_path: str | None = None,
    star_dir: Path | None = None,
    write_disposition: str = "WRITE_TRUNCATE",
) -> dict[str, int]:
    """
    Load star CSV files to BigQuery tables and return table row counts.
    """
    credentials_path = _resolve_credentials_path(credentials_path)
    project_id = (
        project_id
        or os.getenv("PROJECT_ID")
        or _project_id_from_service_account(credentials_path)
    )
    if not project_id:
        raise RuntimeError(
            "Missing required config: PROJECT_ID. "
            "Set PROJECT_ID env or include project_id in service-account file."
        )
    dataset_id = dataset_id or os.getenv("DATASET_ID") or _default_dataset_id(project_id)
    star_dir = star_dir or STAR_DATA_DIR

    client = _get_client(project_id=project_id, credentials_path=credentials_path)
    _ensure_dataset_exists(client, project_id, dataset_id)
    out: dict[str, int] = {}

    print("=== BigQuery Load (star) ===")
    print(f"project={project_id}, dataset={dataset_id}, star_dir={star_dir}")

    for table_name in _STAR_TABLES:
        csv_path = _csv_path_for_table(star_dir, table_name)
        if not csv_path.exists():
            raise RuntimeError(f"Missing star CSV: {csv_path}")

        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table_exists = True
        try:
            client.get_table(table_id)
        except NotFound:
            table_exists = False

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=write_disposition,
            autodetect=not table_exists,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        with open(csv_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

        table = client.get_table(table_id)
        out[table_name] = int(table.num_rows)
        print(f"Loaded {table_name} from {csv_path.name} -> rows={table.num_rows}")

    return out

