import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
DDL_DIR = WAREHOUSE_DIR / "ddl"

load_dotenv(PROJECT_ROOT / ".env")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


PROJECT_ID = required_env("PROJECT_ID")
DATASET_ID = required_env("DATASET_ID")
CREDENTIAL_PATH = required_env("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")


def get_client():
    credentials_path = Path(CREDENTIAL_PATH)
    if not credentials_path.is_absolute():
        credentials_path = PROJECT_ROOT / credentials_path
    if not credentials_path.exists():
        raise RuntimeError(
            "Missing service-account key file. "
            "Set GOOGLE_APPLICATION_CREDENTIALS in .env."
        )

    credentials = service_account.Credentials.from_service_account_file(
        str(credentials_path)
    )
    return bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )


def create_dataset_if_not_exists(client):
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{DATASET_ID}' already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BIGQUERY_LOCATION

        client.create_dataset(dataset)
        print(f"Created dataset '{DATASET_ID}' in location '{BIGQUERY_LOCATION}'")


def render_sql_template(sql: str) -> str:
    return (
        sql.replace("{PROJECT_ID}", PROJECT_ID)
        .replace("{DATASET_ID}", DATASET_ID)
    )


def run_ddl_file(client, file_path: Path):
    print(f"\nRunning: {file_path}")

    try:
        sql = render_sql_template(file_path.read_text(encoding="utf-8"))

        job = client.query(sql)
        job.result()

        print(f"Success: {file_path.name}")

    except Exception as e:
        print(f"Failed: {file_path}")
        print(f"Error: {e}")
        raise


def run_all_ddl():
    print("START RUNNING ALL DDL")
    print(f"project={PROJECT_ID}, dataset={DATASET_ID}")

    client = get_client()
    create_dataset_if_not_exists(client)

    dim_files = sorted((DDL_DIR / "dimensions").glob("*.sql"))

    print("\nRUNNING DIMENSIONS...")
    for file in dim_files:
        run_ddl_file(client, file)

    fact_files = sorted((DDL_DIR / "facts").glob("*.sql"))

    print("\nRUNNING FACTS...")
    for file in fact_files:
        run_ddl_file(client, file)

    print("\nALL DONE!")


if __name__ == "__main__":
    run_all_ddl()
