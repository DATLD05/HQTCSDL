from google.cloud import bigquery
from google.oauth2 import service_account
import glob
from dotenv import load_dotenv
import os

load_dotenv()

# ===== CONFIG =====
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
CREDENTIAL_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


# ===== INIT CLIENT =====
def get_client():
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIAL_PATH
    )
    return bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )


# ===== CREATE DATASET IF NOT EXISTS =====
def create_dataset_if_not_exists(client):
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{DATASET_ID}' already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"

        client.create_dataset(dataset)
        print(f"Created dataset '{DATASET_ID}'")


# ===== RUN 1 Table =====
def run_ddl_file(client, file_path):
    print(f"\n⏳ Running: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()

        # replace project placeholder
        sql = sql.replace("your_project", PROJECT_ID)

        job = client.query(sql)
        job.result()

        print(f"Success: {os.path.basename(file_path)}")

    except Exception as e:
        print(f"Failed: {file_path}")
        print(f"Error: {e}")
        raise e  # dừng luôn nếu lỗi


# ===== RUN ALL =====
def run_all_ddl():
    print("START RUNNING ALL DDL")

    client = get_client()

    # 1. create dataset
    create_dataset_if_not_exists(client)

    # ===== DIMENSIONS =====
    dim_files = sorted(glob.glob("HQTCSDL/warehouse/ddl/dimensions/*.sql"))

    print("\nRUNNING DIMENSIONS...")
    for file in dim_files:
        run_ddl_file(client, file)

    # ===== FACTS =====
    fact_files = sorted(glob.glob("HQTCSDL/warehouse/ddl/facts/*.sql"))

    print("\nRUNNING FACTS...")
    for file in fact_files:
        run_ddl_file(client, file)

    print("\nALL DONE!")


# ===== MAIN =====
if __name__ == "__main__":
    run_all_ddl()