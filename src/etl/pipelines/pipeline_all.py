from pathlib import Path

from prefect import flow, task
from pyspark.sql import DataFrame, SparkSession

from src.etl.csv_utils import write_single_csv
from src.etl.extract.extract_all import (
    RawCsvPaths,
    extract_all_csv,
    extract_all_db,
)
from src.etl.paths import (
    CONDITIONS_CSV,
    ENCOUNTERS_CSV,
    MEDICATIONS_CSV,
    OBSERVATIONS_CSV,
    PATIENTS_CSV,
    PAYERS_CSV,
    PROCEDURES_CSV,
    PROVIDERS_CSV,
    cleaned_csv_path,
)
from src.etl.clean import clean_all
from src.etl.load import load_star_to_bigquery
from src.etl.transform import transform_star


_TABLE_ORDER = (
    "patients",
    "payers",
    "providers",
    "encounters",
    "conditions",
    "observations",
    "procedures",
    "medications",
)

_CSV_BY_KEY: dict[str, Path] = {
    "patients": PATIENTS_CSV,
    "payers": PAYERS_CSV,
    "providers": PROVIDERS_CSV,
    "encounters": ENCOUNTERS_CSV,
    "conditions": CONDITIONS_CSV,
    "observations": OBSERVATIONS_CSV,
    "procedures": PROCEDURES_CSV,
    "medications": MEDICATIONS_CSV,
}


def _output_path_for_table(
    key: str,
    output_dir: Path | None,
) -> Path:
    raw = _CSV_BY_KEY[key]

    if output_dir is None:
        return cleaned_csv_path(raw)

    return (
        Path(output_dir)
        / f"{raw.stem}_cleaned"
    ).with_suffix(".csv")


def _process_etl(
    cleaned: dict[str, DataFrame],
    *,
    write_csv: bool,
    output_dir: Path | None,
    write_star_csv: bool,
    star_output_dir: Path | None,
    load_to_cloud: bool,
    bq_project_id: str | None,
    bq_dataset_id: str | None,
    bq_credentials_path: str | None,
    bq_write_disposition: str,
) -> dict[str, DataFrame]:

    transform_star(
        cleaned,
        write_csv=write_star_csv,
        output_dir=star_output_dir,
    )

    if load_to_cloud:
        load_star_to_bigquery(
            project_id=bq_project_id,
            dataset_id=bq_dataset_id,
            credentials_path=bq_credentials_path,
            star_dir=star_output_dir,
            write_disposition=bq_write_disposition,
        )

    if write_csv:
        for key in _TABLE_ORDER:
            write_single_csv(
                cleaned[key],
                _output_path_for_table(
                    key,
                    output_dir,
                ),
            )

    return cleaned


@task(
    retries=3,
    retry_delay_seconds=10,
)
def run_csv_etl_task(
    spark: SparkSession,
    *,
    raw_paths: RawCsvPaths | None = None,
    write_csv: bool = False,
    output_dir: Path | None = None,
    write_star_csv: bool = True,
    star_output_dir: Path | None = None,
    load_to_cloud: bool = False,
    bq_project_id: str | None = None,
    bq_dataset_id: str | None = None,
    bq_credentials_path: str | None = None,
    bq_write_disposition: str = "WRITE_TRUNCATE",
) -> dict[str, DataFrame]:

    raw = extract_all_csv(
        spark,
        raw_paths,
    )

    cleaned = clean_all(raw)

    return _process_etl(
        cleaned,
        write_csv=write_csv,
        output_dir=output_dir,
        write_star_csv=write_star_csv,
        star_output_dir=star_output_dir,
        load_to_cloud=load_to_cloud,
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
        bq_credentials_path=bq_credentials_path,
        bq_write_disposition=bq_write_disposition,
    )


@task(
    retries=3,
    retry_delay_seconds=10,
)
def run_db_etl_task(
    spark: SparkSession,
    *,
    write_csv: bool = False,
    output_dir: Path | None = None,
    write_star_csv: bool = True,
    star_output_dir: Path | None = None,
    load_to_cloud: bool = False,
    bq_project_id: str | None = None,
    bq_dataset_id: str | None = None,
    bq_credentials_path: str | None = None,
    bq_write_disposition: str = "WRITE_TRUNCATE",
) -> dict[str, DataFrame]:

    raw = extract_all_db(spark)

    cleaned = clean_all(raw)

    return _process_etl(
        cleaned,
        write_csv=write_csv,
        output_dir=output_dir,
        write_star_csv=write_star_csv,
        star_output_dir=star_output_dir,
        load_to_cloud=load_to_cloud,
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
        bq_credentials_path=bq_credentials_path,
        bq_write_disposition=bq_write_disposition,
    )


@flow(
    name="csv-etl-pipeline",
    log_prints=True,
)
def pipeline_csv():

    spark = (
        SparkSession.builder
        .appName("CSV_ETL")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_csv_etl_task(
            spark,
            write_csv=True,
            write_star_csv=True,
            load_to_cloud=True,
        )

        print("CSV ETL completed successfully")

    finally:
        spark.stop()


@flow(
    name="db-etl-pipeline",
    log_prints=True,
)
def pipeline_db():

    spark = (
        SparkSession.builder
        .appName("DB_ETL")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_db_etl_task(
            spark,
            write_csv=True,
            write_star_csv=True,
            load_to_cloud=True,
        )

        print("DB ETL completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    pipeline_csv()