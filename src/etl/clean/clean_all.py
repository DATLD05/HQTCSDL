from __future__ import annotations

from pyspark.sql import DataFrame

from src.etl.clean.clean_conditions import clean as clean_conditions
from src.etl.clean.clean_encounters import clean as clean_encounters
from src.etl.clean.clean_medications import clean as clean_medications
from src.etl.clean.clean_observations import clean as clean_observations
from src.etl.clean.clean_patients import clean as clean_patients
from src.etl.clean.clean_payers import clean as clean_payers
from src.etl.clean.clean_procedures import clean as clean_procedures
from src.etl.clean.clean_providers import clean as clean_providers


def _print_cleaning_summary(raw: dict[str, DataFrame], cleaned: dict[str, DataFrame]) -> None:
    print("=== Cleaning Summary (before -> after) ===")
    for key in (
        "patients",
        "payers",
        "providers",
        "encounters",
        "conditions",
        "observations",
        "procedures",
        "medications",
    ):
        before = raw[key].count()
        after = cleaned[key].count()
        dropped = before - after
        dropped_pct = (dropped / before * 100.0) if before else 0.0
        print(
            f"{key}: {before} -> {after} "
            f"(dropped={dropped}, dropped_pct={dropped_pct:.2f}%)"
        )


def clean_all(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """
    Run the full cleaning graph. Order: dimensions → encounters → clinical facts.
    """
    patients = clean_patients(raw["patients"])
    payers = clean_payers(raw["payers"])
    providers = clean_providers(raw["providers"])
    encounters = clean_encounters(raw["encounters"], patients, payers, providers)
    conditions = clean_conditions(raw["conditions"], encounters, patients)
    observations = clean_observations(raw["observations"], encounters, patients)
    procedures = clean_procedures(raw["procedures"], encounters, patients)
    medications = clean_medications(
        raw["medications"], encounters, patients, payers
    )
    cleaned = {
        "patients": patients,
        "payers": payers,
        "providers": providers,
        "encounters": encounters,
        "conditions": conditions,
        "observations": observations,
        "procedures": procedures,
        "medications": medications,
    }
    _print_cleaning_summary(raw, cleaned)
    return cleaned

