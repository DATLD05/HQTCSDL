from __future__ import annotations

from pyspark.sql import DataFrame

from src.etl.transform.transform_conditions import transform as transform_conditions
from src.etl.transform.transform_encounters import transform as transform_encounters
from src.etl.transform.transform_medications import transform as transform_medications
from src.etl.transform.transform_observations import transform as transform_observations
from src.etl.transform.transform_patients import transform as transform_patients
from src.etl.transform.transform_payers import transform as transform_payers
from src.etl.transform.transform_procedures import transform as transform_procedures
from src.etl.transform.transform_providers import transform as transform_providers


def transform_all(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """
    Run the full cleaning graph. Order: dimensions → encounters → clinical facts.
    """
    patients = transform_patients(raw["patients"])
    payers = transform_payers(raw["payers"])
    providers = transform_providers(raw["providers"])
    encounters = transform_encounters(raw["encounters"], patients, payers, providers)
    conditions = transform_conditions(raw["conditions"], encounters, patients)
    observations = transform_observations(raw["observations"], encounters, patients)
    procedures = transform_procedures(raw["procedures"], encounters, patients)
    medications = transform_medications(
        raw["medications"], encounters, patients, payers
    )
    return {
        "patients": patients,
        "payers": payers,
        "providers": providers,
        "encounters": encounters,
        "conditions": conditions,
        "observations": observations,
        "procedures": procedures,
        "medications": medications,
    }
