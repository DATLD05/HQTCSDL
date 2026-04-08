from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _orphan_count(child: DataFrame, child_key: str, parent: DataFrame, parent_key: str) -> int:
    c = child.select(F.col(child_key).alias("_k")).where(F.col("_k").isNotNull()).distinct()
    p = parent.select(F.col(parent_key).alias("_k")).where(F.col("_k").isNotNull()).distinct()
    return c.join(p, on="_k", how="left_anti").count()


def _mismatch_patient_count(events: DataFrame, encounters: DataFrame) -> int:
    e = encounters.select(F.col("Id").alias("_enc"), F.col("PATIENT").alias("_enc_pat"))
    x = events.select("ENCOUNTER", "PATIENT").where(
        F.col("ENCOUNTER").isNotNull() & F.col("PATIENT").isNotNull()
    )
    return (
        x.join(e, x["ENCOUNTER"] == e["_enc"], "inner")
        .where(x["PATIENT"] != e["_enc_pat"])
        .count()
    )


def build_integrity_report(
    raw: dict[str, DataFrame],
    cleaned: dict[str, DataFrame],
) -> dict[str, int | float]:
    patients = cleaned["patients"]
    payers = cleaned["payers"]
    providers = cleaned["providers"]
    encounters = cleaned["encounters"]

    report: dict[str, int | float] = {}

    report["drop_rate_patients"] = 1.0 - (cleaned["patients"].count() / max(raw["patients"].count(), 1))
    report["drop_rate_payers"] = 1.0 - (cleaned["payers"].count() / max(raw["payers"].count(), 1))
    report["drop_rate_providers"] = 1.0 - (cleaned["providers"].count() / max(raw["providers"].count(), 1))
    report["drop_rate_encounters"] = 1.0 - (cleaned["encounters"].count() / max(raw["encounters"].count(), 1))
    report["drop_rate_conditions"] = 1.0 - (cleaned["conditions"].count() / max(raw["conditions"].count(), 1))
    report["drop_rate_observations"] = 1.0 - (
        cleaned["observations"].count() / max(raw["observations"].count(), 1)
    )
    report["drop_rate_procedures"] = 1.0 - (cleaned["procedures"].count() / max(raw["procedures"].count(), 1))
    report["drop_rate_medications"] = 1.0 - (
        cleaned["medications"].count() / max(raw["medications"].count(), 1)
    )

    report["orphan_encounters_patient"] = _orphan_count(encounters, "PATIENT", patients, "Id")
    report["orphan_encounters_payer"] = _orphan_count(encounters, "PAYER", payers, "Id")
    report["orphan_encounters_provider"] = _orphan_count(encounters, "PROVIDER", providers, "Id")

    report["orphan_conditions_encounter"] = _orphan_count(cleaned["conditions"], "ENCOUNTER", encounters, "Id")
    report["orphan_observations_encounter"] = _orphan_count(
        cleaned["observations"], "ENCOUNTER", encounters, "Id"
    )
    report["orphan_procedures_encounter"] = _orphan_count(cleaned["procedures"], "ENCOUNTER", encounters, "Id")
    report["orphan_medications_encounter"] = _orphan_count(cleaned["medications"], "ENCOUNTER", encounters, "Id")

    report["orphan_conditions_patient"] = _orphan_count(cleaned["conditions"], "PATIENT", patients, "Id")
    report["orphan_observations_patient"] = _orphan_count(cleaned["observations"], "PATIENT", patients, "Id")
    report["orphan_procedures_patient"] = _orphan_count(cleaned["procedures"], "PATIENT", patients, "Id")
    report["orphan_medications_patient"] = _orphan_count(cleaned["medications"], "PATIENT", patients, "Id")
    report["orphan_medications_payer"] = _orphan_count(cleaned["medications"], "PAYER", payers, "Id")

    report["mismatch_conditions_encounter_patient"] = _mismatch_patient_count(
        cleaned["conditions"], encounters
    )
    report["mismatch_observations_encounter_patient"] = _mismatch_patient_count(
        cleaned["observations"], encounters
    )
    report["mismatch_procedures_encounter_patient"] = _mismatch_patient_count(
        cleaned["procedures"], encounters
    )
    report["mismatch_medications_encounter_patient"] = _mismatch_patient_count(
        cleaned["medications"], encounters
    )

    return report


def has_integrity_violations(report: dict[str, int | float]) -> bool:
    return any(v > 0 for k, v in report.items() if k.startswith("orphan_") or k.startswith("mismatch_"))
