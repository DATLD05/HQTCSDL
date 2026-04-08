"""Shared CSV I/O and small Spark column helpers for ETL."""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

PERMISSIVE_CSV_OPTS = {
    "header": True,
    "inferSchema": True,
    "mode": "PERMISSIVE",
    "multiLine": False,
    "escape": '"',
    "quote": '"',
}


def normalize_column_names(df: DataFrame) -> DataFrame:
    out = df
    for name in df.columns:
        stripped = name.strip()
        if name != stripped:
            out = out.withColumnRenamed(name, stripped)
    return out


def read_csv(spark: SparkSession, path: Path | str) -> DataFrame:
    df = spark.read.options(**PERMISSIVE_CSV_OPTS).csv(str(path))
    return normalize_column_names(df)


def write_single_csv(df: DataFrame, path: Path, *, empty_value: str = "") -> None:
    """
    Write a Spark DataFrame to one CSV file (not a directory of parts).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = path.with_suffix(path.suffix + ".writing")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)

    df.coalesce(1).write.mode("overwrite").option("header", True).option(
        "emptyValue", empty_value
    ).csv(str(tmp_dir))

    parts = sorted(tmp_dir.glob("part-*.csv"))
    if not parts:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError(f"No part file written under {tmp_dir}")
    shutil.move(parts[0], path)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def trim_empty_to_null(df: DataFrame, columns: list[str]) -> DataFrame:
    out = df
    for name in columns:
        if name in out.columns:
            out = out.withColumn(
                name, F.nullif(F.trim(F.col(name).cast("string")), F.lit(""))
            )
    return out


def collapse_whitespace(col: F.Column) -> F.Column:
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def normalize_key_uuid(column: str) -> F.Column:
    """Trim; empty -> NULL. UUIDs kept lowercase for stable joins."""
    c = F.trim(F.col(column).cast("string"))
    return F.when(c.isNull() | (c == ""), F.lit(None)).otherwise(F.lower(c))


def parse_ts_any(s_col: F.Column) -> F.Column:
    """
    Parse timestamp from ISO / common CSV date-time strings.
    Coalesce multiple attempts; invalid -> NULL.
    """
    c = F.trim(s_col.cast("string"))
    c = F.when(c == "", F.lit(None)).otherwise(c)

    # Spark ANSI-safe parsing: try_* returns NULL instead of throwing.
    iso = F.try_to_timestamp(c)
    iso_z = F.try_to_timestamp(c, F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'"))
    iso_frac = F.try_to_timestamp(c, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    us = F.try_to_timestamp(c, F.lit("M/d/yyyy HH:mm:ss"))
    us_date = F.when(
        c.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
        F.try_to_timestamp(F.concat(c, F.lit(" 00:00:00")), F.lit("M/d/yyyy HH:mm:ss")),
    )
    d_only = F.when(
        c.rlike(r"^\d{4}-\d{2}-\d{2}$"),
        F.try_to_timestamp(F.concat(c, F.lit(" 00:00:00")), F.lit("yyyy-MM-dd HH:mm:ss")),
    )
    return F.coalesce(iso, iso_z, iso_frac, us, us_date, d_only)


def parse_date_any(s_col: F.Column) -> F.Column:
    """Parse date-only (conditions) from M/d/yyyy or yyyy-MM-dd."""
    c = F.trim(s_col.cast("string"))
    d1 = F.to_date(c, "M/d/yyyy")
    d2 = F.to_date(c, "MM/dd/yyyy")
    d3 = F.to_date(c, "yyyy-MM-dd")
    return F.coalesce(d1, d2, d3)


def normalize_long_code(column: str) -> F.Column:
    """
    Codes that may arrive as double / scientific notation → integer string.
    """
    c = F.col(column)
    num = F.when(
        c.cast("string").rlike("^[+-]?[0-9]*\\.?[0-9]+([eE][+-]?[0-9]+)?$"),
        F.format_string("%.0f", c.cast("double")),
    ).otherwise(F.trim(c.cast("string")))
    return F.when(c.isNull(), F.lit(None)).otherwise(num)


def fix_stop_after_start(start_col: str, stop_col: str) -> tuple[F.Column, F.Column]:
    """
    If STOP < START, set STOP to NULL (keep START), per plan safety rule.
    Applies to timestamp columns.
    """
    s = F.col(start_col)
    e = F.col(stop_col)
    ok = e.isNull() | (e >= s)
    fixed_stop = F.when(ok, e).otherwise(F.lit(None).cast(T.TimestampType()))
    return s, fixed_stop


def lowercase_categorical(col_name: str) -> F.Column:
    return F.lower(F.trim(F.col(col_name).cast("string")))
