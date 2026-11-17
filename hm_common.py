#!/usr/bin/env python3
"""Shared configuration, schema constants, and helpers for the H&M pipeline.

The Bronze, Silver, and Gold layers live in hm_bronze.py, hm_silver.py, and
hm_gold.py. They communicate exclusively through the Parquet files persisted
under one shared output directory, described by :class:`PipelineContext`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    import findspark

    findspark.init()
except ImportError:
    pass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger("hm_medallion_pipeline")

TRANSACTION_COLUMNS = ["t_dat", "customer_id", "article_id", "price", "sales_channel_id"]
ARTICLE_COLUMNS = [
    "article_id",
    "product_code",
    "prod_name",
    "product_type_no",
    "product_type_name",
    "product_group_name",
    "graphical_appearance_no",
    "graphical_appearance_name",
    "colour_group_code",
    "colour_group_name",
    "perceived_colour_value_id",
    "perceived_colour_value_name",
    "perceived_colour_master_id",
    "perceived_colour_master_name",
    "department_no",
    "department_name",
    "index_code",
    "index_name",
    "index_group_no",
    "index_group_name",
    "section_no",
    "section_name",
    "garment_group_no",
    "garment_group_name",
    "detail_desc",
]
CUSTOMER_COLUMNS = [
    "customer_id",
    "FN",
    "Active",
    "club_member_status",
    "fashion_news_frequency",
    "age",
    "postal_code",
]


def _read_as_strings(spark: SparkSession, path: Path, columns: list[str]) -> DataFrame:
    """Read one source file while preserving raw values for Bronze."""
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as source:
            header = next(csv.reader(source), [])
        if header != columns:
            raise ValueError(f"Unexpected CSV schema in {path}: {header}")
        frame = (
            spark.read.option("header", True)
            .option("mode", "FAILFAST")
            .option("multiLine", True)
            .option("escape", '"')
            .schema(", ".join(f"`{column}` STRING" for column in columns))
            .csv(str(path))
        )
    else:
        frame = spark.read.parquet(str(path))
        if set(frame.columns) != set(columns):
            raise ValueError(f"Unexpected Parquet schema in {path}: {frame.columns}")
        frame = frame.select(
            [F.col(column).cast("string").alias(column) for column in columns]
        )

    return frame.select(
        *[
            F.col(column).cast("string").alias(column)
            for column in columns
        ]
    )


def _source_file_column(path: Path) -> F.Column:
    source_name = F.regexp_extract(F.input_file_name(), r"([^/]+$)", 1)
    return F.coalesce(F.nullif(source_name, F.lit("")), F.lit(path.name))


def _source_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clean_text(column: str) -> F.Column:
    return F.coalesce(F.nullif(F.trim(F.col(column)), F.lit("")), F.lit("UNKNOWN"))


class PipelineContext:
    """Validated run configuration and storage layout shared by all layers."""

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        batch_id: Optional[str] = None,
        limit_rows: Optional[int] = None,
        master: Optional[str] = None,
    ) -> None:
        self.input_dir = Path(input_dir).expanduser()
        self.output_dir = Path(output_dir).expanduser()
        self.batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.limit_rows = limit_rows
        if limit_rows is not None and limit_rows <= 0:
            raise ValueError("limit_rows must be positive")
        if not self.batch_id or Path(self.batch_id).name != self.batch_id or self.batch_id in (".", ".."):
            raise ValueError("batch_id must be a non-empty filename component")
        self.master = master
        self.spark: Optional[SparkSession] = None

    def create_spark_session(self) -> SparkSession:
        builder = (
            SparkSession.builder.appName("DAT535-HM-Medallion")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "400"))
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
            .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "3g"))
        )
        if self.master:
            builder = builder.master(self.master)
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        LOGGER.info("Spark %s on %s", self.spark.version, self.spark.sparkContext.master)
        return self.spark

    def require_spark(self) -> SparkSession:
        if self.spark is None:
            raise RuntimeError("create_spark_session must be called first")
        return self.spark

    def _find_source(self, stem: str) -> Path:
        for suffix in (".csv", ".parquet"):
            path = self.input_dir / f"{stem}{suffix}"
            if path.is_file():
                return path
        raise FileNotFoundError(
            f"Could not find {stem}.csv or {stem}.parquet in {self.input_dir}"
        )

    def _bronze_path(self, table: str) -> str:
        return str(self.output_dir / "bronze" / table)

    def _silver_path(self, table: str) -> str:
        return str(self.output_dir / "silver" / table)

    def _gold_path(self, table: str) -> str:
        return str(self.output_dir / "gold" / table)

    def _write_quality(self, metrics: dict[str, object]) -> None:
        quality_path = self.output_dir / "quality"
        quality_path.mkdir(parents=True, exist_ok=True)
        destination = quality_path / f"{self.batch_id}.json"
        temporary = destination.with_suffix(".partial")
        temporary.write_text(
            json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        temporary.replace(destination)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="H&M transaction pipeline using Bronze, Silver, and Gold layers."
    )
    parser.add_argument(
        "--input-dir",
        default=os.environ.get("HM_DATASET_DIR", "/home/ubuntu/HMDataSet"),
        help="Directory containing transactions_train, articles, and customers CSV/Parquet files",
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("DAT535_OUTPUT_DIR", "/home/ubuntu/spark-lab-data/hm"),
        help="Root directory for Bronze, Silver, and Gold outputs",
    )
    parser.add_argument("--batch-id", help="Stable identifier for this Bronze ingestion batch")
    parser.add_argument("--limit-rows", type=int, help="Limit each source for a small development run")
    parser.add_argument("--master", help="Optional Spark master, for example local[2] or spark://host:7077")
    return parser.parse_args()
