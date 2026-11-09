#!/usr/bin/env python3
"""H&M transaction pipeline using Bronze, Silver, and Gold layers.

The pipeline accepts either the CSV files from the Kaggle download or the
Parquet conversion currently present on the Spark VM. Bronze writes are
append-only; Silver and Gold are rebuilt from the complete Bronze history.
"""
from __future__ import annotations

import argparse
import csv
import fcntl
import json
import logging
import os
import hashlib
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

try:
    import findspark

    findspark.init()
except ImportError:
    pass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from py4j.protocol import Py4JError

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


class HMMedallionPipeline:
    """Build H&M Bronze, Silver, and Gold datasets."""

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

    def _latest_reference(self, table: str, key: str, columns: list[str]) -> DataFrame:
        if self.spark is None:
            raise RuntimeError("create_spark_session must be called first")
        raw = self.spark.read.parquet(self._bronze_path(table))
        normal_key = (
            F.expr("try_cast(article_id AS BIGINT)") if key == "article_id"
            else F.trim(F.col(key))
        )
        versions = raw.withColumn(key, normal_key).dropDuplicates(columns + ["source_fingerprint"])
        if versions.groupBy(key, "source_fingerprint").count().filter("count > 1").limit(1).count():
            raise ValueError(f"Conflicting {table} records within a source version")
        window = Window.partitionBy(key).orderBy(
            F.desc("ingestion_timestamp"), F.desc("source_fingerprint")
        )
        return versions.withColumn("_version_rank", F.row_number().over(window)).filter("_version_rank = 1")

    def ingest_bronze(self) -> Dict[str, DataFrame]:
        if self.spark is None:
            raise RuntimeError("create_spark_session must be called first")

        sources = {
            "transactions": (self._find_source("transactions_train"), TRANSACTION_COLUMNS),
            "articles": (self._find_source("articles"), ARTICLE_COLUMNS),
            "customers": (self._find_source("customers"), CUSTOMER_COLUMNS),
        }
        bronze: Dict[str, DataFrame] = {}
        config_path = self.output_dir / "_ingestion_config.json"
        config = {"version": 2, "limit_rows": self.limit_rows}
        if config_path.exists():
            if json.loads(config_path.read_text()) != config:
                raise ValueError("Do not mix different sample limits or full runs in one output directory")
        else:
            if (self.output_dir / "bronze").exists():
                raise ValueError("Legacy Bronze layout detected; use a fresh output directory")
            self.output_dir.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(config), encoding="utf-8")

        for table, (path, columns) in sources.items():
            fingerprint = _source_fingerprint(path)
            destination = Path(self._bronze_path(table)) / f"source_version={fingerprint}"
            if (destination / "_SUCCESS").exists():
                LOGGER.info("Bronze %s: unchanged source already ingested; skipping", table)
                bronze[table] = self.spark.read.parquet(str(destination))
                continue
            if table == "transactions" and destination.parent.exists():
                if any(destination.parent.glob("source_version=*/_SUCCESS")):
                    raise ValueError(
                        "Changed transaction snapshot detected; use a fresh output directory "
                        "to avoid double-counting overlapping historical purchases"
                    )
            archive = self.output_dir / "raw" / table / fingerprint / path.name
            archive.parent.mkdir(parents=True, exist_ok=True)
            if not archive.exists():
                temporary = archive.with_suffix(archive.suffix + ".partial")
                shutil.copyfile(path, temporary)
                if _source_fingerprint(temporary) != fingerprint:
                    raise ValueError(f"Source changed while archiving: {path}")
                temporary.replace(archive)
            elif _source_fingerprint(archive) != fingerprint:
                raise ValueError(f"Archived source checksum mismatch: {archive}")
            frame = _read_as_strings(self.spark, archive, columns)
            if self.limit_rows is not None:
                frame = frame.limit(self.limit_rows)
            enriched = (
                frame.withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("source_file", _source_file_column(path))
                .withColumn("batch_id", F.lit(self.batch_id))
                .withColumn("source_fingerprint", F.lit(fingerprint))
            )
            staging = self.output_dir / "_staging" / f"{table}-{uuid.uuid4().hex}"
            enriched.write.mode("errorifexists").parquet(str(staging))
            destination.parent.mkdir(parents=True, exist_ok=True)
            staging.rename(destination)
            bronze[table] = self.spark.read.parquet(str(destination))
            LOGGER.info("Bronze %s committed from %s (sha256=%s)", table, path, fingerprint)
        return bronze

    def build_silver(self) -> DataFrame:
        if self.spark is None:
            raise RuntimeError("create_spark_session must be called first")

        raw_transactions = self.spark.read.option("mergeSchema", "true").parquet(
            self._bronze_path("transactions")
        )
        parsed_transactions = raw_transactions.select(
            "t_dat",
            "customer_id",
            "article_id",
            "price",
            "sales_channel_id",
            "source_file",
            "ingestion_timestamp",
            "batch_id",
            F.expr("try_cast(t_dat AS DATE)").alias("transaction_date"),
            F.trim("customer_id").alias("clean_customer_id"),
            F.expr("try_cast(article_id AS BIGINT)").alias("clean_article_id"),
            F.expr("try_cast(price AS DECIMAL(12, 4))").alias("clean_price"),
            F.expr("try_cast(sales_channel_id AS INT)").alias("clean_sales_channel_id"),
        )
        invalid = (
            F.col("transaction_date").isNull()
            | F.col("clean_customer_id").isNull()
            | (F.col("clean_customer_id") == "")
            | F.col("clean_article_id").isNull()
            | (F.col("clean_article_id") <= 0)
            | F.col("clean_price").isNull()
            | (F.col("clean_price") <= 0)
            | F.col("clean_sales_channel_id").isNull()
            | ~F.col("clean_sales_channel_id").isin(1, 2)
        )
        rejected = (
            parsed_transactions.filter(invalid)
            .withColumn(
                "rejection_reason",
                F.when(F.col("transaction_date").isNull(), "invalid_date")
                .when(F.col("clean_customer_id").isNull() | (F.col("clean_customer_id") == ""), "missing_customer_id")
                .when(F.col("clean_article_id").isNull() | (F.col("clean_article_id") <= 0), "invalid_article_id")
                .when(F.col("clean_price").isNull() | (F.col("clean_price") <= 0), "invalid_price")
                .otherwise("invalid_sales_channel_id"),
            )
            .select(
                "t_dat",
                "customer_id",
                "article_id",
                "price",
                "sales_channel_id",
                "source_file",
                "ingestion_timestamp",
                "batch_id",
                "transaction_date",
                "rejection_reason",
            )
        )
        quality_counts = parsed_transactions.select(
            F.count("*").alias("bronze_transactions"),
            F.sum(F.when(invalid, 1).otherwise(0)).alias("rejected_transactions"),
        ).first()
        rejected.write.mode("overwrite").parquet(str(self.output_dir / "silver" / "rejected_transactions"))
        if quality_counts["bronze_transactions"] == (quality_counts["rejected_transactions"] or 0):
            self._write_quality({
                "batch_id": self.batch_id,
                "bronze_transactions": quality_counts["bronze_transactions"],
                "rejected_transactions": quality_counts["rejected_transactions"] or 0,
                "accepted_transactions": 0,
                "silver_transactions": 0,
                "duplicate_transactions_removed": 0,
                "quality_passed": False,
            })
            raise ValueError("Silver non-empty quality gate failed: no valid transactions")
        valid_rows = parsed_transactions.filter(~invalid)
        # There is no unique purchase ID: identical observations may be real purchases.
        transactions = valid_rows.select(
            "transaction_date",
            "source_file",
            "ingestion_timestamp",
            "batch_id",
            F.col("clean_customer_id").alias("customer_id"),
            F.col("clean_article_id").alias("article_id"),
            F.col("clean_price").alias("price"),
            F.col("clean_sales_channel_id").alias("sales_channel_id"),
        ).withColumn("transaction_year", F.year("transaction_date")).withColumn(
            "transaction_month", F.month("transaction_date")
        )
        articles = self._latest_reference("articles", "article_id", ARTICLE_COLUMNS).select(
            F.expr("try_cast(article_id AS BIGINT)").alias("article_id"),
            *[
                _clean_text(column).alias(column)
                for column in (
                    "prod_name",
                    "product_type_name",
                    "product_group_name",
                    "colour_group_name",
                    "department_name",
                    "index_name",
                    "index_group_name",
                    "section_name",
                    "garment_group_name",
                    "detail_desc",
                )
            ],
        )
        # Conflicting reference records within a source cannot be resolved safely.
        if articles.groupBy("article_id").count().filter("count > 1 OR article_id IS NULL OR article_id <= 0").limit(1).count():
            raise ValueError("Invalid or conflicting article reference keys")
        customers = self._latest_reference("customers", "customer_id", CUSTOMER_COLUMNS).select(
            F.trim("customer_id").alias("customer_id"),
            F.expr("try_cast(age AS DOUBLE)").alias("age"),
            _clean_text("club_member_status").alias(
                "club_member_status"
            ),
            _clean_text("fashion_news_frequency").alias(
                "fashion_news_frequency"
            ),
            _clean_text("postal_code").alias("postal_code"),
        )
        if customers.groupBy("customer_id").count().filter("count > 1 OR customer_id IS NULL OR customer_id = ''").limit(1).count():
            raise ValueError("Invalid or conflicting customer reference keys")

        customer_clean = customers.withColumn(
            "age",
            F.when(
                F.col("age").between(0, 120) & ~F.isnan("age") & (F.col("age") == F.floor("age")),
                F.col("age").cast("int"),
            ),
        ).withColumn(
            "age_group",
            F.when(F.col("age").isNull(), "UNKNOWN")
            .when(F.col("age") < 20, "UNDER_20")
            .when(F.col("age") < 30, "20_29")
            .when(F.col("age") < 40, "30_39")
            .when(F.col("age") < 50, "40_49")
            .when(F.col("age") < 60, "50_59")
            .otherwise("60_PLUS"),
        )

        silver = (
            transactions.join(articles.withColumn("article_matched", F.lit(True)), "article_id", "left")
            .join(customer_clean.withColumn("customer_matched", F.lit(True)), "customer_id", "left")
            .fillna(False, subset=["article_matched", "customer_matched"])
            .withColumn("age_group", F.coalesce(F.col("age_group"), F.lit("UNKNOWN")))
            .withColumn("product_group_name", F.coalesce(F.col("product_group_name"), F.lit("UNKNOWN")))
            .withColumn("colour_group_name", F.coalesce(F.col("colour_group_name"), F.lit("UNKNOWN")))
            .withColumn("garment_group_name", F.coalesce(F.col("garment_group_name"), F.lit("UNKNOWN")))
        )
        for column in articles.columns + customer_clean.columns:
            if column not in ("article_id", "customer_id", "age", "age_group"):
                silver = silver.withColumn(column, _clean_text(column))
        silver.write.mode("overwrite").partitionBy("transaction_date").parquet(
            self._silver_path("master_transactions")
        )
        materialized = self.spark.read.parquet(
            self._silver_path("master_transactions")
        )
        summary = materialized.agg(
            F.count("*").alias("count"),
            F.sum(F.when(
                F.col("transaction_date").isNull()
                | F.col("customer_id").isNull() | (F.col("customer_id") == "")
                | F.col("article_id").isNull() | (F.col("article_id") <= 0)
                | F.col("price").isNull() | (F.col("price") <= 0)
                | F.col("sales_channel_id").isNull() | ~F.col("sales_channel_id").isin(1, 2),
                1,
            ).otherwise(0)).alias("invalid_silver_rows"),
            F.sum(F.when(~F.col("article_matched"), 1).otherwise(0)).alias("unmatched_articles"),
            F.sum(F.when(~F.col("customer_matched"), 1).otherwise(0)).alias("unmatched_customers"),
        ).first()
        silver_count = summary["count"]
        rejected_count = rejected.count()
        metrics = {
            "batch_id": self.batch_id,
            "bronze_transactions": quality_counts["bronze_transactions"],
            "rejected_transactions": quality_counts["rejected_transactions"] or 0,
            "accepted_transactions": silver_count,
            "silver_transactions": silver_count,
            "duplicate_transactions_removed": 0,
            "deduplication_policy": "preserve_purchase_multiplicity",
            "invalid_silver_rows": summary["invalid_silver_rows"] or 0,
            "unmatched_article_transactions": summary["unmatched_articles"] or 0,
            "unmatched_customer_transactions": summary["unmatched_customers"] or 0,
            "quality_passed": (
                silver_count > 0
                and summary["invalid_silver_rows"] == 0
                and quality_counts["bronze_transactions"] == rejected_count + silver_count
                and rejected_count == (quality_counts["rejected_transactions"] or 0)
            ),
        }
        self._write_quality(metrics)
        LOGGER.info("Silver quality metrics: %s", metrics)
        if not metrics["quality_passed"]:
            raise ValueError(f"Silver row-accounting or non-empty quality gate failed: {metrics}")
        # Read the materialized layer back so Gold does not recompute the
        # expensive validation and reference joins for every output.
        return materialized

    def build_gold(self, silver: DataFrame) -> Dict[str, DataFrame]:
        if self.spark is None:
            raise RuntimeError("create_spark_session must be called first")

        segment_window = Window.partitionBy("age_group").orderBy(
            F.desc("total_units"), F.asc("garment_group_name"), F.asc("colour_group_name")
        )
        segment = (
            silver.groupBy("age_group", "garment_group_name", "colour_group_name")
            .agg(
                F.sum(F.lit(1)).alias("total_units"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("article_id").alias("unique_articles"),
            )
            .withColumn("preference_rank", F.row_number().over(segment_window))
            .filter(F.col("preference_rank") <= 10)
        )

        max_date = silver.agg(F.max("transaction_date").alias("max_date")).first()["max_date"]
        if max_date is None:
            raise ValueError("Cannot publish Gold from empty Silver")
        recent = F.col("transaction_date").between(
            F.date_sub(F.lit(max_date), 29), F.lit(max_date)
        )
        inventory_metrics = (
            silver.groupBy("article_id")
            .agg(
                F.count(F.lit(1)).alias("total_units_sold"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("transaction_date").alias("sales_days"),
                F.max("transaction_date").alias("last_sale_date"),
                F.sum(F.when(recent, 1).otherwise(0)).alias("recent_units_30d"),
            )
        )
        all_articles = self.spark.read.option("mergeSchema", "true").parquet(
            self._bronze_path("articles")
        ).select(
            F.col("article_id").cast("long").alias("article_id")
        ).filter("article_id IS NOT NULL AND article_id > 0").union(
            silver.select("article_id").distinct()
        ).distinct()
        inventory = (
            all_articles.join(inventory_metrics, "article_id", "left")
            .withColumn("total_units_sold", F.coalesce(F.col("total_units_sold"), F.lit(0)))
            .withColumn("unique_customers", F.coalesce(F.col("unique_customers"), F.lit(0)))
            .withColumn("sales_days", F.coalesce(F.col("sales_days"), F.lit(0)))
            .withColumn("dataset_max_date", F.lit(max_date).cast("date"))
            .withColumn(
                "recent_units_30d",
                F.coalesce(F.col("recent_units_30d"), F.lit(0)),
            )
            .withColumn(
                "deadstock_last_30d",
                F.col("recent_units_30d") == 0,
            )
        )

        recommendation_window = Window.partitionBy("customer_id").orderBy(
            F.desc("interaction_count"), F.desc("last_purchase_date"), F.asc("article_id")
        )
        recommendations = (
            silver.groupBy("customer_id", "article_id")
            .agg(
                F.count(F.lit(1)).alias("interaction_count"),
                F.sum("price").alias("total_spend"),
                F.max("transaction_date").alias("last_purchase_date"),
            )
            .withColumn("customer_item_rank", F.row_number().over(recommendation_window))
        )

        outputs = {
            "customer_segment_preferences": segment,
            "inventory_velocity": inventory,
            "recommendation_features": recommendations,
            "daily_sales_demand": silver.groupBy("transaction_date").agg(
                F.count("*").alias("transaction_count"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("price").alias("total_sales"),
                F.avg("price").alias("average_price"),
            ),
        }
        for name, frame in outputs.items():
            frame.write.mode("overwrite").parquet(self._gold_path(name))
            LOGGER.info("Gold %s written", name)
        expected = silver.agg(F.count("*").alias("units"), F.sum("price").alias("sales")).first()
        for name, units, sales in (
            ("daily_sales_demand", "transaction_count", "total_sales"),
            ("recommendation_features", "interaction_count", "total_spend"),
            ("inventory_velocity", "total_units_sold", None),
        ):
            written = self.spark.read.parquet(self._gold_path(name))
            actual = written.agg(
                F.sum(units).alias("units"),
                F.sum(sales).alias("sales") if sales else F.lit(None).alias("sales"),
            ).first()
            if actual["units"] != expected["units"] or (sales and actual["sales"] != expected["sales"]):
                raise ValueError(f"Gold reconciliation failed for {name}: {actual} != {expected}")
        return outputs

    def run(self) -> Dict[str, DataFrame]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        with (self.output_dir / "_run.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ready = self.output_dir / "_READY.json"
            ready.unlink(missing_ok=True)
            self.create_spark_session()
            try:
                self.ingest_bronze()
                silver = self.build_silver()
                outputs = self.build_gold(silver)
                temporary = ready.with_suffix(".partial")
                temporary.write_text(json.dumps({
                    "batch_id": self.batch_id,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "gold_tables": list(outputs),
                    "quality_report": f"quality/{self.batch_id}.json",
                }, indent=2) + "\n", encoding="utf-8")
                temporary.replace(ready)
                return outputs
            finally:
                failed = sys.exc_info()[0] is not None
                assert self.spark is not None
                try:
                    self.spark.stop()
                except (Py4JError, ConnectionError):
                    if not failed:
                        ready.unlink(missing_ok=True)
                        raise
                    LOGGER.exception("Spark shutdown failed after pipeline failure; preserving original error")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
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


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = parse_args()
    HMMedallionPipeline(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        batch_id=args.batch_id,
        limit_rows=args.limit_rows,
        master=args.master,
    ).run()


if __name__ == "__main__":
    main()
