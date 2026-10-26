#!/usr/bin/env python3
"""H&M transaction pipeline using Bronze, Silver, and Gold layers.

The pipeline accepts either the CSV files from the Kaggle download or the
Parquet conversion currently present on the Spark VM. Bronze writes are
append-only; Silver and Gold are rebuilt from the complete Bronze history.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import hashlib
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
        frame = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .schema("`" + "`,`".join(f"{column}` STRING" for column in columns))
            .csv(str(path))
        )
    else:
        frame = spark.read.parquet(str(path))
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
    stat = path.stat()
    value = f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


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

    def _batch_marker(self, table: str) -> Path:
        return self.output_dir / "bronze" / "_batches" / f"{table}-{self.batch_id}.complete"

    def ingest_bronze(self) -> Dict[str, DataFrame]:
        if self.spark is None:
            raise RuntimeError("create_spark_session must be called first")

        sources = {
            "transactions": (self._find_source("transactions_train"), TRANSACTION_COLUMNS),
            "articles": (self._find_source("articles"), ARTICLE_COLUMNS),
            "customers": (self._find_source("customers"), CUSTOMER_COLUMNS),
        }
        bronze: Dict[str, DataFrame] = {}

        for table, (path, columns) in sources.items():
            fingerprint = _source_fingerprint(path)
            source_marker = self.output_dir / "bronze" / "_sources" / f"{table}-{fingerprint}.complete"
            if source_marker.exists():
                LOGGER.info("Bronze %s: unchanged source already ingested; skipping", table)
                bronze[table] = self.spark.read.option("mergeSchema", "true").parquet(
                    self._bronze_path(table)
                ).filter(
                    F.col("source_fingerprint") == fingerprint
                )
                continue
            frame = _read_as_strings(self.spark, path, columns)
            if self.limit_rows is not None:
                frame = frame.limit(self.limit_rows)
            enriched = (
                frame.withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("source_file", _source_file_column(path))
                .withColumn("batch_id", F.lit(self.batch_id))
                .withColumn("source_fingerprint", F.lit(fingerprint))
            )
            enriched.write.mode("append").parquet(self._bronze_path(table))
            marker = self._batch_marker(table)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
            source_marker.parent.mkdir(parents=True, exist_ok=True)
            source_marker.touch()
            bronze[table] = enriched
            LOGGER.info("Bronze %s: %s rows appended from %s", table, enriched.count(), path)
        return bronze

    def build_silver(self) -> DataFrame:
        if self.spark is None:
            raise RuntimeError("create_spark_session must be called first")

        raw_transactions = self.spark.read.option("mergeSchema", "true").parquet(
            self._bronze_path("transactions")
        )
        parsed_transactions = raw_transactions.select(
            "*",
            F.to_date("t_dat").alias("transaction_date"),
            F.trim("customer_id").alias("clean_customer_id"),
            F.col("article_id").cast("long").alias("clean_article_id"),
            F.col("price").cast("decimal(12, 4)").alias("clean_price"),
            F.col("sales_channel_id").cast("int").alias("clean_sales_channel_id"),
        )
        invalid = (
            F.col("transaction_date").isNull()
            | F.col("clean_customer_id").isNull()
            | (F.col("clean_customer_id") == "")
            | F.col("clean_article_id").isNull()
            | F.col("clean_price").isNull()
            | (F.col("clean_price") <= 0)
            | ~F.col("clean_sales_channel_id").isin(1, 2)
        )
        rejected = (
            parsed_transactions.filter(invalid)
            .withColumn(
                "rejection_reason",
                F.when(F.col("transaction_date").isNull(), "invalid_date")
                .when(F.col("clean_customer_id").isNull() | (F.col("clean_customer_id") == ""), "missing_customer_id")
                .when(F.col("clean_article_id").isNull(), "invalid_article_id")
                .when(F.col("clean_price").isNull() | (F.col("clean_price") <= 0), "invalid_price")
                .otherwise("invalid_sales_channel_id"),
            )
        )
        quality_counts = parsed_transactions.select(
            F.count("*").alias("bronze_transactions"),
            F.sum(F.when(invalid, 1).otherwise(0)).alias("rejected_transactions"),
        ).first()
        rejected.write.mode("overwrite").parquet(str(self.output_dir / "silver" / "rejected_transactions"))
        valid_rows = parsed_transactions.filter(~invalid)
        valid = valid_rows.dropDuplicates(
            ["t_dat", "customer_id", "article_id", "price", "sales_channel_id"]
        )
        transactions = valid.select(
            "transaction_date",
            F.col("clean_customer_id").alias("customer_id"),
            F.col("clean_article_id").alias("article_id"),
            F.col("clean_price").alias("price"),
            F.col("clean_sales_channel_id").alias("sales_channel_id"),
        )
        articles = self.spark.read.option("mergeSchema", "true").parquet(
            self._bronze_path("articles")
        ).select(
            F.col("article_id").cast("long").alias("article_id"),
            *[
                F.coalesce(F.trim(F.col(column)), F.lit("UNKNOWN")).alias(column)
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
        ).dropDuplicates(["article_id"])
        customers = self.spark.read.option("mergeSchema", "true").parquet(
            self._bronze_path("customers")
        ).select(
            F.trim("customer_id").alias("customer_id"),
            F.col("age").cast("double").alias("age"),
            F.coalesce(F.trim("club_member_status"), F.lit("UNKNOWN")).alias(
                "club_member_status"
            ),
            F.coalesce(F.trim("fashion_news_frequency"), F.lit("UNKNOWN")).alias(
                "fashion_news_frequency"
            ),
            F.coalesce(F.trim("postal_code"), F.lit("UNKNOWN")).alias("postal_code"),
        ).dropDuplicates(["customer_id"])

        customer_clean = customers.withColumn(
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
            transactions.join(articles, "article_id", "left")
            .join(customer_clean, "customer_id", "left")
            .withColumn("transaction_date", F.coalesce(F.col("transaction_date"), F.to_date(F.lit("1900-01-01"))))
            .withColumn("price", F.coalesce(F.col("price"), F.lit(0).cast("decimal(12,4)")))
            .withColumn("age_group", F.coalesce(F.col("age_group"), F.lit("UNKNOWN")))
            .withColumn("product_group_name", F.coalesce(F.col("product_group_name"), F.lit("UNKNOWN")))
            .withColumn("colour_group_name", F.coalesce(F.col("colour_group_name"), F.lit("UNKNOWN")))
            .withColumn("garment_group_name", F.coalesce(F.col("garment_group_name"), F.lit("UNKNOWN")))
        )
        silver.write.mode("overwrite").partitionBy("transaction_date").parquet(
            self._silver_path("master_transactions")
        )
        silver_count = self.spark.read.parquet(
            self._silver_path("master_transactions")
        ).count()
        metrics = {
            "batch_id": self.batch_id,
            "bronze_transactions": quality_counts["bronze_transactions"],
            "rejected_transactions": quality_counts["rejected_transactions"] or 0,
            "accepted_transactions": silver_count,
            "silver_transactions": silver_count,
            "duplicate_transactions_removed": (
                quality_counts["bronze_transactions"]
                - quality_counts["rejected_transactions"]
                - silver_count
            ),
        }
        quality_path = self.output_dir / "quality"
        quality_path.mkdir(parents=True, exist_ok=True)
        (quality_path / f"{self.batch_id}.json").write_text(
            json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        LOGGER.info("Silver quality metrics: %s", metrics)
        # Read the materialized layer back so Gold does not recompute the
        # expensive validation and reference joins for every output.
        return self.spark.read.parquet(self._silver_path("master_transactions"))

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
        inventory_metrics = (
            silver.groupBy("article_id")
            .agg(
                F.count(F.lit(1)).alias("total_units_sold"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("transaction_date").alias("sales_days"),
                F.max("transaction_date").alias("last_sale_date"),
            )
        )
        all_articles = self.spark.read.option("mergeSchema", "true").parquet(
            self._bronze_path("articles")
        ).select(
            F.col("article_id").cast("long").alias("article_id")
        ).distinct()
        inventory = (
            all_articles.join(inventory_metrics, "article_id", "left")
            .withColumn("total_units_sold", F.coalesce(F.col("total_units_sold"), F.lit(0)))
            .withColumn("unique_customers", F.coalesce(F.col("unique_customers"), F.lit(0)))
            .withColumn("sales_days", F.coalesce(F.col("sales_days"), F.lit(0)))
            .withColumn("dataset_max_date", F.lit(max_date).cast("date"))
            .withColumn(
                "recent_units_30d",
                F.when(
                    F.col("last_sale_date") >= F.date_sub(F.col("dataset_max_date"), 29),
                    F.col("total_units_sold"),
                ).otherwise(F.lit(0)),
            )
            .withColumn(
                "deadstock_last_30d",
                F.col("recent_units_30d") == 0,
            )
        )

        recommendation_window = Window.partitionBy("customer_id").orderBy(
            F.desc("interaction_count"), F.desc("last_purchase_date")
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
        return outputs

    def run(self) -> Dict[str, DataFrame]:
        self.create_spark_session()
        try:
            self.ingest_bronze()
            silver = self.build_silver()
            return self.build_gold(silver)
        finally:
            assert self.spark is not None
            self.spark.stop()


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
