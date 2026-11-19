#!/usr/bin/env python3
"""Silver layer: standardize, validate, and enrich H&M transactions.

Rebuilds <output-dir>/silver from the committed Bronze layer in the same
output directory; the original source dataset is never read. Accepted rows are
written date-partitioned to silver/master_transactions, rejected rows with a
rejection reason to silver/rejected_transactions, and a quality report with
before/after row accounting to quality/<batch_id>.json. The build fails rather
than publishing when a quality gate does not hold.

Requires the bronze stage (hm_bronze.py) to have completed first.
Run standalone with: python hm_silver.py [--output-dir ...]
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hm_common import (
    ARTICLE_COLUMNS,
    CUSTOMER_COLUMNS,
    LOGGER,
    PipelineContext,
    _clean_text,
)


class SilverLayer:
    """Build the validated, enriched Silver layer from Bronze."""

    def __init__(self, context: PipelineContext) -> None:
        self.context = context

    def read(self) -> DataFrame:
        """Read the materialized Silver layer written by a previous run."""
        spark = self.context.require_spark()
        return spark.read.parquet(self.context._silver_path("master_transactions"))

    def latest_reference(self, table: str, key: str, columns: list[str]) -> DataFrame:
        spark = self.context.require_spark()
        raw = spark.read.parquet(self.context._bronze_path(table))
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

    def build(self) -> DataFrame:
        context = self.context
        spark = context.require_spark()

        raw_transactions = spark.read.option("mergeSchema", "true").parquet(
            context._bronze_path("transactions")
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
        rejected.write.mode("overwrite").parquet(str(context.output_dir / "silver" / "rejected_transactions"))
        if quality_counts["bronze_transactions"] == (quality_counts["rejected_transactions"] or 0):
            context._write_quality({
                "batch_id": context.batch_id,
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
        ).withColumn(
            "season",
            F.when(F.col("transaction_month").isin(3, 4, 5), "SPRING")
            .when(F.col("transaction_month").isin(6, 7, 8), "SUMMER")
            .when(F.col("transaction_month").isin(9, 10, 11), "AUTUMN")
            .otherwise("WINTER"),
        )
        articles = self.latest_reference("articles", "article_id", ARTICLE_COLUMNS).select(
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
        customers = self.latest_reference("customers", "customer_id", CUSTOMER_COLUMNS).select(
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
            context._silver_path("master_transactions")
        )
        materialized = spark.read.parquet(
            context._silver_path("master_transactions")
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
        rejected_by_reason = {
            row["rejection_reason"]: row["count"]
            for row in rejected.groupBy("rejection_reason").count().collect()
        }
        rejected_count = sum(rejected_by_reason.values())
        metrics = {
            "batch_id": context.batch_id,
            "bronze_transactions": quality_counts["bronze_transactions"],
            "rejected_transactions": quality_counts["rejected_transactions"] or 0,
            "rejected_by_reason": rejected_by_reason,
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
        context._write_quality(metrics)
        LOGGER.info("Silver quality metrics: %s", metrics)
        if not metrics["quality_passed"]:
            raise ValueError(f"Silver row-accounting or non-empty quality gate failed: {metrics}")
        # Read the materialized layer back so Gold does not recompute the
        # expensive validation and reference joins for every output.
        return materialized


if __name__ == "__main__":
    from hm_medallion_pipeline import run_stages

    run_stages(("silver",))
