#!/usr/bin/env python3
"""Gold layer: serve consumer-shaped analytical outputs.

Rebuilds <output-dir>/gold from the materialized Silver layer in the same
output directory (plus the Bronze article catalog for never-sold coverage);
the original source dataset is never read. Every Gold table is reconciled
against Silver totals after writing, and the run fails on any mismatch.

Requires the silver stage (hm_silver.py) to have completed first.
Run standalone with: python hm_gold.py [--output-dir ...]
"""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hm_common import LOGGER, PipelineContext


class GoldLayer:
    """Build the purpose-specific Gold tables from Silver."""

    def __init__(self, context: PipelineContext) -> None:
        self.context = context

    def build(self, silver: DataFrame) -> Dict[str, DataFrame]:
        context = self.context
        spark = context.require_spark()

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
        all_articles = spark.read.option("mergeSchema", "true").parquet(
            context._bronze_path("articles")
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
            frame.write.mode("overwrite").parquet(context._gold_path(name))
            LOGGER.info("Gold %s written", name)
        expected = silver.agg(F.count("*").alias("units"), F.sum("price").alias("sales")).first()
        for name, units, sales in (
            ("daily_sales_demand", "transaction_count", "total_sales"),
            ("recommendation_features", "interaction_count", "total_spend"),
            ("inventory_velocity", "total_units_sold", None),
        ):
            written = spark.read.parquet(context._gold_path(name))
            actual = written.agg(
                F.sum(units).alias("units"),
                F.sum(sales).alias("sales") if sales else F.lit(None).alias("sales"),
            ).first()
            if actual["units"] != expected["units"] or (sales and actual["sales"] != expected["sales"]):
                raise ValueError(f"Gold reconciliation failed for {name}: {actual} != {expected}")
        return outputs


if __name__ == "__main__":
    from hm_medallion_pipeline import run_stages

    run_stages(("gold",))
