#!/usr/bin/env python3
"""
DAT535 Lab 2: Spark Fundamentals & the Medallion Architecture Pipeline
========================================================================

This pipeline demonstrates:
- Spark session configuration
- DataFrame creation, basic operations, filtering, sorting, aggregations
- Data I/O (Parquet, CSV, JSON, partitioned)
- RDD transformations and MapReduce patterns
- The Medallion Architecture: Bronze -> Silver -> Gold

It generates the single canonical e-commerce clickstream dataset used by both
labs and saves the Bronze/Silver/Gold output to ~/spark-lab-data/shared/ so that
Lab 3 (lab3_pipeline.py) can load the exact same Silver-layer data instead of
regenerating it.

Usage:
    python lab2_pipeline.py

    # Or via run_pipeline.py:
    python run_pipeline.py lab2
"""
#freddy fazbear

import logging
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

try:
    import findspark
except ImportError:
    findspark = None

if findspark is not None:
    findspark.init()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, count, sum as spark_sum,
        min as spark_min, max as spark_max, round as spark_round,
        desc, to_timestamp, to_date, hour, dayofweek,
        lower, upper, trim, countDistinct, first
    )
except ImportError as e:
    logger.error(f"PySpark import failed: {e}")
    logger.error("Please install PySpark: pip install pyspark")
    sys.exit(1)


class Lab2Pipeline:
    """Spark Fundamentals & Medallion Architecture Pipeline - Lab 2"""

    def __init__(self, base_dir: str = None, shared_dir: str = None):
        """Initialize the pipeline with output directories."""
        self.base_dir = base_dir or os.path.expanduser("~/spark-lab-data/lab2")
        self.shared_dir = shared_dir or os.path.expanduser("~/spark-lab-data/shared")
        self.bronze_dir = f"{self.shared_dir}/bronze"
        self.silver_dir = f"{self.shared_dir}/silver"
        self.gold_dir = f"{self.shared_dir}/gold"
        self.spark = None
        self.events_df = None
        self.raw_json_events = None
        # checking to see if the pipeline works as it should.

    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        logger.info("Creating Spark session...")

        self.spark = SparkSession.builder \
            .appName("DAT535-Lab2-SparkFundamentals") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "2g") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Spark version: {self.spark.version}")
        logger.info(f"Spark UI: {self.spark.sparkContext.uiWebUrl}")

        return self.spark

    def generate_data(self, num_events: int = 6000, num_users: int = 200,
                       num_products: int = 100) -> list:
        """Generate the canonical e-commerce clickstream dataset shared by both labs.

        A small fraction (~3%) of records are intentionally malformed (bad user_id
        or bad timestamp) so the Bronze/Silver quality gates have real issues to catch.
        """
        logger.info(f"Generating {num_events} events...")

        random.seed(42)

        event_types = ['page_view', 'search', 'add_to_cart', 'remove_from_cart',
                       'purchase', 'login', 'logout', 'wishlist_add']
        devices = ['mobile', 'desktop', 'tablet']
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']
        countries = ['US', 'UK', 'DE', 'FR', 'CA', 'AU', 'JP', 'IN']

        events = []
        base_time = datetime(2024, 1, 1, 0, 0, 0)

        for i in range(num_events):
            timestamp = base_time + timedelta(
                days=random.randint(0, 29),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )

            event_type = random.choice(event_types)
            user_id = random.randint(1, num_users)

            event = {
                'event_id': f'evt_{i+1:06d}',
                'timestamp': timestamp.isoformat(),
                'user_id': user_id,
                'event_type': event_type,
                'device': random.choice(devices),
                'country': random.choice(countries),
                'session_id': f'sess_{user_id}_{random.randint(1, 5):03d}',
                'product_id': None,
                'category': None,
                'price': None,
                'quantity': None,
                'total_amount': None,
                'search_query': None,
            }

            if event_type in ['page_view', 'add_to_cart', 'purchase', 'wishlist_add']:
                event['product_id'] = f'prod_{random.randint(1, num_products):04d}'
                event['category'] = random.choice(categories)
                event['price'] = round(random.uniform(9.99, 499.99), 2)

            if event_type == 'purchase':
                event['quantity'] = random.randint(1, 5)
                event['total_amount'] = round(event['price'] * event['quantity'], 2)

            if event_type == 'search':
                event['search_query'] = random.choice(
                    ['laptop', 'shoes', 'phone', 'book', 'jacket', 'headphones'])

            # Inject ~3% bad data so the Medallion pipeline has real quality issues to catch
            if random.random() < 0.03:
                if random.random() < 0.5:
                    event['user_id'] = 'invalid'
                else:
                    event['timestamp'] = 'not-a-date'

            events.append(event)

        logger.info(f"Generated {len(events)} events")
        self.raw_json_events = [json.dumps(e) for e in events]
        return events

    def create_dataframe(self, events: list):
        """Create a DataFrame from the generated events (schema inference)."""
        logger.info("Creating DataFrame...")

        self.events_df = self.spark.createDataFrame(events)

        logger.info(f"DataFrame created with {self.events_df.count()} rows")
        logger.info(f"Columns: {self.events_df.columns}")

        return self.events_df

    def run_basic_operations(self):
        """Demonstrate select/filter DataFrame operations."""
        logger.info("Running basic DataFrame operations...")

        results = {}

        selected = self.events_df.select("event_id", "user_id", "event_type", "device")
        results['selected_columns'] = selected.columns

        purchases = self.events_df.filter(col("event_type") == "purchase")
        results['total_purchases'] = purchases.count()

        mobile_purchases = self.events_df.filter(
            (col("event_type") == "purchase") & (col("device") == "mobile")
        )
        results['mobile_purchases'] = mobile_purchases.count()

        high_value = self.events_df.filter(
            (col("total_amount").isNotNull()) & (col("total_amount") > 100)
        )
        results['high_value_purchases'] = high_value.count()

        logger.info(f"Basic operations results: {results}")
        return results

    def run_aggregations(self):
        """Run aggregation and groupBy operations."""
        logger.info("Running aggregations...")

        results = {}

        overall_stats = self.events_df.agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            spark_sum("total_amount").alias("total_revenue")
        ).collect()[0]

        results['total_events'] = overall_stats['total_events']
        results['unique_users'] = overall_stats['unique_users']
        results['total_revenue'] = overall_stats['total_revenue']

        event_dist = self.events_df.groupBy("event_type").agg(
            count("*").alias("count")
        ).orderBy(desc("count")).collect()
        results['event_distribution'] = {row['event_type']: row['count'] for row in event_dist}

        device_dist = self.events_df.groupBy("device").agg(count("*").alias("count")).collect()
        results['device_distribution'] = {row['device']: row['count'] for row in device_dist}

        purchases = self.events_df.filter(
            (col("event_type") == "purchase") & col("category").isNotNull()
        )
        category_stats = purchases.groupBy("category").agg(
            count("*").alias("num_sales"),
            spark_sum("total_amount").alias("revenue")
        ).orderBy(desc("revenue")).collect()
        results['category_revenue'] = {
            row['category']: round(row['revenue'], 2) for row in category_stats
        }

        logger.info("Aggregation results computed")
        return results

    def run_mapreduce_demo(self):
        """Demonstrate the classic Map -> Shuffle -> Reduce pattern with RDDs."""
        logger.info("Running MapReduce demonstration (revenue by category)...")

        raw_rdd = self.spark.sparkContext.parallelize(self.raw_json_events)

        def extract_purchase_info(json_str):
            try:
                event = json.loads(json_str)
                if event.get('event_type') == 'purchase' and event.get('total_amount'):
                    return [(event.get('category', 'Unknown'), event['total_amount'])]
                return []
            except Exception:
                return []

        category_sales_rdd = raw_rdd.flatMap(extract_purchase_info)
        total_by_category = category_sales_rdd.reduceByKey(lambda a, b: a + b)

        results = dict(total_by_category.collect())
        logger.info(f"MapReduce - revenue by category: {results}")
        return results

    def save_outputs(self):
        """Save the DataFrame in multiple formats to demonstrate Spark I/O."""
        logger.info("Saving Lab 2 I/O examples (Parquet/CSV/partitioned)...")

        os.makedirs(self.base_dir, exist_ok=True)

        df_with_date = self.events_df.withColumn(
            "event_date", to_date(to_timestamp(col("timestamp")))
        )

        parquet_path = f"{self.base_dir}/events.parquet"
        df_with_date.write.mode("overwrite").parquet(parquet_path)
        logger.info(f"Saved Parquet: {parquet_path}")

        csv_path = f"{self.base_dir}/events.csv"
        df_with_date.write.mode("overwrite").option("header", "true").csv(csv_path)
        logger.info(f"Saved CSV: {csv_path}")

        partitioned_path = f"{self.base_dir}/events_partitioned.parquet"
        df_with_date.write.mode("overwrite").partitionBy("event_date").parquet(partitioned_path)
        logger.info(f"Saved partitioned Parquet: {partitioned_path}")

        return {'parquet': parquet_path, 'csv': csv_path, 'partitioned': partitioned_path}

    def run_bronze_layer(self):
        """Bronze Layer: parse raw JSON events and attach lineage metadata."""
        logger.info("=" * 50)
        logger.info("BRONZE LAYER: Raw Data Ingestion")
        logger.info("=" * 50)

        raw_rdd = self.spark.sparkContext.parallelize(self.raw_json_events)

        def parse_event_with_metadata(json_str):
            try:
                event = json.loads(json_str)
                event['_bronze_ingestion_time'] = datetime.now().isoformat()
                event['_bronze_source'] = 'raw_clickstream_feed'
                event['_bronze_status'] = 'valid'
                event['_bronze_raw_data'] = json_str
                return event
            except Exception as e:
                return {
                    '_bronze_ingestion_time': datetime.now().isoformat(),
                    '_bronze_source': 'raw_clickstream_feed',
                    '_bronze_status': 'parse_error',
                    '_bronze_error': str(e),
                    '_bronze_raw_data': json_str,
                }

        bronze_rdd = raw_rdd.map(parse_event_with_metadata)
        bronze_df = self.spark.createDataFrame(bronze_rdd)

        os.makedirs(self.bronze_dir, exist_ok=True)
        bronze_path = f"{self.bronze_dir}/events"
        bronze_df.write.mode("overwrite").parquet(bronze_path)

        total = bronze_df.count()
        valid = bronze_df.filter(col("_bronze_status") == "valid").count()
        logger.info(f"Bronze Layer - Total: {total}, Valid: {valid}, Errors: {total - valid}")

        return bronze_df, bronze_path

    def run_silver_layer(self, bronze_path: str):
        """Silver Layer: clean, validate, cast types, and quarantine bad records."""
        logger.info("=" * 50)
        logger.info("SILVER LAYER: Data Cleaning & Validation")
        logger.info("=" * 50)

        bronze_df = self.spark.read.parquet(bronze_path)
        valid_bronze = bronze_df.filter(col("_bronze_status") == "valid")

        silver_df = valid_bronze \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("event_date", to_date(col("event_timestamp"))) \
            .withColumn("event_hour", hour(col("event_timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("event_date"))) \
            .withColumn("user_id_clean",
                        when(col("user_id").cast("int").isNotNull(),
                             col("user_id").cast("int")).otherwise(lit(None))) \
            .withColumn("device", lower(trim(col("device")))) \
            .withColumn("country", upper(trim(col("country")))) \
            .withColumn("event_type", lower(trim(col("event_type")))) \
            .withColumn("price", col("price").cast("double")) \
            .withColumn("quantity", col("quantity").cast("int")) \
            .withColumn("total_amount", col("total_amount").cast("double")) \
            .withColumn("_silver_processed_time", lit(datetime.now().isoformat())) \
            .withColumn("_silver_is_valid",
                        when(col("user_id_clean").isNotNull() &
                             col("event_timestamp").isNotNull() &
                             col("event_type").isNotNull(), lit(True))
                        .otherwise(lit(False)))

        silver_valid = silver_df.filter(col("_silver_is_valid") == True)
        silver_quarantine = silver_df.filter(col("_silver_is_valid") == False)

        silver_final = silver_valid.select(
            col("event_id"), col("event_timestamp"), col("event_date"), col("event_hour"),
            col("day_of_week"), col("user_id_clean").alias("user_id"), col("event_type"),
            col("device"), col("country"), col("session_id"), col("product_id"),
            col("category"), col("price"), col("quantity"), col("total_amount"),
            col("_silver_processed_time")
        )

        os.makedirs(self.silver_dir, exist_ok=True)
        silver_path = f"{self.silver_dir}/events"
        quarantine_path = f"{self.silver_dir}/quarantine"

        silver_final.write.mode("overwrite").parquet(silver_path)
        silver_quarantine.write.mode("overwrite").parquet(quarantine_path)

        valid_count = silver_final.count()
        quarantine_count = silver_quarantine.count()
        logger.info(f"Silver Layer - Valid: {valid_count}, Quarantined: {quarantine_count}")

        return silver_final, silver_path

    def run_gold_layer(self, silver_path: str):
        """Gold Layer: business-ready aggregations (daily/user/product/category)."""
        logger.info("=" * 50)
        logger.info("GOLD LAYER: Business Aggregations")
        logger.info("=" * 50)

        silver_df = self.spark.read.parquet(silver_path)
        silver_df.cache()

        os.makedirs(self.gold_dir, exist_ok=True)

        daily_metrics = silver_df.groupBy("event_date").agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("num_purchases"),
            spark_sum(when(col("event_type") == "purchase", col("total_amount")).otherwise(0)).alias("daily_revenue"),
        ).withColumn("conversion_rate",
                     spark_round(col("num_purchases") / col("unique_users") * 100, 2)
        ).withColumn("avg_order_value",
                     spark_round(col("daily_revenue") / col("num_purchases"), 2)
        ).orderBy("event_date")
        daily_metrics.write.mode("overwrite").parquet(f"{self.gold_dir}/daily_metrics")

        user_metrics = silver_df.groupBy("user_id").agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("event_date").alias("active_days"),
            spark_min("event_timestamp").alias("first_seen"),
            spark_max("event_timestamp").alias("last_seen"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("num_purchases"),
            spark_sum(when(col("event_type") == "purchase", col("total_amount")).otherwise(0)).alias("total_spent"),
            first("device").alias("primary_device"),
        ).withColumn("user_segment",
                     when(col("total_spent") > 500, "High Value")
                     .when(col("total_spent") > 100, "Medium Value")
                     .when(col("total_spent") > 0, "Low Value")
                     .otherwise("Non-Purchaser")
        ).orderBy(desc("total_spent"))
        user_metrics.write.mode("overwrite").parquet(f"{self.gold_dir}/user_metrics")

        product_metrics = silver_df.filter(col("product_id").isNotNull()) \
            .groupBy("product_id", "category").agg(
                count("*").alias("total_interactions"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                spark_sum(when(col("event_type") == "purchase", col("total_amount")).otherwise(0)).alias("revenue"),
                countDistinct("user_id").alias("unique_users"),
            ).orderBy(desc("revenue"))
        product_metrics.write.mode("overwrite").parquet(f"{self.gold_dir}/product_metrics")

        category_metrics = silver_df.filter(col("category").isNotNull()).groupBy("category").agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("num_purchases"),
            spark_round(spark_sum(when(col("event_type") == "purchase", col("total_amount")).otherwise(0)), 2).alias("total_revenue"),
            countDistinct("user_id").alias("unique_customers"),
        ).orderBy(desc("total_revenue"))
        category_metrics.write.mode("overwrite").parquet(f"{self.gold_dir}/category_metrics")

        silver_df.unpersist()
        logger.info("Gold Layer - created 4 aggregation tables")

        return {
            'daily_metrics': daily_metrics.count(),
            'user_metrics': user_metrics.count(),
            'product_metrics': product_metrics.count(),
            'category_metrics': category_metrics.count(),
        }

    def generate_summary(self):
        """Generate an end-to-end Medallion pipeline summary."""
        logger.info("=" * 50)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 50)

        bronze_count = self.spark.read.parquet(f"{self.bronze_dir}/events").count()
        silver_count = self.spark.read.parquet(f"{self.silver_dir}/events").count()
        quarantine_count = self.spark.read.parquet(f"{self.silver_dir}/quarantine").count()

        daily = self.spark.read.parquet(f"{self.gold_dir}/daily_metrics")
        users = self.spark.read.parquet(f"{self.gold_dir}/user_metrics")

        total_revenue = daily.agg(spark_sum("daily_revenue")).collect()[0][0]
        total_purchases = daily.agg(spark_sum("num_purchases")).collect()[0][0]

        summary = {
            'bronze_records': bronze_count,
            'silver_records': silver_count,
            'quarantine_records': quarantine_count,
            'data_quality_rate': round(silver_count / bronze_count * 100, 1),
            'total_revenue': total_revenue,
            'total_purchases': total_purchases,
            'total_users': users.count(),
            'avg_order_value': round(total_revenue / total_purchases, 2) if total_purchases else 0,
        }

        logger.info(f"Bronze records: {summary['bronze_records']}")
        logger.info(f"Silver records: {summary['silver_records']}")
        logger.info(f"Data quality rate: {summary['data_quality_rate']}%")
        logger.info(f"Total revenue: ${summary['total_revenue']:,.2f}")

        return summary

    def run(self):
        """Execute the complete Lab 2 pipeline."""
        logger.info("=" * 60)
        logger.info("LAB 2: SPARK FUNDAMENTALS & MEDALLION ARCHITECTURE PIPELINE")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            self.create_spark_session()
            events = self.generate_data()
            self.create_dataframe(events)

            basic_results = self.run_basic_operations()
            agg_results = self.run_aggregations()
            mapreduce_results = self.run_mapreduce_demo()
            output_paths = self.save_outputs()

            bronze_df, bronze_path = self.run_bronze_layer()
            silver_df, silver_path = self.run_silver_layer(bronze_path)
            gold_counts = self.run_gold_layer(silver_path)
            summary = self.generate_summary()

            elapsed_time = time.time() - start_time

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Unique users: {agg_results['unique_users']}")
            logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")

            return {
                'status': 'success',
                'basic_results': basic_results,
                'aggregation_results': agg_results,
                'mapreduce_results': mapreduce_results,
                'output_paths': output_paths,
                'gold_counts': gold_counts,
                'summary': summary,
                'elapsed_time': elapsed_time,
            }

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    """Main entry point."""
    pipeline = Lab2Pipeline()
    results = pipeline.run()

    print("\n" + "=" * 60)
    print("LAB 2 PIPELINE RESULTS")
    print("=" * 60)
    print(f"Status: {results['status']}")
    print(f"Total events: {results['aggregation_results']['total_events']}")
    print(f"Unique users: {results['aggregation_results']['unique_users']}")
    print(f"Bronze records: {results['summary']['bronze_records']}")
    print(f"Silver records: {results['summary']['silver_records']}")
    print(f"Data quality rate: {results['summary']['data_quality_rate']}%")
    print(f"Elapsed time: {results['elapsed_time']:.2f}s")
    print("=" * 60)

    return 0 if results['status'] == 'success' else 1


if __name__ == "__main__":
    sys.exit(main())
