"""Small synthetic Spark tests; no Kaggle download is required."""

import csv
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from hm_medallion_pipeline import (
    ARTICLE_COLUMNS,
    CUSTOMER_COLUMNS,
    TRANSACTION_COLUMNS,
    HMMedallionPipeline,
    _read_as_strings,
    _source_fingerprint,
)


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
        cls.bootstrap = HMMedallionPipeline(".", ".", master="local[1]")
        cls.spark = cls.bootstrap.create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="hm-regression-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.pipeline = HMMedallionPipeline(
            str(self.source), str(self.root / "output"), batch_id="regression", master="local[1]"
        )
        self.pipeline.spark = self.spark
        self.write("articles", ARTICLE_COLUMNS, [
            {"article_id": "1", "colour_group_name": "  ", "garment_group_name": " Tops "},
            {"article_id": "2"},
            {"article_id": "3"},
        ])
        self.write("customers", CUSTOMER_COLUMNS, [
            {"customer_id": "c1", "age": "25", "club_member_status": " "},
            {"customer_id": "c2", "age": "NaN"},
            {"customer_id": "c3", "age": "999"},
        ])
        self.valid = [
            ["2020-01-01", "c1", "1", "1.5", "1"],
            ["2020-01-01", "c1", "1", "1.5", "1"],
            ["2020-01-02", "c2", "1", "2", "2"],
            ["2020-01-31", "c3", "1", "3", "1"],
            ["2020-01-31", "missing", "99", "4", "1"],
            ["2020-01-01", "c1", "3", "1", "1"],
        ]
        invalid = [
            ["bad-date", "c1", "1", "1", "1"],
            ["2020-01-31", "", "1", "1", "1"],
            ["2020-01-31", "c1", "-1", "1", "1"],
            ["2020-01-31", "c1", "1", "0", "1"],
            ["2020-01-31", "c1", "1", "NaN", "1"],
            ["2020-01-31", "c1", "1", "1", ""],
            ["2020-01-31", "c1", "1", "1", "bad"],
            ["2020-01-31", "c1", "1", "1", "3"],
        ]
        self.write_transactions(self.valid + invalid)

    def write(self, name, columns, rows):
        with (self.source / f"{name}.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    def write_transactions(self, rows):
        self.write("transactions_train", TRANSACTION_COLUMNS, [
            dict(zip(TRANSACTION_COLUMNS, row)) for row in rows
        ])

    def test_end_to_end_quality_and_gold(self):
        with patch.object(self.pipeline, "create_spark_session", return_value=self.spark), patch.object(self.spark, "stop"):
            self.pipeline.run()
        output = self.pipeline.output_dir
        silver = self.spark.read.parquet(str(output / "silver/master_transactions"))
        self.assertEqual(silver.count(), 6)  # Identical purchases are preserved.
        self.assertEqual(silver.filter("NOT customer_matched").count(), 1)
        self.assertEqual(silver.filter("NOT article_matched").count(), 1)
        self.assertEqual(silver.filter("customer_id IN ('c2', 'c3') AND age_group = 'UNKNOWN'").count(), 2)
        self.assertEqual(silver.filter("customer_id = 'c1' AND club_member_status = 'UNKNOWN'").count(), 3)
        self.assertTrue({"batch_id", "source_file", "ingestion_timestamp", "transaction_year"} <= set(silver.columns))
        metrics = json.loads((output / "quality/regression.json").read_text())
        self.assertEqual(metrics["bronze_transactions"], 14)
        self.assertEqual(metrics["rejected_transactions"], 8)
        self.assertEqual(metrics["duplicate_transactions_removed"], 0)
        self.assertTrue(metrics["quality_passed"])
        rejected = self.spark.read.parquet(str(output / "silver/rejected_transactions"))
        self.assertEqual(rejected.filter("rejection_reason = 'invalid_sales_channel_id'").count(), 3)
        inventory = {
            row.article_id: row for row in self.spark.read.parquet(str(output / "gold/inventory_velocity")).collect()
        }
        self.assertEqual(inventory[1].total_units_sold, 4)
        self.assertEqual(inventory[1].recent_units_30d, 2)  # Jan 2 included; Jan 1 excluded.
        self.assertTrue(inventory[2].deadstock_last_30d)  # Never sold.
        self.assertTrue(inventory[3].deadstock_last_30d)  # Sold only before window.
        self.assertEqual(inventory[99].total_units_sold, 1)  # Unmatched article retained.
        daily = self.spark.read.parquet(str(output / "gold/daily_sales_demand"))
        self.assertEqual(sum(row.transaction_count for row in daily.collect()), 6)
        preferences = self.spark.read.parquet(str(output / "gold/customer_segment_preferences"))
        top = preferences.filter("age_group = '20_29' AND preference_rank = 1").first()
        self.assertEqual(top.garment_group_name, "Tops")
        self.assertEqual(top.total_units, 2)
        self.assertTrue((output / "_READY.json").exists())
        for name in ("transactions", "articles", "customers"):
            original = self.source / ("transactions_train.csv" if name == "transactions" else f"{name}.csv")
            archived = next((output / "raw" / name).glob("*/*.csv"))
            self.assertEqual(original.read_bytes(), archived.read_bytes())
        # A failed rerun must revoke the previous readiness signal.
        with patch.object(self.pipeline, "create_spark_session", return_value=self.spark), patch.object(self.spark, "stop"), patch.object(self.pipeline, "build_gold", side_effect=ValueError("injected failure")):
            with self.assertRaisesRegex(ValueError, "injected failure"):
                self.pipeline.run()
        self.assertFalse((output / "_READY.json").exists())

    def test_ingestion_retries_and_sample_isolation(self):
        self.pipeline.ingest_bronze()
        self.pipeline.batch_id = "retry"
        self.pipeline.ingest_bronze()
        self.assertEqual(self.spark.read.parquet(self.pipeline._bronze_path("transactions")).count(), 14)
        # Simulate a partial write outside the committed Bronze tree.
        partial = self.pipeline.output_dir / "_staging" / "interrupted"
        partial.mkdir(parents=True)
        (partial / "partial-data").write_text("not committed")
        self.pipeline.ingest_bronze()
        self.assertEqual(self.spark.read.parquet(self.pipeline._bronze_path("transactions")).count(), 14)
        self.pipeline.limit_rows = 1
        with self.assertRaisesRegex(ValueError, "Do not mix"):
            self.pipeline.ingest_bronze()
        self.pipeline.limit_rows = None
        self.write_transactions(self.valid)
        with self.assertRaisesRegex(ValueError, "Changed transaction snapshot"):
            self.pipeline.ingest_bronze()

    def test_fingerprint_tracks_content_not_mtime(self):
        path = self.source / "transactions_train.csv"
        before = _source_fingerprint(path)
        path.touch()
        self.assertEqual(before, _source_fingerprint(path))
        path.write_text(path.read_text().replace("2020", "2021"))
        self.assertNotEqual(before, _source_fingerprint(path))

    def test_csv_schema_drift_fails_explicitly(self):
        path = self.source / "transactions_train.csv"
        path.write_text("unexpected_column\nvalue\n")
        with self.assertRaisesRegex(ValueError, "Unexpected CSV schema"):
            _read_as_strings(self.spark, path, TRANSACTION_COLUMNS)

    def test_parquet_input(self):
        path = self.root / "input.parquet"
        self.spark.createDataFrame([("1",)], ["article_id"]).write.parquet(str(path))
        # Parquet directories are read by the helper, but the CLI expects single source files.
        self.assertEqual(_read_as_strings(self.spark, path, ["article_id"]).first().article_id, "1")

    def test_csv_quotes_and_multiline_are_preserved(self):
        path = self.source / "articles.csv"
        self.write("articles", ARTICLE_COLUMNS, [
            {"article_id": "1", "detail_desc": 'Cotton, "soft"\nand washable'}
        ])
        self.assertEqual(
            _read_as_strings(self.spark, path, ARTICLE_COLUMNS).first().detail_desc,
            'Cotton, "soft"\nand washable',
        )

    def test_empty_silver_blocks_gold(self):
        self.write_transactions([["bad", "c1", "1", "1", "1"]])
        self.pipeline.ingest_bronze()
        with self.assertRaisesRegex(ValueError, "quality gate failed"):
            self.pipeline.build_silver()
        self.assertFalse((self.pipeline.output_dir / "_READY.json").exists())

    def test_conflicting_reference_fails(self):
        self.write("customers", CUSTOMER_COLUMNS, [
            {"customer_id": "c1", "age": "25"},
            {"customer_id": "c1", "age": "30"},
        ])
        self.pipeline.ingest_bronze()
        with self.assertRaisesRegex(ValueError, "Conflicting customers"):
            self.pipeline.build_silver()

    def test_reference_versions_and_identical_duplicates(self):
        self.write("customers", CUSTOMER_COLUMNS, [
            {"customer_id": "c1", "age": "25"},
            {"customer_id": "c1", "age": "25"},
        ])
        self.pipeline.ingest_bronze()
        first = self.pipeline._latest_reference("customers", "customer_id", CUSTOMER_COLUMNS)
        self.assertEqual(first.count(), 1)
        self.assertEqual(first.first().age, "25")
        self.write("customers", CUSTOMER_COLUMNS, [{"customer_id": "c1", "age": "30"}])
        self.pipeline.ingest_bronze()
        latest = self.pipeline._latest_reference("customers", "customer_id", CUSTOMER_COLUMNS)
        self.assertEqual(latest.count(), 1)
        self.assertEqual(latest.first().age, "30")

    def test_argument_guards(self):
        with self.assertRaisesRegex(ValueError, "positive"):
            HMMedallionPipeline(".", ".", limit_rows=0)
        with self.assertRaisesRegex(ValueError, "filename"):
            HMMedallionPipeline(".", ".", batch_id="../escape")


if __name__ == "__main__":
    unittest.main()
