#!/usr/bin/env python3
"""Orchestrator for the H&M Bronze, Silver, and Gold medallion pipeline.

The layer implementations live in hm_bronze.py, hm_silver.py, and hm_gold.py
and share configuration through hm_common.PipelineContext. The pipeline
accepts either the CSV files from the Kaggle download or the Parquet
conversion currently present on the Spark VM. Bronze writes are append-only;
Silver and Gold are rebuilt from the complete Bronze history.

Run everything with run_pipeline.py, or a single stage with the layer modules
directly: python hm_bronze.py / hm_silver.py / hm_gold.py.
"""

from __future__ import annotations

import fcntl
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame
from py4j.protocol import Py4JError

# Re-exported so existing imports (tests, notebooks) keep one entry point.
from hm_common import (  # noqa: F401
    ARTICLE_COLUMNS,
    CUSTOMER_COLUMNS,
    LOGGER,
    TRANSACTION_COLUMNS,
    PipelineContext,
    _clean_text,
    _read_as_strings,
    _source_fingerprint,
    parse_args,
)
from hm_bronze import BronzeLayer
from hm_silver import SilverLayer
from hm_gold import GoldLayer


class HMMedallionPipeline(PipelineContext):
    """Compose the Bronze, Silver, and Gold layers over one shared context."""

    STAGES = ("bronze", "silver", "gold")

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        batch_id: Optional[str] = None,
        limit_rows: Optional[int] = None,
        master: Optional[str] = None,
    ) -> None:
        super().__init__(
            input_dir=input_dir,
            output_dir=output_dir,
            batch_id=batch_id,
            limit_rows=limit_rows,
            master=master,
        )
        self.bronze = BronzeLayer(self)
        self.silver = SilverLayer(self)
        self.gold = GoldLayer(self)

    def ingest_bronze(self) -> Dict[str, DataFrame]:
        return self.bronze.ingest()

    def build_silver(self) -> DataFrame:
        return self.silver.build()

    def build_gold(self, silver: DataFrame) -> Dict[str, DataFrame]:
        return self.gold.build(silver)

    def read_silver(self) -> DataFrame:
        return self.silver.read()

    def _latest_reference(self, table: str, key: str, columns: list[str]) -> DataFrame:
        return self.silver.latest_reference(table, key, columns)

    def _check_stage_inputs(self, stages: tuple[str, ...]) -> None:
        """Fail fast when a stage depends on a layer that was not built yet."""
        if "silver" in stages and "bronze" not in stages:
            for table in ("transactions", "articles", "customers"):
                if not Path(self._bronze_path(table)).exists():
                    raise FileNotFoundError(
                        f"Bronze {table} missing in {self.output_dir}; run the bronze stage first"
                    )
        if "gold" in stages and "silver" not in stages:
            if not Path(self._silver_path("master_transactions")).exists():
                raise FileNotFoundError(
                    f"Silver master_transactions missing in {self.output_dir}; run the silver stage first"
                )
            if not Path(self._bronze_path("articles")).exists():
                raise FileNotFoundError(
                    f"Bronze articles missing in {self.output_dir}; gold inventory needs the article catalog"
                )

    def run(self, stages: tuple[str, ...] = STAGES) -> Dict[str, DataFrame]:
        unknown = set(stages) - set(self.STAGES)
        if unknown:
            raise ValueError(f"Unknown pipeline stages: {sorted(unknown)}")
        ordered = tuple(stage for stage in self.STAGES if stage in stages)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        with (self.output_dir / "_run.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ready = self.output_dir / "_READY.json"
            # Any rerun of any stage invalidates the published state until
            # gold completes again against the rebuilt upstream layers.
            ready.unlink(missing_ok=True)
            self._check_stage_inputs(ordered)
            self.create_spark_session()
            try:
                outputs: Dict[str, DataFrame] = {}
                silver: Optional[DataFrame] = None
                if "bronze" in ordered:
                    self.ingest_bronze()
                if "silver" in ordered:
                    silver = self.build_silver()
                if "gold" in ordered:
                    outputs = self.build_gold(silver if silver is not None else self.read_silver())
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


def run_stages(stages: tuple[str, ...] = HMMedallionPipeline.STAGES) -> None:
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
    ).run(stages=stages)


def main() -> None:
    run_stages()


if __name__ == "__main__":
    main()
