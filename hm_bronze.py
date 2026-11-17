#!/usr/bin/env python3
"""Bronze layer: ingest and preserve the raw H&M source files.

Reads the dataset from the configured input directory and commits it as
append-only Parquet under <output-dir>/bronze, archiving each original file
unchanged under <output-dir>/raw. Ingestion is idempotent: a source file with
an unchanged SHA-256 content fingerprint is skipped, so the dataset is only
processed once per version and never needs to be re-downloaded for Silver or
Gold runs.

Run standalone with: python hm_bronze.py [--input-dir ...] [--output-dir ...]
"""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from hm_common import (
    ARTICLE_COLUMNS,
    CUSTOMER_COLUMNS,
    LOGGER,
    TRANSACTION_COLUMNS,
    PipelineContext,
    _read_as_strings,
    _source_file_column,
    _source_fingerprint,
)


class BronzeLayer:
    """Ingest the raw source files into the append-only Bronze layer."""

    def __init__(self, context: PipelineContext) -> None:
        self.context = context

    def ingest(self) -> Dict[str, DataFrame]:
        context = self.context
        spark = context.require_spark()

        sources = {
            "transactions": (context._find_source("transactions_train"), TRANSACTION_COLUMNS),
            "articles": (context._find_source("articles"), ARTICLE_COLUMNS),
            "customers": (context._find_source("customers"), CUSTOMER_COLUMNS),
        }
        bronze: Dict[str, DataFrame] = {}
        config_path = context.output_dir / "_ingestion_config.json"
        config = {"version": 2, "limit_rows": context.limit_rows}
        if config_path.exists():
            if json.loads(config_path.read_text()) != config:
                raise ValueError("Do not mix different sample limits or full runs in one output directory")
        else:
            if (context.output_dir / "bronze").exists():
                raise ValueError("Legacy Bronze layout detected; use a fresh output directory")
            context.output_dir.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(config), encoding="utf-8")

        for table, (path, columns) in sources.items():
            fingerprint = _source_fingerprint(path)
            destination = Path(context._bronze_path(table)) / f"source_version={fingerprint}"
            if (destination / "_SUCCESS").exists():
                LOGGER.info("Bronze %s: unchanged source already ingested; skipping", table)
                bronze[table] = spark.read.parquet(str(destination))
                continue
            if table == "transactions" and destination.parent.exists():
                if any(destination.parent.glob("source_version=*/_SUCCESS")):
                    raise ValueError(
                        "Changed transaction snapshot detected; use a fresh output directory "
                        "to avoid double-counting overlapping historical purchases"
                    )
            archive = context.output_dir / "raw" / table / fingerprint / path.name
            archive.parent.mkdir(parents=True, exist_ok=True)
            if not archive.exists():
                temporary = archive.with_suffix(archive.suffix + ".partial")
                shutil.copyfile(path, temporary)
                if _source_fingerprint(temporary) != fingerprint:
                    raise ValueError(f"Source changed while archiving: {path}")
                temporary.replace(archive)
            elif _source_fingerprint(archive) != fingerprint:
                raise ValueError(f"Archived source checksum mismatch: {archive}")
            frame = _read_as_strings(spark, archive, columns)
            if context.limit_rows is not None:
                frame = frame.limit(context.limit_rows)
            enriched = (
                frame.withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("source_file", _source_file_column(path))
                .withColumn("batch_id", F.lit(context.batch_id))
                .withColumn("source_fingerprint", F.lit(fingerprint))
            )
            staging = context.output_dir / "_staging" / f"{table}-{uuid.uuid4().hex}"
            enriched.write.mode("errorifexists").parquet(str(staging))
            destination.parent.mkdir(parents=True, exist_ok=True)
            staging.rename(destination)
            bronze[table] = spark.read.parquet(str(destination))
            LOGGER.info("Bronze %s committed from %s (sha256=%s)", table, path, fingerprint)
        return bronze


if __name__ == "__main__":
    from hm_medallion_pipeline import run_stages

    run_stages(("bronze",))
