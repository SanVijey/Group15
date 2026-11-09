#!/usr/bin/env python3
"""Entry point for the H&M Spark medallion pipeline."""

from __future__ import annotations

import argparse
import logging
import os

from hm_medallion_pipeline import HMMedallionPipeline


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pipeline", nargs="?", default="hm", choices=("hm",))
    parser.add_argument("--input-dir", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--limit-rows", type=int, default=None)
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    HMMedallionPipeline(
        input_dir=args.input_dir or os.environ.get("HM_DATASET_DIR", "/home/ubuntu/HMDataSet"),
        output_dir=args.output_dir or os.environ.get(
            "DAT535_OUTPUT_DIR", "/home/ubuntu/spark-lab-data/hm"
        ),
        batch_id=args.batch_id,
        limit_rows=args.limit_rows,
        master=args.master,
    ).run()


if __name__ == "__main__":
    main()
