# H&M Data Engineering Learning Project

This project builds a repeatable PySpark pipeline for the H&M Fashion
Transactions dataset. It uses the Medallion Architecture to turn raw
transaction, article, and customer files into trusted transaction data and
consumer-ready retail metrics. Real-time processing, recommendations served
online, and model training are outside the initial scope.

## Source and reproducibility

The input is the Kaggle H&M Fashion Transactions dataset in CSV or Parquet
format. A run expects the following files in `HM_DATASET_DIR`:

```text
transactions_train.csv|parquet
articles.csv|parquet
customers.csv|parquet
```

The dataset is suitable for distributed processing because the transaction
history is large, append-oriented, and joined to reference tables. The source
is external and may contain missing values, invalid identifiers, duplicates, or
schema changes. Download it separately according to its licence; source data
and credentials are intentionally not committed to this repository.

The original source is the [H&M Personalized Fashion Recommendations
competition](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data).
Follow Kaggle's access rules and competition terms; this project does not grant
redistribution rights. The current VM contains pre-converted Parquet files.
Their original conversion procedure/version has not yet been recorded; preserve
that provenance before claiming exact reproduction of the original CSV dataset.
Per-run archives/checksums reproduce the inputs supplied to this pipeline, not
an undocumented prior conversion.

## Medallion layers

### Bronze: preserve and trace

Each source is read as strings and written to Parquet without business
transformation. Every row receives `source_file`, `ingestion_timestamp`, and
`batch_id`, plus a content SHA-256 `source_fingerprint`. Original files are
archived unchanged under `raw/<dataset>/<sha256>/`. Bronze is organized by
dataset and content version. Writes are staged outside Bronze and renamed
into place only after Spark finishes, so interrupted writes are not read on
retry. Already committed contents are skipped even when the batch ID changes.
CSV headers and Parquet columns must match the declared contract; schema
changes fail explicitly and require a reviewed schema update.

This commit protocol supports a local/shared POSIX filesystem, not object-store
URIs. Runs against the same output root take an exclusive lock. Different sample
limits and full datasets cannot share a root. Legacy output layouts require a
fresh root; no old data is deleted or migrated automatically. Changed transaction
snapshots also require a fresh root to prevent overlapping historical purchases
being counted twice. Incremental late-arrival ingestion is not implemented.
Allow disk capacity for the original-file archive as well as processed outputs.

### Silver: validate, standardize, and enrich

The Silver grain is one accepted H&M transaction (`t_dat`, customer, article,
price, and sales channel). The pipeline:

- casts dates, identifiers, and prices to explicit types;
- rejects records with invalid dates or IDs, non-positive prices, or unsupported
  sales channels to `silver/rejected_transactions`;
- preserves identical transaction observations: without a unique purchase ID,
  identical rows can represent genuine repeat purchases;
- removes exact reference duplicates, rejects conflicting values within a
  reference version, and selects the latest ingested version per key;
- fills missing descriptive attributes with `UNKNOWN` and derives `age_group`;
- partitions the reusable `master_transactions` output by transaction date;
- retains transaction lineage, adds calendar fields and reference-match flags,
  and treats blank text and invalid/out-of-range ages as unknown.

Each run writes JSON quality evidence under `quality/<batch_id>.json`, including
Bronze, rejected, accepted, Silver, unmatched-reference, invalid-Silver, and
duplicate-removal counts. Duplicate removal is now zero by policy. Non-empty
Silver, valid required fields and exact input/accepted/rejected accounting are
enforced before Gold. Missing reference matches are flagged and counted, not
rejected, so accepted transactions are retained.

### Gold: serve a defined outcome

Gold outputs support retail demand and merchandising analysis:

- `daily_sales_demand`: daily transaction count, customers, sales, and average
  price;
- `customer_segment_preferences`: top product and colour preferences by age
  group;
- `inventory_velocity`: sales-day, recency, actual final-30-day purchase counts
  and no-recent-sales indicators by article, including never-sold articles;
- `recommendation_features`: customer/article interaction and spend features.

Gold is purpose-specific while Silver remains reusable; the interaction-feature
table is not necessarily small. Daily, recommendation and inventory counts are
reconciled to Silver, and daily/recommendation spend totals must match exactly.
The final 30-day window includes the dataset maximum date and the preceding
29 days, not the current calendar date. "Deadstock" is a no-observed-recent-sales
proxy, not evidence of stock on hand. Source price units must not be presented as
currency revenue without verifying the source's normalization.

Consumers must require `_READY.json`, written atomically only after every Gold
write and reconciliation succeeds. It is revoked at the start of a rerun.
Individual Parquet tables are not an atomic multi-table transaction. Prefer
immutable per-run roots (as in Actions), and consume only completed runs.

## Run locally

```bash
cd /home/ubuntu/group15
source ~/spark-env/bin/activate
export HM_DATASET_DIR=/path/to/HMDataSet
python run_pipeline.py hm
```

For a bounded development run:

```bash
python run_pipeline.py hm --limit-rows 1000 --output-dir /tmp/hm-smoke
```

Use `--batch-id` to label lineage and quality reports. Retry safety is based on
committed source contents, not the label alone. Outputs
default to `DAT535_OUTPUT_DIR` or `~/spark-lab-data/hm`.

## Delivery and operations

The development workflow validates Python source and runs the pipeline on
`dev`; the production workflow does the same on `main`. The scheduled workflow
runs monthly and all workflows upload Bronze, Silver, Gold, rejected records,
and quality evidence as run artifacts. Configure the self-hosted runner with
Spark, Java, PySpark, and `HM_DATASET_DIR` before enabling scheduled execution.

The `dev` workflow intentionally processes 100,000 rows per source by default
so it fits the small self-hosted VM. Set the repository/environment variable
`HM_DEV_LIMIT_ROWS` to change the development sample size. Full-dataset runs
should use the production workflow on a larger or cluster-backed Spark
resource.

### GitHub Actions setup

These workflows use a **self-hosted** GitHub Actions runner because the dataset
is local to the Spark machine. On the machine that has the dataset and Spark:

1. In GitHub, open **Settings > Actions > Runners > New self-hosted runner**,
   follow the generated commands, and add the runner to this repository.
2. Install Java, Spark, PySpark, and `findspark`; verify that
   `~/spark-env/bin/python` can import `pyspark`.
3. Put the source files in the runner's configured dataset directory:
   `/home/ubuntu/HMDataSet` by default.
4. If using a Spark standalone cluster, start the master and worker and create
   a repository variable named `SPARK_MASTER_URL`, for example
   `spark://spark-host:7077`. Without that variable, the workflow uses
   `local[2]` on the runner to keep memory use bounded.
5. If the dataset is elsewhere, create the repository variable
   `HM_DATASET_DIR` with the absolute path. Variables are configured under
   **Settings > Secrets and variables > Actions > Variables**.

Pushes to `dev` run the development workflow. After reviewing the result,
merge to `main` to run the production workflow. The **Pipeline** workflow can
also be started manually from **Actions > Pipeline > Run workflow** and runs
monthly on the first day of the month. Open the completed run to download the
Parquet layers and the JSON quality evidence as artifacts.

Changes should be tested with `python -m py_compile hm_medallion_pipeline.py
run_pipeline.py` and the synthetic regression suite before deployment:

```bash
source ~/spark-env/bin/activate
SPARK_DRIVER_MEMORY=512m SPARK_EXECUTOR_MEMORY=512m SPARK_SHUFFLE_PARTITIONS=2 \
  PYSPARK_SUBMIT_ARGS='--driver-memory 512m pyspark-shell' \
  python -m unittest discover -s tests -v
```

The tests use temporary synthetic CSV/Parquet inputs, not the H&M dataset.
All workflows run this suite before processing data and share a concurrency
group to avoid overlapping Actions jobs on the small runner. Dev and production
upload available quality reports even on failure. This does not serialize
manually started jobs against different output directories.

Run a bounded real-data batch in a fresh directory after VM cleanup. Full-scale
production validation and measured performance tuning remain outstanding.
Logs include row
counts and quality metrics; failed input discovery, Spark failures, or quality
processing failures fail the workflow instead of producing success-shaped
output.
