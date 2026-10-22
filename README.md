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

## Medallion layers

### Bronze: preserve and trace

Each source is read as strings and written to Parquet without business
transformation. Every row receives `source_file`, `ingestion_timestamp`, and
`batch_id`. Bronze is organized by dataset and a stable batch marker makes a
rerun with the same batch ID idempotent. A new batch appends data, preserving
history for replay and audit.

### Silver: validate, standardize, and enrich

The Silver grain is one accepted H&M transaction (`t_dat`, customer, article,
price, and sales channel). The pipeline:

- casts dates, identifiers, and prices to explicit types;
- rejects records with invalid dates or IDs, non-positive prices, or unsupported
  sales channels to `silver/rejected_transactions`;
- removes exact duplicate transactions;
- deduplicates article and customer reference records before left joins;
- fills missing descriptive attributes with `UNKNOWN` and derives `age_group`;
- partitions the reusable `master_transactions` output by transaction date.

Each run writes JSON quality evidence under `quality/<batch_id>.json`, including
Bronze, rejected, accepted, Silver, and duplicate-removal counts. This makes
data-quality improvement and rejected-record handling visible rather than
silently coercing bad data.

### Gold: serve a defined outcome

Gold outputs support retail demand and merchandising analysis:

- `daily_sales_demand`: daily transaction count, customers, sales, and average
  price;
- `customer_segment_preferences`: top product and colour preferences by age
  group;
- `inventory_velocity`: sales-day, recency, and dead-stock measures by article;
- `recommendation_features`: customer/article interaction and spend features.

Gold is smaller and purpose-specific while Silver remains reusable. Aggregates
are published only after the Silver build and quality metrics complete.

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

Use `--batch-id` when a run must be reproducible or safely retried. Outputs
default to `DAT535_OUTPUT_DIR` or `~/spark-lab-data/hm`.

## Delivery and operations

The development workflow validates Python source and runs the pipeline on
`dev`; the production workflow does the same on `main`. The scheduled workflow
runs monthly and all workflows upload Bronze, Silver, Gold, rejected records,
and quality evidence as run artifacts. Configure the self-hosted runner with
Spark, Java, PySpark, and `HM_DATASET_DIR` before enabling scheduled execution.

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
   `local[*]` on the runner.
5. If the dataset is elsewhere, create the repository variable
   `HM_DATASET_DIR` with the absolute path. Variables are configured under
   **Settings > Secrets and variables > Actions > Variables**.

Pushes to `dev` run the development workflow. After reviewing the result,
merge to `main` to run the production workflow. The **Pipeline** workflow can
also be started manually from **Actions > Pipeline > Run workflow** and runs
monthly on the first day of the month. Open the completed run to download the
Parquet layers and the JSON quality evidence as artifacts.

Changes should be tested with `python -m py_compile hm_medallion_pipeline.py
run_pipeline.py` and a bounded local run before deployment. Logs include row
counts and quality metrics; failed input discovery, Spark failures, or quality
processing failures fail the workflow instead of producing success-shaped
output.
