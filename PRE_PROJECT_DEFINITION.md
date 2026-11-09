# H&M Fashion Analytics Pipeline
## Project definition check-in

### 1. Project statement and scope

We will build a batch PySpark pipeline that transforms
H&M Fashion Transactions data into reusable datasets for retail
analysis. The business need is to understand customer purchasing preferences,
identify high-velocity and slow-moving products, and provide reliable inputs
for recommendation analysis and merchandising dashboards.

The outcome is a Medallion Architecture pipeline that
produces:

- a traceable Bronze copy of the source data
- a validated, enriched Silver transaction dataset 
- focused Gold datasets for customer preferences, product velocity, and
  recommendation features.

The primary users are data analysts, merchandising teams, and a dashboard or
analytical model consuming the Gold outputs. 

### 2. Dataset source and suitability

The project uses the H&M Personalized Fashion Recommendations dataset,
consisting of `transactions_train`, `articles`, and `customers` source files.
The files are available as CSV or Parquet; the pipeline supports both. The
transaction history contains more than 31 million records, while the article
and customer files provide product and customer reference data. The volume,
transaction structure, and the required distributed joins
make the dataset suitable for Spark processing.

Transactions describe a customer purchase, including date, customer ID,
article ID, price, and sales channel. Articles provide product, garment,
colour, department, and descriptive attributes. Customers provide age,
membership, news-frequency, and postal-code attributes. The source may contain
missing values, duplicate records, invalid identifiers, inconsistent text,
and future schema changes. Customer identifiers and demographic data also
require careful handling because they are sensitive business data.

### 3. Bronze layer: ingest and preserve

Bronze will ingest the three source files with Spark in batch mode. The raw
fields will initially be preserved as strings so that malformed values are
not silently discarded during ingestion. Each source will be written to
Parquet under a dataset-specific path:

```text
bronze/transactions
bronze/articles
bronze/customers
```

Every row will receive `source_file`, `ingestion_timestamp`, and `batch_id`.
The original source values will not be cleaned or joined in Bronze. A stable
batch identifier and completion marker will prevent accidental duplicate
ingestion when a workflow is retried. New batches append to the raw history,
allowing later replay and audit. Source discovery and required-file checks will
fail the run explicitly when inputs are missing.

GitHub Actions will validate the Python source and execute the ingestion on a
self-hosted Spark-capable runner. Development runs will use the `dev` branch;
approved changes will run in production from `main`; a scheduled workflow will
provide the regular batch trigger. The dataset location, Spark master, and
output location will be environment-specific configuration rather than
hard-coded source data in Git.

### 4. Silver layer: standardize, validate, and enrich

Silver is the primary focus. Its core grain is one accepted H&M transaction:
one purchase identified by transaction date, customer, article, price, and
sales channel. The transaction table is the fact-like core, filled with
deduplicated article and customer reference records. Silver will retain
transaction-level detail so it can support several downstream analyses rather
than only one dashboard.

The transformation design is:

1. Parse the transaction date and cast article IDs, sales channels, customer
   attributes, and prices to explicit types.
2. Trim identifiers and text fields and standardize missing descriptive
   attributes to an `UNKNOWN` value.
3. Validate dates, customer IDs, article IDs, positive prices, and supported
   sales-channel values.
4. Remove exact duplicate transaction records.
5. Deduplicate reference records before joining them to transactions, avoiding
   accidental row multiplication.
6. Derive reusable attributes such as `age_group` and calendar fields.
7. Write accepted records as date-partitioned Parquet in
   `silver/master_transactions`.
8. Write rejected records with a rejection reason to
   `silver/rejected_transactions` rather than silently dropping them.

Quality evidence will be generated for every run. It will compare Bronze input
with accepted and rejected records and record the number of exact duplicates
removed. Required checks include schema availability, accepted-row counts,
rejected-row counts, duplicate counts, non-null key fields, positive prices,
valid dates, and valid sales channels. Silver outputs should be published only
after these transformations and checks complete successfully.

The implementation will use Spark DataFrame functions for parsing, filtering,
deduplication, joins, and aggregations, with Spark SQL-compatible expressions
where they improve clarity. Expensive joins and aggregations will be done
using Spark stage and task metrics. Evidence-based tuning may include early
filtering, selecting only required columns, date partitioning, adaptive query
execution, appropriate shuffle partitions, and broadcasting only genuinely
small reference data. No optimization will be claimed without measuring its
effect.

The design scales by processing Parquet rather than repeatedly scanning raw
CSV, partitioning Silver by transaction date, and allowing later batches to be
identified independently. Late-arriving data can be rerun by batch and Silver
can be rebuilt from Bronze. If source schemas evolve, the ingestion schema and
validation rules will be versioned and changed through review. Tests and
GitHub Actions checks will validate transformations and schemas before
deployment.

### 5. Gold layer: serve the analytical outcome

Gold will provide smaller, purpose-specific outputs for retail demand and
merchandising analysis. The initial consumer is a dashboard or analytical
model answering questions such as: which products and colours are preferred
by customer age group, which articles sell quickly, which products show
dead-stock risk, and which customer/article interactions can support
recommendation experiments?

The planned outputs are:

- `customer_segment_preferences`: top garment-group and colour preferences by
  age group, including units, distinct customers, and distinct articles
- `inventory_velocity`: units sold, sales days, last sale date, recent
  activity, and a dead-stock indicator by article
- `recommendation_features`: customer/article interaction counts, spend, and
  most recent purchase date
- `daily_sales_demand`: daily transaction count, unique customers, total sales,
  and average price.

Silver remains the reusable transaction-level source; Gold is intentionally
smaller and optimized for direct consumption. Gold aggregates will be written
only after Silver succeeds, and their row counts and measures will be checked
against Silver totals where applicable. A failed upstream quality run will
prevent a successful Gold publication.

### 6. End-to-end delivery and ownership

Another engineer should be able to clone the repository, configure the source
and output paths, activate the documented Spark environment, and run a bounded
development batch or the full pipeline. Development and production use
separate output locations and GitHub Actions environments. The repository
contains the pipeline source, workflow files, configuration conventions, and
run instructions; source data and credentials remain outside version control.

Automated checks will compile the Python source, verify required inputs,
execute the pipeline, and upload Bronze, Silver, Gold, rejected records, and
quality metrics as workflow artifacts. Logs will show the Spark environment,
batch ID, row counts, rejected records, and quality results. A failed input
check, Spark job, transformation, or quality gate will fail the workflow
instead of producing a success-shaped result.

