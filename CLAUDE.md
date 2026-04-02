# Cineberg — Claude Code Instructions

Movie analytics lakehouse: TMDB/OMDb/Reddit APIs → PySpark → Apache Iceberg (MinIO + Nessie) → Trino/Dremio → dbt → Metabase. AI layer: embeddings + text-to-SQL + sentiment.

Full project plan: `Ideas/PROJECT_PLAN.md`

## Key Commands

```bash
# Start core services
docker compose up -d minio nessie spark

# Run ingestion
python ingestion/tmdb/bulk_load.py

# dbt
cd dbt_project && dbt build
cd dbt_project && dbt test
cd dbt_project && dbt run --select staging

# Spark job
spark-submit spark/maintenance/compact_and_expire.py

# AI services
python ai/embeddings/generate_embeddings.py
python ai/text_to_sql/agent.py
```

## Architecture Rules

- **Build incrementally.** Each step adds services — don't add Dremio until Step 7, Redpanda until Step 5, etc.
- **Python for ingestion, SQL (dbt) for transformation.** Never mix these concerns.
- **Always use Iceberg-native operations:** hidden partitioning, MERGE INTO, schema evolution, time travel. No workarounds.
- **dbt-trino adapter only.** Not dbt-spark. Trino is the primary query engine for transformations.
- **PySpark only.** No Scala.
- **Python 3.11+** for all Python code.

## Directory Layout

```
ingestion/          Python ingestion scripts (tmdb/, omdb/, reddit/)
spark/              PySpark batch jobs and streaming sinks
dbt_project/        dbt models: staging/ → marts/ → ml_features/
ai/                 AI services: embeddings/, text_to_sql/, sentiment/
infra/              Service configs: spark/, trino/, dremio/, airflow/
notebooks/          Jupyter exploration notebooks
```

## Ingestion Script Rules

Every ingestion script must:
- Have a module docstring: purpose, step number, dependencies
- Read all secrets from environment variables (never hardcode)
- Be **idempotent** — running twice must not corrupt data
- Handle API rate limits and errors (retry with backoff)
- Use explicit Iceberg schemas (never infer from DataFrame)
- Write to `nessie.<layer>.<table>` (e.g., `nessie.raw.tmdb_movies`)

## dbt Model Rules

- Staging: read `raw.*`, clean types, handle nulls, deduplicate → materialized as `table`
- Marts: read `staging.*`, star schema joins → `table` or `incremental`
- Every model and column needs a YAML description (feeds the text-to-SQL agent)
- All models target the `iceberg` Trino catalog

## Spark + Iceberg + Nessie Config

```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", os.getenv("NESSIE_URI")) \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()
```

## Docker Compose Rules

- Add services incrementally — comment which step introduced each service
- Do not add health checks or complex networking until needed
- Services run on a single machine (16 GB RAM budget: Spark + Dremio are heaviest)
- Network access only needed for TMDB, OMDb, Reddit API calls

## Gotchas

- Trino port is **8090** (not default 8080 — that's Spark UI)
- Nessie API v2: `http://nessie:19120/api/v2`
- OMDb free tier: 1000 requests/day — implement state tracking to resume across days
- Reddit streaming: small-file problem accumulates in Iceberg → run compaction (Step 6)
- `dbt_project/profiles.yml` is gitignored if it contains local paths — use `profiles.yml.example`
