# Cineberg

> *Cinema + Iceberg. A movie analytics lakehouse built on open-source tools.*

A local, fully open-source data lakehouse for movie analytics. Ingests real data from TMDB, OMDb, and Reddit, models it into a star schema, serves it through multiple query engines, and layers on AI capabilities including semantic search and text-to-SQL.

The primary goal is to learn the modern on-premises data stack by building something real — not by reading documentation in the abstract.

## Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   TMDB API  │  │  OMDb API   │  │ Reddit API  │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       ▼                ▼                ▼
┌──────────────────────────────────────────────┐
│            Python Ingestion Services          │
│  (batch polling / Redpanda producer)          │
└──────────────┬──────────────┬────────────────┘
               │         stream via Redpanda
          batch load          │
               ▼              ▼
┌──────────────────────────────────────────────┐
│   Apache Spark (PySpark batch + Streaming)    │
└──────────────────────┬───────────────────────┘
                       │ writes Iceberg tables
                       ▼
┌──────────────────────────────────────────────┐
│   MinIO (S3-compatible)  ×  Nessie Catalog    │
│     raw  →  staged  →  modeled (dbt)          │
└───────┬──────────────┬───────────────┬───────┘
        ▼              ▼               ▼
     Trino          Dremio           Spark
        └──────┬────────┘
               ▼
          Metabase (BI)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Table format | Apache Iceberg |
| Catalog | Nessie |
| Object storage | MinIO |
| Batch processing | Apache Spark (PySpark) |
| Interactive query | Trino |
| Alternative query | Dremio Community Edition |
| Streaming | Redpanda |
| Transformation | dbt (dbt-trino) |
| Orchestration | Apache Airflow (optional) |
| BI | Metabase |
| AI / ML | sentence-transformers + Claude API / Ollama |

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- API keys: [TMDB](https://www.themoviedb.org/settings/api), [OMDb](https://www.omdbapi.com/apikey.aspx), [Reddit](https://www.reddit.com/prefs/apps)

### Setup

```bash
# Clone and enter the project
git clone <repo-url> cineberg
cd cineberg

# Copy environment template and fill in your API keys
cp .env.example .env

# Start core services (Step 1: MinIO + Nessie + Spark/Jupyter)
docker compose up -d minio nessie spark

# Install Python dependencies for ingestion
pip install -r ingestion/requirements.txt

# Run first ingestion
python ingestion/tmdb/bulk_load.py
```

Visit:
- **Jupyter**: http://localhost:8888
- **MinIO UI**: http://localhost:9001 (admin / password123)
- **Nessie UI**: http://localhost:19120

### Service Ports

| Service | Port | Introduced |
|---|---|---|
| MinIO API | 9000 | Step 1 |
| MinIO UI | 9001 | Step 1 |
| Nessie | 19120 | Step 1 |
| Spark UI / Jupyter | 8080 / 8888 | Step 1 |
| Trino | 8090 | Step 2 |
| Redpanda | 9092 | Step 5 |
| Dremio | 9047 | Step 7 |
| Metabase | 3000 | Step 8 |

## Project Structure

```
cineberg/
├── docker-compose.yml          # Services (added incrementally per step)
├── .env.example                # Environment variable template
├── infra/                      # Service configuration files
│   ├── spark/
│   ├── trino/
│   ├── dremio/
│   └── airflow/
├── ingestion/                  # Python ingestion scripts
│   ├── tmdb/                   # Steps 1, 2, 3
│   ├── omdb/                   # Step 4
│   └── reddit/                 # Step 5
├── spark/                      # Spark jobs
│   ├── streaming/              # Step 5
│   └── maintenance/            # Step 6
├── dbt_project/                # dbt models (Trino adapter)
│   └── models/
│       ├── staging/
│       ├── marts/
│       └── ml_features/
├── ai/                         # AI services
│   ├── embeddings/             # Step 9
│   ├── text_to_sql/            # Step 10
│   └── sentiment/              # Step 11
├── notebooks/                  # Jupyter exploration notebooks
└── docs/                       # Architecture docs, data dictionary, runbook
```

## Build Plan

The project is built incrementally across 11 steps:

| Step | Goal | New Services |
|---|---|---|
| 1 | First Iceberg table from TMDB | MinIO, Nessie, Spark |
| 2 | Star schema with dbt + Trino | Trino |
| 3 | Incremental ingestion + MERGE INTO | — |
| 4 | OMDb enrichment + schema evolution | — |
| 5 | Reddit streaming via Redpanda | Redpanda |
| 6 | Table maintenance (compaction, expiry) | — |
| 7 | Multi-engine: Trino vs Dremio | Dremio |
| 8 | BI dashboard | Metabase |
| 9 | Semantic movie search (embeddings) | — |
| 10 | Text-to-SQL natural language queries | — |
| 11 | Reddit sentiment vs box office | — |

See [`Ideas/PROJECT_PLAN.md`](Ideas/PROJECT_PLAN.md) for detailed tasks, success criteria, and Iceberg concepts for each step.

## Key Commands

```bash
# Run all dbt models
cd dbt_project && dbt build

# Run dbt tests only
cd dbt_project && dbt test

# Run a specific ingestion script
python ingestion/tmdb/bulk_load.py

# Spark table maintenance
spark-submit spark/maintenance/compact_and_expire.py

# Run AI embedding generation
python ai/embeddings/generate_embeddings.py
```

## Environment Variables

Copy `.env.example` to `.env` and configure:

```env
TMDB_API_KEY=your_tmdb_api_key_here
OMDB_API_KEY=your_omdb_api_key_here
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123
```

## References

- [Apache Iceberg docs](https://iceberg.apache.org/docs/latest/)
- [Nessie docs](https://projectnessie.org/docs/)
- [TMDB API](https://developer.themoviedb.org/docs)
- [dbt-trino adapter](https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
