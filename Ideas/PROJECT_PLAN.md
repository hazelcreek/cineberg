# Cineberg — Project Plan

> *Cinema + Iceberg. A movie analytics lakehouse built on open-source tools.*

## Overview

Build a local, fully open-source **data lakehouse** for movie analytics using **Apache Iceberg** as the table format. The project ingests real data from TMDB, OMDb, and Reddit, models it into a star schema, serves it through multiple query engines, and layers on AI capabilities including semantic search and text-to-SQL.

The primary goal is to learn the modern on-premises data stack by building something real and interesting — not by reading documentation in the abstract.

### Core Technologies

| Layer | Technology | Purpose |
|---|---|---|
| Table format | Apache Iceberg | Open table format with schema evolution, time travel, hidden partitioning |
| Catalog | Nessie | Git-like REST catalog for Iceberg with branch/tag support |
| Object storage | MinIO | S3-compatible local object store |
| Batch processing | Apache Spark (PySpark) | Bulk ingestion, transformations, ML workloads |
| Interactive query | Trino | Fast federated SQL engine |
| Alternative query | Dremio Community Edition | Lakehouse platform with reflections and built-in catalog |
| Streaming | Redpanda | Lightweight Kafka-compatible event streaming |
| Transformation | dbt (dbt-trino) | SQL-based data modeling and documentation |
| Orchestration | Apache Airflow (optional) | DAG-based scheduling for ingestion and maintenance |
| BI | Metabase | Dashboards and visual analytics |
| AI / ML | Python + sentence-transformers + LLM (Claude API or Ollama) | Embeddings, sentiment, text-to-SQL |

### Data Sources

| Source | Type | Access | What it provides |
|---|---|---|---|
| TMDB API | REST API (free key, non-commercial) | `api.themoviedb.org/3` | Movies, TV, people, credits, genres, companies, images |
| OMDb API | REST API (free tier: 1000 req/day) | `omdbapi.com` | Rotten Tomatoes, Metacritic, MPAA ratings, IMDB data |
| Reddit API | REST API (free with OAuth) | `oauth.reddit.com` | Posts and comments from movie subreddits |

---

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
               │              │
          batch load     stream via
               │         Redpanda
               ▼              ▼
┌──────────────────────────────────────────────┐
│              Apache Spark                     │
│  (PySpark batch / Structured Streaming)       │
└──────────────────────┬───────────────────────┘
                       │ writes Iceberg
                       ▼
┌──────────────────────────────────────────────┐
│                   MinIO                       │
│          (S3-compatible object store)         │
│   ┌──────────────────────────────────┐       │
│   │     Iceberg Tables (Parquet)     │       │
│   │  ┌────────┐ ┌────────┐ ┌──────┐ │       │
│   │  │  raw   │ │ staged │ │modeled│ │       │
│   │  └────────┘ └────────┘ └──────┘ │       │
│   └──────────────────────────────────┘       │
└──────────────────────┬───────────────────────┘
                       │ registered in
                       ▼
┌──────────────────────────────────────────────┐
│               Nessie Catalog                  │
│         (REST catalog, git-like branches)     │
└───────┬──────────────┬───────────────┬───────┘
        │              │               │
        ▼              ▼               ▼
┌────────────┐ ┌─────────────┐ ┌────────────┐
│   Trino    │ │   Dremio    │ │   Spark    │
│  (query)   │ │  (query)    │ │  (query)   │
└─────┬──────┘ └──────┬──────┘ └────────────┘
      │               │
      ▼               ▼
┌──────────────────────────────────────────────┐
│              Metabase (BI)                    │
│         connected via Trino JDBC              │
└──────────────────────────────────────────────┘
```

### Medallion / Layered Architecture

All Iceberg tables follow a three-layer convention:

- **raw** — data as received from source APIs, minimal transformation (JSON flattened to tabular). Append-only or full-refresh depending on source.
- **staged** — cleaned, deduplicated, typed. One staged table per source entity.
- **modeled** — star schema with fact and dimension tables, built by dbt. This is the layer BI and AI consume.

---

## Directory Structure

```
cineberg/
├── README.md
├── PROJECT_PLAN.md              ← this file
├── docker-compose.yml
├── .env.example                 ← API keys template (TMDB_API_KEY, OMDB_API_KEY, etc.)
├── .gitignore
│
├── infra/                       ← infrastructure config
│   ├── spark/
│   │   └── spark-defaults.conf
│   ├── trino/
│   │   ├── catalog/
│   │   │   └── iceberg.properties
│   │   └── config.properties
│   ├── dremio/                  ← (step 7)
│   ├── metabase/                ← (step 8)
│   └── airflow/                 ← (optional)
│       └── dags/
│
├── ingestion/                   ← Python ingestion scripts
│   ├── requirements.txt
│   ├── tmdb/
│   │   ├── bulk_load.py         ← step 1: initial full load
│   │   ├── incremental.py       ← step 3: daily delta
│   │   └── credits.py           ← step 2: people and credits
│   ├── omdb/
│   │   └── enrich.py            ← step 4: ratings enrichment
│   └── reddit/
│       └── producer.py          ← step 5: Redpanda producer
│
├── spark/                       ← Spark jobs
│   ├── streaming/
│   │   └── reddit_sink.py       ← step 5: streaming sink
│   └── maintenance/
│       └── compact_and_expire.py← step 6: table maintenance
│
├── dbt_project/                 ← dbt models
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_tmdb_movies.sql
│   │   │   ├── stg_tmdb_people.sql
│   │   │   ├── stg_tmdb_credits.sql
│   │   │   ├── stg_omdb_ratings.sql
│   │   │   └── stg_reddit_posts.sql
│   │   ├── marts/
│   │   │   ├── fact_movies.sql
│   │   │   ├── fact_reddit_mentions.sql
│   │   │   ├── dim_people.sql
│   │   │   ├── dim_genres.sql
│   │   │   ├── dim_companies.sql
│   │   │   ├── dim_dates.sql
│   │   │   └── bridge_movie_people.sql
│   │   └── ml_features/         ← step 9+
│   │       ├── movie_embeddings.sql
│   │       └── reddit_sentiment.sql
│   └── tests/
│
├── ai/                          ← AI services
│   ├── requirements.txt
│   ├── embeddings/
│   │   └── generate_embeddings.py   ← step 9
│   ├── text_to_sql/
│   │   └── agent.py                 ← step 10
│   └── sentiment/
│       └── analyze.py               ← step 11
│
├── notebooks/                   ← exploratory Jupyter notebooks
│   ├── 01_explore_iceberg.ipynb
│   ├── 02_star_schema.ipynb
│   └── 03_ml_experiments.ipynb
│
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    └── runbook.md               ← operational playbook
```

---

## Step-by-Step Build Plan

### Step 1 — Local Foundation: First Iceberg Table

**Goal:** Write and read your first Iceberg table from real TMDB data.

**Services:** MinIO, Nessie, Spark (via Jupyter notebook).

**Tasks:**

1. Create `docker-compose.yml` with MinIO, Nessie, and a PySpark Jupyter container.
2. Configure Spark to use the Nessie REST catalog with MinIO as the warehouse.
3. Register for a TMDB API key at `https://www.themoviedb.org/settings/api`.
4. Write `ingestion/tmdb/bulk_load.py`:
   - Paginate through TMDB's `/discover/movie` endpoint (sort by popularity descending).
   - Collect fields: `id`, `title`, `original_title`, `original_language`, `overview`, `release_date`, `genre_ids`, `budget`, `revenue`, `vote_average`, `vote_count`, `popularity`, `poster_path`, `adult`.
   - For each movie, also call `/movie/{id}` to get `budget`, `revenue`, `imdb_id`, `runtime`, `status`, `production_companies`, `production_countries`, `spoken_languages`.
   - Write to Iceberg table `raw.tmdb_movies` partitioned by `release_year` (use Iceberg's hidden partitioning via a year transform on `release_date`).
5. Use the Jupyter notebook to query the table with Spark SQL and inspect results.
6. **Manual exploration:** use MinIO's web UI (port 9001) to browse the `warehouse/` bucket. Navigate into the Iceberg metadata directory and examine the `metadata.json`, manifest lists, and manifests. Understand the snapshot model.

**Success criteria:** You can run `SELECT * FROM raw.tmdb_movies WHERE release_year = 2024 LIMIT 10` in Spark SQL and get results. You understand the file layout in MinIO.

**Key Iceberg concepts:** hidden partitioning, snapshots, manifest files, Parquet data files.

---

### Step 2 — Dimensional Model: Star Schema in dbt

**Goal:** Build a clean star schema on top of raw data using dbt and Trino.

**New services:** Trino (pointed at same Nessie catalog).

**Tasks:**

1. Add Trino to `docker-compose.yml` with an Iceberg catalog connector pointing at Nessie/MinIO.
2. Verify Trino can read the table written by Spark in step 1.
3. Write `ingestion/tmdb/credits.py`:
   - For each movie, call `/movie/{id}/credits` to get cast (actor, character, order) and crew (name, job, department).
   - Write to `raw.tmdb_credits`.
4. Pull genre and company reference data into `raw.tmdb_genres` and `raw.tmdb_companies`.
5. Initialize the dbt project with `dbt-trino` adapter.
6. Build staging models:
   - `stg_tmdb_movies` — clean types, handle nulls, extract `imdb_id`.
   - `stg_tmdb_people` — deduplicate actors/directors from credits.
   - `stg_tmdb_credits` — normalize the movie-person relationship.
7. Build mart models:
   - `dim_people` — unique people with name, known_for_department.
   - `dim_genres` — genre id and name.
   - `dim_companies` — production company id, name, origin_country.
   - `dim_dates` — date spine with year, quarter, month, day_of_week.
   - `fact_movies` — one row per movie with foreign keys to all dimensions, plus measures (budget, revenue, vote_average, vote_count, popularity, runtime).
   - `bridge_movie_people` — many-to-many bridge with role (actor/director/writer), character name, billing order.
8. Add dbt tests: unique keys, not-null constraints, referential integrity.
9. Add dbt docs: descriptions for every model and column (these become context for the text-to-SQL agent later).

**Success criteria:** `dbt build` runs cleanly. You can query `SELECT d.name, AVG(f.revenue) FROM mart.fact_movies f JOIN mart.bridge_movie_people bp ON ... JOIN mart.dim_people d ON ... GROUP BY 1 ORDER BY 2 DESC LIMIT 10` from Trino and get meaningful results.

**Key Iceberg concepts:** schema evolution (add columns to raw tables without rewriting), Trino + Spark coexistence on same catalog.

---

### Step 3 — Incremental Ingestion: Daily Deltas

**Goal:** Move from one-time bulk load to repeatable incremental ingestion.

**Tasks:**

1. Write `ingestion/tmdb/incremental.py`:
   - Call TMDB's `/movie/changes` endpoint to get movie IDs updated in the last 24 hours.
   - Fetch full details for each changed movie.
   - Use Spark's `MERGE INTO` (Iceberg v2) to upsert into `raw.tmdb_movies` — insert new movies, update changed ones.
2. Create a small metadata table `raw.ingestion_log` tracking each run's timestamp, record count, and status.
3. Add a dbt incremental model for `stg_tmdb_movies` using Iceberg's time-travel: `SELECT * FROM raw.tmdb_movies FOR TIMESTAMP AS OF '...'` to process only new snapshots.
4. (Optional) Wrap this in an Airflow DAG with a daily schedule.

**Success criteria:** Run the incremental script twice on consecutive days. Verify via time travel that you can see the table state before and after each run: `SELECT * FROM raw.tmdb_movies FOR TIMESTAMP AS OF TIMESTAMP '2025-01-01 00:00:00'`.

**Key Iceberg concepts:** MERGE INTO (upsert), time travel, snapshot isolation, incremental materialization.

---

### Step 4 — Multi-Source Enrichment: OMDb Ratings

**Goal:** Enrich movies with external ratings from a second API.

**Tasks:**

1. Register for an OMDb API key at `https://www.omdbapi.com/apikey.aspx`.
2. Write `ingestion/omdb/enrich.py`:
   - For each movie with an `imdb_id`, call OMDb to get Rotten Tomatoes score, Metacritic score, MPAA rating (Rated), awards text, and box office string.
   - Write to `raw.omdb_ratings`.
   - Respect rate limits (1000/day free tier) — implement batching with state tracking to resume across days.
3. Build `stg_omdb_ratings` in dbt — parse percentage strings to numeric, clean up MPAA categories.
4. Extend `fact_movies` to join in OMDb fields via `imdb_id`.
5. Handle the schema evolution: the fact table now has new columns that didn't exist before. Verify Iceberg handles this without rewriting historical data.

**Success criteria:** `fact_movies` now includes Rotten Tomatoes and Metacritic scores. Historical snapshots still work — querying the table as of before the OMDb enrichment returns rows without those columns (nulls).

**Key Iceberg concepts:** schema evolution (adding columns), multi-source joining, late-arriving dimensions.

---

### Step 5 — Streaming Layer: Reddit Buzz

**Goal:** Add a real-time streaming data source.

**New services:** Redpanda.

**Tasks:**

1. Add Redpanda to `docker-compose.yml`.
2. Register a Reddit app for API access at `https://www.reddit.com/prefs/apps`.
3. Write `ingestion/reddit/producer.py`:
   - Poll `/r/movies/new`, `/r/boxoffice/new`, `/r/moviesuggestions/new` every 60 seconds.
   - For each post: extract `id`, `title`, `selftext`, `author`, `subreddit`, `score`, `num_comments`, `created_utc`, `url`.
   - Produce as JSON to Redpanda topic `reddit.movie_posts`.
4. Write `spark/streaming/reddit_sink.py`:
   - Spark Structured Streaming job reading from Redpanda topic.
   - Write to `raw.reddit_posts` as an append-only Iceberg table, partitioned by ingestion date.
5. Build `stg_reddit_posts` in dbt.
6. Build `fact_reddit_mentions` that attempts to link posts to movies via fuzzy title matching (use Trino's `LIKE` or Levenshtein functions, or a Python UDF for better matching).

**Success criteria:** Posts appear in the Iceberg table within seconds of being published to Reddit. The streaming job handles Redpanda restarts gracefully via checkpointing.

**Key Iceberg concepts:** streaming append to Iceberg, small file problem (many tiny files from micro-batches), checkpoint-based exactly-once semantics.

---

### Step 6 — Table Maintenance

**Goal:** Solve the operational problems that accumulate over time.

**Tasks:**

1. Write `spark/maintenance/compact_and_expire.py`:
   - **Compaction:** rewrite small data files into larger ones (target 128–256 MB per file). Focus on the Reddit streaming table which accumulates many tiny files.
   - **Snapshot expiration:** retain only the last 7 days of snapshots. Older snapshots are no longer needed for time travel.
   - **Orphan file cleanup:** remove data files that are no longer referenced by any snapshot.
   - **Sort order optimization:** optionally rewrite `fact_movies` sorted by `release_date` for better query performance on time-range filters.
2. Schedule this as a weekly job (cron, Airflow, or a simple bash script).
3. Measure before/after: count data files, total storage size, and query latency on a representative query.

**Success criteria:** The Reddit table goes from hundreds of tiny files to a handful of well-sized ones. Query latency improves measurably. Storage usage decreases after orphan cleanup.

**Key Iceberg concepts:** compaction, snapshot expiration, orphan file removal, sort order optimization.

---

### Step 7 — Multi-Engine Queries: Trino vs Dremio

**Goal:** Experience the engine-agnostic promise of Iceberg.

**New services:** Dremio Community Edition.

**Tasks:**

1. Add Dremio to `docker-compose.yml`.
2. Configure Dremio to use Nessie as its catalog source, pointed at the same MinIO bucket.
3. Run the same set of benchmark queries on both engines:
   - Simple aggregation: average revenue by genre.
   - Complex join: top 10 directors by ROI on films with budget > $10M.
   - Time-range scan: all films released in Q4 2024 with their Reddit mention count.
4. Compare: query plans, execution time, developer experience.
5. Experiment with Dremio reflections — create a reflection on `fact_movies` aggregated by genre and year, then rerun the aggregation query and observe the speedup.
6. (Optional) Try DuckDB as a third engine — use PyIceberg to read the Iceberg table metadata and DuckDB to query the underlying Parquet files directly.

**Success criteria:** Identical analytical results from Trino and Dremio on the same Iceberg tables. You can articulate the trade-offs between the two engines.

**Key Iceberg concepts:** catalog-level interoperability, engine-agnostic format, query optimization strategies.

---

### Step 8 — BI Dashboard

**Goal:** Make insights visual and explorable.

**New services:** Metabase.

**Tasks:**

1. Add Metabase to `docker-compose.yml`, connected to Trino via JDBC.
2. Build dashboard pages:
   - **Release timeline:** bar chart of movie count by month, colored by primary genre.
   - **Revenue analysis:** scatter plot of budget vs. revenue with a diagonal break-even line. Filter by genre, year range.
   - **Ratings comparison:** grouped bar chart comparing TMDB, Rotten Tomatoes, and Metacritic scores for top films.
   - **Reddit buzz tracker:** line chart of Reddit mention volume over time for user-selected movies.
   - **Director/actor leaderboards:** tables with sortable columns for average revenue, average rating, film count.
3. Add interactive filters: genre multi-select, year range slider, minimum vote count.
4. (Optional) Publish the dashboard with a shareable link for your portfolio.

**Alternative BI tools (pick one):**

| Tool | Strength | Best for |
|---|---|---|
| Metabase | Simple setup, great UX | Quick dashboards, non-technical users |
| Apache Superset | Feature-rich, open source | Complex visualizations, SQL Lab |
| Evidence | BI-as-code (markdown + SQL) | Developer-native, git-versioned dashboards |
| Rill | Fast exploration, DuckDB-native | Rapid prototyping, local analysis |

**Success criteria:** A polished, interactive dashboard that someone unfamiliar with the project could explore and understand.

---

### Step 9 — AI: Semantic Movie Search

**Goal:** Enable natural language movie discovery via embeddings.

**Tasks:**

1. Write `ai/embeddings/generate_embeddings.py`:
   - Read movie overviews from `stg_tmdb_movies` via PyIceberg or Spark.
   - Generate embeddings using `sentence-transformers/all-MiniLM-L6-v2` (small, fast, runs locally).
   - Write embeddings to `ml_features.movie_embeddings` as an Iceberg table with columns: `movie_id`, `embedding` (array of floats), `model_version`, `generated_at`.
2. Build a small search service (FastAPI or a notebook):
   - Accepts a natural language query.
   - Embeds the query with the same model.
   - Loads embeddings from the Iceberg table.
   - Computes cosine similarity, returns top 10 matches with title, overview, poster URL, and similarity score.
3. (Optional) Add a dbt model `ml_features.movie_embeddings` that joins embedding metadata with the movie dimension for a single query-friendly view.
4. (Optional) Compare results with a more powerful model (e.g., `all-mpnet-base-v2`) and store both versions using `model_version` column to track lineage.

**Success criteria:** Query "feel-good movies about cooking" returns relevant results like *Julie & Julia*, *Ratatouille*, *The Hundred-Foot Journey*.

**Key concepts:** embeddings as lakehouse-native feature tables, model versioning, vector similarity search without a dedicated vector DB.

---

### Step 10 — AI: Text-to-SQL Movie Analyst

**Goal:** Ask natural language questions about your lakehouse and get SQL-backed answers.

**Tasks:**

1. Write `ai/text_to_sql/agent.py`:
   - On startup, read schema metadata from Nessie catalog (table names, column names, types).
   - Read dbt documentation YAML files for human-readable descriptions.
   - Build a system prompt with the schema and descriptions.
2. Implement the query loop:
   - User asks a question in natural language.
   - LLM generates a SQL query targeting the `mart` schema.
   - Agent executes the SQL against Trino (via `trino` Python client).
   - LLM interprets the results and generates a natural language answer.
3. Add guardrails: validate generated SQL is read-only (no INSERT/UPDATE/DELETE), add a timeout, handle errors gracefully and re-prompt.
4. Test with increasingly complex questions:
   - Simple: "How many movies were released in 2023?"
   - Medium: "What's the highest-rated sci-fi movie with over 1000 votes?"
   - Complex: "Compare average Rotten Tomatoes scores for Christopher Nolan vs Denis Villeneuve films."
   - Cross-source: "Which movies had the most Reddit mentions relative to their budget?"
5. (Optional) Add a simple chat UI (Streamlit or Gradio).

**LLM options:**

| Option | Pros | Cons |
|---|---|---|
| Claude API | High quality SQL generation, tool use | Requires API key, costs money |
| Ollama (local, e.g. Codestral, Qwen2.5-Coder) | Free, private, no network needed | Lower quality on complex joins |
| OpenAI API | Good SQL quality | Requires API key, costs money |

**Success criteria:** The agent correctly answers at least 8 out of 10 test questions, generating valid SQL and interpreting results accurately.

**Key concepts:** LLM + structured data integration, schema-as-context, SQL validation, the importance of good dbt documentation.

---

### Step 11 — AI: Sentiment Analysis Meets Box Office

**Goal:** Explore whether Reddit sentiment predicts box office performance.

**Tasks:**

1. Write `ai/sentiment/analyze.py`:
   - Read Reddit posts from `stg_reddit_posts`.
   - Run sentiment analysis using a lightweight model (e.g., `cardiffnlp/twitter-roberta-base-sentiment-latest` via HuggingFace, or few-shot prompts to a local LLM).
   - Write results to `ml_features.reddit_sentiment`: `post_id`, `movie_id`, `sentiment_score` (-1 to 1), `sentiment_label` (positive/neutral/negative), `model_version`, `scored_at`.
2. Build a dbt model that aggregates daily sentiment per movie: avg score, volume of mentions, ratio of positive to negative.
3. Join with `fact_movies` revenue data and build a simple analysis notebook:
   - Scatter plot: pre-release average sentiment vs. opening weekend revenue.
   - Time series: sentiment trajectory in the weeks before and after release.
   - Correlation analysis: does Reddit buzz volume or sentiment better predict revenue?
4. (Optional) Train a simple regression model (scikit-learn) predicting opening weekend revenue from features: budget, genre, director historical average, Reddit sentiment, Reddit volume. Write predictions to `ml_features.revenue_predictions`.
5. Add a panel to the Metabase dashboard showing sentiment trends.

**Success criteria:** You can show a visualization of sentiment vs. revenue correlation, even if the correlation is weak (that's a valid finding). The full pipeline from Reddit ingestion → sentiment scoring → Iceberg storage → dbt model → dashboard works end-to-end.

**Key concepts:** ML read/write loop on lakehouse, feature engineering in dbt, model predictions as first-class Iceberg tables.

---

## Optional Extensions

These are independent enhancements you can pursue in any order after completing the core steps.

### Extension A — Nessie Branching Workflow

Use Nessie's git-like capabilities to create a development branch, run experimental dbt model changes on the branch, validate results, and merge back to main. This simulates a real data engineering workflow where schema changes are tested before going to production.

**Tasks:**
- Create a branch `feature/add-tv-shows` in Nessie.
- Ingest TMDB TV show data on the branch.
- Run dbt models on the branch.
- Compare results between branches.
- Merge when satisfied.

### Extension B — TV Shows Expansion

Extend the project to cover TV shows using TMDB's `/tv` endpoints. This exercises schema evolution (TV has seasons and episodes, different from movies), and the dimensional model needs to accommodate both entity types.

### Extension C — Data Quality with Great Expectations or Soda

Add a data quality layer that validates incoming data before it enters the modeled layer. Check for: revenue values that are clearly wrong (e.g., $1), release dates in the future, duplicate movie IDs, missing IMDB IDs. Fail the dbt build if quality checks don't pass.

### Extension D — Slowly Changing Dimensions

Track how movie attributes change over time (e.g., vote_average, vote_count, popularity). Implement a Type 2 SCD for `dim_movies` that preserves history. This is a classic data warehousing pattern that Iceberg makes easier with its merge and time-travel capabilities.

### Extension E — Data Lineage and Observability

Add OpenLineage integration to track data lineage across Spark jobs and dbt models. Visualize the lineage graph to see how data flows from raw APIs to the final dashboard. Marquez is an open-source metadata server that works with OpenLineage.

### Extension F — Iceberg REST Catalog (Polaris)

Replace Nessie with Apache Polaris (Snowflake's open-sourced REST catalog) to compare the two catalog implementations. Migrate your tables from one catalog to another and verify everything still works.

### Extension G — Apache Flink as an Alternative Streaming Engine

Replace the Spark Structured Streaming job with an Apache Flink job for the Reddit ingestion. Compare the two approaches in terms of latency, checkpointing, and developer experience.

### Extension H — Image Embeddings for Poster Analysis

Download movie poster images via TMDB's image API. Generate image embeddings using a CLIP model. Enable multi-modal search: find movies with similar visual aesthetics, or search by text description and match against poster imagery.

### Extension I — GraphQL API Layer

Build a GraphQL API (e.g., with Strawberry or Hasura) on top of your Trino-backed star schema. This simulates how a production application might serve lakehouse data to a frontend.

### Extension J — Cost and Performance Benchmarking

Systematically benchmark different Iceberg configurations: partition strategies, file sizes, sort orders, Parquet compression codecs. Document the performance impact of each choice with reproducible benchmarks.

---

## Docker Compose Service Reference

Below is a summary of services and the step at which they are introduced. Build `docker-compose.yml` incrementally — only add services when their step arrives.

| Service | Image | Ports | Introduced |
|---|---|---|---|
| minio | `minio/minio` | 9000 (API), 9001 (UI) | Step 1 |
| nessie | `projectnessie/nessie` | 19120 | Step 1 |
| spark | `bitnami/spark` or custom | 8080 (UI), 8888 (Jupyter) | Step 1 |
| trino | `trinodb/trino` | 8090 | Step 2 |
| redpanda | `redpandadata/redpanda` | 9092, 8081 (console) | Step 5 |
| dremio | `dremio/dremio-oss` | 9047 | Step 7 |
| metabase | `metabase/metabase` | 3000 | Step 8 |
| airflow | `apache/airflow` | 8082 (optional) | Step 3+ |

---

## Environment Variables

Create `.env` from `.env.example`:

```env
# API Keys
TMDB_API_KEY=your_tmdb_api_key_here
OMDB_API_KEY=your_omdb_api_key_here
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret

# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123
MINIO_BUCKET=warehouse

# Nessie
NESSIE_URI=http://nessie:19120/api/v2

# Spark
SPARK_MASTER=local[*]

# Trino
TRINO_HOST=trino
TRINO_PORT=8090
```

---

## Coding Agent Instructions

This section is intended for AI coding assistants (Claude, Copilot, Cursor, etc.) helping to build the Cineberg project.

### General Principles

- **Build incrementally.** Follow the step order. Do not add Dremio at step 1 or dbt at step 3. Each step's `docker-compose.yml` should only include the services introduced so far.
- **Prefer simplicity.** Use the minimal viable configuration for each service. Avoid over-engineering Docker networking, volumes, or health checks until needed.
- **Test at each step.** Every step has explicit success criteria. Verify them before moving on.
- **Use Python for ingestion, SQL for transformation.** Ingestion scripts are Python. Data modeling is dbt SQL. Do not mix these concerns.
- **Iceberg-native operations.** Always prefer Iceberg's built-in features (hidden partitioning, schema evolution, MERGE INTO, time travel) over workarounds. The goal is to learn Iceberg, not to work around it.

### Technical Constraints

- All services run in Docker Compose on a single machine. No cloud dependencies.
- Network access is required only for API calls to TMDB, OMDb, and Reddit.
- Target local development on a machine with at least 16 GB RAM (Spark and Dremio are the heaviest services).
- Use Python 3.11+ for all Python code.
- Use PySpark (not Scala) for Spark jobs.
- Use dbt-trino (not dbt-spark) for transformations — Trino is the primary query engine.

### Configuration Specifics

**Spark + Iceberg + Nessie:**
- Use `org.apache.iceberg:iceberg-spark-runtime` matching the Spark version.
- Use `org.projectnessie.nessie-integrations:nessie-spark-extensions` for Nessie support.
- Catalog config in SparkSession:
  ```python
  .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
  .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2")
  .config("spark.sql.catalog.nessie.ref", "main")
  .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/")
  .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
  ```

**Trino + Iceberg + Nessie:**
- Catalog properties file `iceberg.properties`:
  ```properties
  connector.name=iceberg
  iceberg.catalog.type=nessie
  iceberg.nessie-catalog.uri=http://nessie:19120/api/v2
  iceberg.nessie-catalog.default-warehouse-dir=s3a://warehouse/
  ```

**dbt + Trino:**
- Use `dbt-trino` adapter.
- `profiles.yml` should target the `iceberg` catalog in Trino.
- All models should be materialized as `table` (Iceberg) or `incremental` where specified.

### Ingestion Script Patterns

All ingestion scripts should follow this pattern:

```python
"""
Cineberg — ingestion/tmdb/bulk_load.py
Step: 1
Purpose: Bulk load movies from TMDB discover API into raw.tmdb_movies Iceberg table.
Dependencies: TMDB_API_KEY env var, running Spark/Nessie/MinIO.
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# 1. Initialize Spark with Iceberg + Nessie config
# 2. Call TMDB API with pagination
# 3. Convert to Spark DataFrame
# 4. Write to Iceberg table using .writeTo("nessie.raw.tmdb_movies")
# 5. Log ingestion metadata
```

### dbt Model Patterns

Staging models should:
- Read from `raw.*` tables.
- Clean types, handle nulls, deduplicate.
- Be materialized as `table` (full refresh is fine for staging).

Mart models should:
- Read from `staging.*` models.
- Implement star schema joins and business logic.
- Include thorough column descriptions in YAML (these feed the text-to-SQL agent).
- Be materialized as `table` or `incremental` depending on size.

### When Generating Code

- Always include docstrings explaining the purpose and step reference.
- Include error handling for API calls (rate limits, timeouts, missing fields).
- Use environment variables for all secrets and configuration.
- Write idempotent scripts — running them twice should not corrupt data.
- Prefer explicit schemas over inferred schemas when writing Iceberg tables.
- When generating `docker-compose.yml`, include comments noting which step introduced each service.

---

## Suggested Learning Pace

| Phase | Steps | Estimated Time | Focus |
|---|---|---|---|
| Foundation | 1–2 | 1 weekend | Iceberg basics, star schema, Spark + Trino |
| Ingestion | 3–4 | 1 week (evenings) | Incremental loads, multi-source, schema evolution |
| Streaming & Ops | 5–6 | 1 weekend | Kafka/Redpanda, streaming writes, maintenance |
| Multi-engine & BI | 7–8 | 1 weekend | Dremio, dashboards, Metabase |
| AI layer | 9–11 (pick any) | 1 week each | Embeddings, text-to-SQL, sentiment |
| Extensions | A–J (pick any) | Varies | Deep dives into specific areas |

Total estimated time to complete steps 1–8: **3–4 weeks of part-time work.** At that point, Cineberg is a fully functional lakehouse with real data, multiple engines, and a dashboard.

---

## References

- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
- [Nessie documentation](https://projectnessie.org/docs/)
- [TMDB API documentation](https://developer.themoviedb.org/docs)
- [OMDb API documentation](https://www.omdbapi.com/)
- [Reddit API documentation](https://www.reddit.com/dev/api/)
- [dbt-trino documentation](https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Dremio + Nessie guide](https://docs.dremio.com/current/sonar/data-sources/nessie/)
- [PyIceberg documentation](https://py.iceberg.apache.org/)
- [Redpanda quickstart](https://docs.redpanda.com/docs/get-started/quick-start/)
- [Metabase documentation](https://www.metabase.com/docs/latest/)
