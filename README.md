# Medallion Architecture Pipeline

A local end-to-end ELT pipeline demonstrating Medallion Architecture
(Bronze → Silver → Gold) using two public data sources, two ingestion
patterns, and ClickHouse as the central analytical store.

## What this project does

| Layer   | Real-time path                        | Batch path                          |
|---------|---------------------------------------|-------------------------------------|
| Source  | Wikimedia SSE stream (live edits)     | Open-Meteo Weather API (daily)      |
| Ingest  | Apache NiFi                           | Apache Airflow                      |
| Bronze  | `raw.wiki_edits`                      | `raw.weather_daily`                 |
| Silver  | `clean.wiki_edits` (Materialized View)| `clean.weather_daily` (dbt)         |
| Gold    | `mart.wiki_edit_stats` (dbt)          | `mart.weather_summary` (dbt)        |
| Consume | Grafana / SQL client                  | Grafana / SQL client                |

## Stack

- **Apache NiFi 1.25** — real-time SSE ingestion
- **Apache Airflow 2.9.1** — daily batch orchestration
- **dbt-clickhouse 1.7** — Silver and Gold transformations
- **ClickHouse 24.3** — columnar OLAP store (all three layers)
- **Docker Compose** — single-command local deployment

## Data sources

### Wikimedia Event Stream (real-time)
- URL: `https://stream.wikimedia.org/v2/stream/recentchange`
- Protocol: Server-Sent Events (SSE) — continuous HTTP stream
- No API key or account required
- Emits one JSON event per Wikipedia/Wikimedia edit globally
- Typical volume: 50–200 events per second across all wikis

### Open-Meteo Weather API (batch)
- URL: `https://api.open-meteo.com/v1/forecast`
- Protocol: REST — standard HTTP GET, returns JSON
- No API key, no account, completely free for non-commercial use
- Rate limit: 10,000 calls/day (your DAG uses ~100 calls/run)
- Provides hourly and daily weather for any lat/lon coordinate
- Historical data available back to 1940

## Quick start

### Prerequisites
- Docker Desktop 4.x+
- Python 3.10+ (for dbt CLI)
- 8 GB RAM available for Docker

### Start all services

git clone https://github.com/Empizoil/medallion-project.git
cd medallion-project
cp .env.example .env
docker compose up -d

### Verify services are running

docker compose ps

| Service              | Port  | URL                          |
|----------------------|-------|------------------------------|
| ClickHouse HTTP      | 8123  | http://localhost:8123/ping   |
| ClickHouse Native    | 9000  | —                            |
| NiFi UI              | 8443  | http://localhost:8443/nifi   |
| Airflow UI           | 8080  | http://localhost:8080        |

### Check data is flowing

# ClickHouse CLI
docker exec -it clickhouse clickhouse-client --password clickpass

-- Streaming data (should grow over time)
SELECT count() FROM raw.wiki_edits;

-- Batch data (populated after first DAG run)
SELECT count() FROM raw.weather_daily;

## Project structure

medallion-project/
├── README.md
├── docker-compose.yml
├── .env.example
├── .env                        ← created by you, not committed
├── .gitignore
├── nifi/
│   └── templates/
│       └── wiki_stream.xml    ← NiFi flow template
├── airflow/
│   └── dags/
│       └── weather_batch_dag.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/
│       │   ├── stg_wiki_edits.sql
│       │   └── stg_weather_daily.sql
│       ├── intermediate/
│       │   └── int_daily_weather.sql
│       └── marts/
│           ├── mart_wiki_edit_stats.sql
│           ├── mart_weather_summary.sql
│           └── schema.yml
└── clickhouse/
    └── init/
        ├── 01_create_databases.sql
        ├── 02_bronze_tables.sql
        └── 03_materialized_views.sql

## Key design decisions

### Why ClickHouse Materialized Views for streaming Silver?
The Wikimedia stream is continuous — NiFi inserts rows every few
seconds. Running dbt on a schedule to clean streaming data would
introduce latency and complexity. Instead, a ClickHouse Materialized
View fires on every INSERT into `raw.wiki_edits` and populates
`clean.wiki_edits` instantly. No scheduler needed. No lag.

### Why dbt for batch Silver and Gold?
The Open-Meteo data arrives once a day in a predictable shape. dbt's
incremental models, dependency graph, and built-in test framework are
exactly the right tool for scheduled, structured transformation. The
Airflow DAG runs `dbt run` and `dbt test` after each ingestion.

### Why ReplacingMergeTree for Silver?
Wikipedia edits can be replayed by NiFi if a connection drops and
reconnects. `ReplacingMergeTree` deduplicates rows sharing the same
`ORDER BY` key in the background, keeping only the most recent version.
This protects against double-counting without adding application logic.

### Why Open-Meteo over Kaggle?
Open-Meteo gives a genuinely live batch pipeline — new weather data
arrives every day. A Kaggle CSV is a static snapshot. With Open-Meteo,
the dbt incremental model has a real reason to exist because new rows
actually arrive each run. This makes the portfolio project significantly
more impressive.

## License

MIT — free to use, fork, and build on.
```

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│                                                                      │
│   Wikimedia SSE Stream              Open-Meteo Weather API           │
│   stream.wikimedia.org              api.open-meteo.com               │
│   (continuous, no auth)             (daily REST, no auth)            │
└────────────┬────────────────────────────────┬────────────────────────┘
             │ SSE events                      │ JSON response
             ▼                                 ▼
┌────────────────────────┐        ┌────────────────────────────────────┐
│     Apache NiFi         │        │           Apache Airflow           │
│                         │        │                                    │
│  InvokeHTTP             │        │  PythonOperator                    │
│  → SplitText            │        │  → fetches 100 cities/day          │
│  → EvaluateJsonPath     │        │  → cleans with pandas              │
│  → PutDatabaseRecord    │        │  → bulk inserts to ClickHouse      │
│                         │        │  → BashOperator: dbt run           │
│  (always running)       │        │  → BashOperator: dbt test          │
│                         │        │  (schedule: 0 3 * * *)             │
└────────────┬────────────┘        └────────────────┬───────────────────┘
             │ INSERT                               │ INSERT
             ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ClickHouse                                    │
│              Columnar OLAP · MergeTree engine                        │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  BRONZE — raw, immutable, exact copy of source               │   │
│  │                                                              │   │
│  │  raw.wiki_edits (MergeTree)    raw.weather_daily (MergeTree) │   │
│  │  · rev_id, wiki, title         · city, lat, lon, date        │   │
│  │  · user, is_bot, byte_delta    · temp, humidity, precip      │   │
│  │  · event_time, raw_payload     · wind, condition             │   │
│  └──────────────┬───────────────────────────────────┬───────────┘   │
│                 │ Materialized View                  │ dbt run       │
│                 │ (fires on every INSERT)            │ (daily)       │
│                 ▼                                    ▼               │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  SILVER — cleaned, typed, validated, deduplicated            │   │
│  │                                                              │   │
│  │  clean.wiki_edits                clean.weather_daily         │   │
│  │  (ReplacingMergeTree)            (dbt table model)           │   │
│  │  · bots filtered out             · nulls dropped             │   │
│  │  · UInt8 → Bool cast             · units normalised          │   │
│  │  · empty titles removed          · sanity bounds applied     │   │
│  └──────────────┬───────────────────────────────────┬───────────┘   │
│                 │ dbt run (daily)                    │ dbt run       │
│                 ▼                                    ▼               │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  GOLD — business-ready aggregates, BI-ready                  │   │
│  │                                                              │   │
│  │  mart.wiki_edit_stats            mart.weather_summary        │   │
│  │  · edits/hr by wiki              · monthly avg temp          │   │
│  │  · human vs bot ratio            · rolling rain totals       │   │
│  │  · top edited articles           · anomaly vs baseline       │   │
│  └──────────────┬───────────────────────────────────┬───────────┘   │
└─────────────────┼───────────────────────────────────┼───────────────┘
                  │                                   │
                  ▼                                   ▼
         Grafana / Superset / SQL client
```

### Data flow timing

| Event | Latency |
|---|---|
| Wikipedia edit occurs → lands in `raw.wiki_edits` | ~2–5 seconds |
| `raw.wiki_edits` insert → `clean.wiki_edits` (MV fires) | Milliseconds |
| Open-Meteo fetch → `raw.weather_daily` | Daily at 03:00 UTC |
| `raw.weather_daily` → dbt Silver + Gold | ~3–5 minutes after ingest |

---

## 3. Folder Structure

Create this exact structure before writing any code:

```
medallion-project/
├── README.md                          ← copy from Section 1
├── docker-compose.yml
├── .env                               ← secrets, never commit
├── .env.example                       ← template, safe to commit
├── .gitignore
│
├── nifi/
│   └── templates/
│       └── wiki_stream.xml            ← exported from NiFi UI
│
├── airflow/
│   ├── dags/
│   │   └── weather_batch_dag.py
│   └── plugins/                       ← empty for now
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                   ← ClickHouse connection
│   └── models/
│       ├── sources.yml                ← declares raw.* sources
│       ├── staging/
│       │   ├── stg_wiki_edits.sql
│       │   └── stg_weather_daily.sql
│       ├── intermediate/
│       │   └── int_daily_weather.sql
│       └── marts/
│           ├── mart_wiki_edit_stats.sql
│           ├── mart_weather_summary.sql
│           └── schema.yml
│
└── clickhouse/
    └── init/
        ├── 01_create_databases.sql
        ├── 02_bronze_tables.sql
        └── 03_materialized_views.sql