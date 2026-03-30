# Real-Time Stock Market Data Engineering Pipeline

This project streams near-real-time stock market data for `AAPL`, `GOOGL`, `MSFT`, `AMZN`, and `TSLA`, stores raw events in PostgreSQL, transforms them with dbt, orchestrates scheduled runs with Kestra, and visualizes the curated models in Grafana. Everything runs locally with Docker Compose and has no cloud infrastructure dependency.

## Architecture

```text
yfinance API
     |
     v
Kafka Producer (Python)
     |
     v
Apache Kafka
     |
     v
Kafka Consumer (Python)
     |
     v
PostgreSQL (raw_stock_prices)
     |
     v
Kestra (orchestrates)
     |
     v
dbt run -> dbt test
     |
     v
PostgreSQL (mart models)
     |
     v
Grafana Dashboard
```

## Why These Tools

Each tool in this project was chosen to solve a specific part of the pipeline:

- `yfinance`: used as the market data source because it is simple for pulling stock prices in a local project without needing a paid market data provider.
- `Python`: used for the producer and consumer because it is quick to develop with, has strong Kafka and PostgreSQL libraries, and fits well for lightweight data services.
- `Kafka`: used as the streaming layer to decouple data collection from data storage and to simulate a real event-driven pipeline.
- `ZooKeeper`: used because this project runs Kafka in the classic ZooKeeper-based deployment model requested for the pipeline.
- `PostgreSQL`: used as the warehouse to store both raw ingested events and transformed analytics models in one reliable relational database.
- `dbt`: used to transform raw stock records into clean staging and mart models so the business logic stays modular, testable, and SQL-first.
- `Kestra`: used to orchestrate ingestion checks, `dbt run`, and `dbt test` on schedules so the pipeline works automatically end to end.
- `Grafana`: used to turn the transformed PostgreSQL models into a live dashboard for monitoring trends, gainers and losers, moving averages, and volume spikes.
- `Docker Compose`: used to run the whole stack locally with one command so every service starts in a consistent environment without manual setup.

## Prerequisites

- Docker Desktop or Docker Engine
- Docker Compose
- Internet access for pulling Docker images and Yahoo Finance data
- The following local ports available: `3000`, `5050`, `5432`, and `8080`

## Step-by-Step Startup Guide

Follow these steps from a clean terminal session.

### 1. Open the project folder

If you already have the project locally, open a terminal in the `stock_pipeline` folder.

```powershell
cd path\to\stock_pipeline
```

If you are cloning it for the first time:

```powershell
git clone <your-repo-url>
cd stock_pipeline
```

### 2. Create the environment file

Create a local `.env` file from the example file.

PowerShell:

```powershell
Copy-Item .env.example .env
```

Bash:

```bash
cp .env.example .env
```

### 3. Review the default credentials

The stack will start with these default credentials unless you change them in `.env`:

- PostgreSQL: `admin / admin`
- pgAdmin: `admin@stockpipeline.com / admin`
- Grafana: `admin / admin`
- Kestra: `admin@kestra.io / Admin1234`

### 4. Start the full project

Build and start every service in the background:

```powershell
docker compose up -d --build
```

If your machine uses the legacy Compose binary, you can use:

```powershell
docker-compose up -d --build
```

### 5. Wait for the containers to become healthy

Check the running services:

```powershell
docker compose ps
```

You should see these services:

- `zookeeper`
- `kafka`
- `postgres`
- `pgadmin`
- `producer`
- `consumer`
- `dbt`
- `kestra`
- `grafana`

Expected healthy services:

- `postgres`
- `kafka`
- `consumer`

The first startup can take a little longer because Docker may need to download images and initialize PostgreSQL.

### 6. Confirm that ingestion is running

Watch the producer and consumer logs:

```powershell
docker compose logs -f producer consumer
```

You should start seeing log lines showing:

- the producer publishing stock snapshots to Kafka
- the consumer storing records in PostgreSQL

Press `Ctrl+C` to stop following the logs without stopping the containers.

### 7. Open the project services in your browser

Use these URLs after the stack is up:

- pgAdmin: http://localhost:5050
- Grafana: http://localhost:3000
- Kestra: http://localhost:8080

PostgreSQL is not a browser-based service. Connect to it with pgAdmin, `psql`, or another SQL client on `localhost:5432`.

## Service URLs

- pgAdmin: http://localhost:5050
  - Username: `admin@stockpipeline.com`
  - Password: `admin`
- Grafana: http://localhost:3000
  - Username: `admin`
  - Password: `admin`
- Kestra: http://localhost:8080
  - Username: `admin@kestra.io`
  - Password: `Admin1234`
- PostgreSQL: `localhost:5432`

### 8. Connect to PostgreSQL from pgAdmin

1. Open http://localhost:5050.
2. Sign in with the pgAdmin credentials.
3. Click `Add New Server`.
4. In `General`, enter a name such as `Stock Pipeline Postgres`.
5. In `Connection`, use:
   - Host: `postgres`
   - Port: `5432`
   - Maintenance database: `stocks`
   - Username: `admin`
   - Password: `admin`
6. Click `Save`.

If you connect from a desktop database client on your host machine instead of pgAdmin in Docker, use `localhost` as the host.

### 9. Verify that data exists in PostgreSQL

Run this command to confirm raw stock events are being written:

```powershell
docker compose exec postgres psql -U admin -d stocks -c "select count(*) from raw_stock_prices;"
```

To list the project schemas and main relations:

```powershell
docker compose exec postgres psql -U admin -d stocks -c "\dt public.*"
docker compose exec postgres psql -U admin -d stocks -c "\dt mart.*"
docker compose exec postgres psql -U admin -d stocks -c "\dv staging.*"
```

### 10. Let Kestra and dbt build the analytics layer

After startup:

- the producer keeps sending stock data every 10 seconds
- the consumer keeps writing raw events into PostgreSQL
- `02_dbt_run` runs every 5 minutes
- `03_dbt_test` runs after successful `02_dbt_run`
- `04_full_pipeline` runs every 15 minutes

If you do not want to wait for the schedule, open Kestra and manually run `04_full_pipeline`.

### 11. Run dbt manually if needed

The dbt container stays idle so Kestra can call it, but you can still run dbt yourself:

```powershell
docker compose exec dbt dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project
docker compose exec dbt dbt test --project-dir /app/dbt_project --profiles-dir /app/dbt_project
```

### 12. Stop, restart, or rebuild the project

Stop the stack without deleting data:

```powershell
docker compose down
```

Restart the full stack:

```powershell
docker compose down
docker compose up -d
```

Rebuild and restart everything:

```powershell
docker compose up -d --build
```

Stop the stack and delete all Docker volumes:

```powershell
docker compose down -v
```

## What Starts Automatically

- pgAdmin starts with the stack so you can inspect PostgreSQL in the browser.
- The Python producer fetches market data every 10 seconds and publishes it to Kafka.
- The Python consumer subscribes to Kafka, writes idempotent records into PostgreSQL, manages monthly partitions, and exposes `GET /health`.
- Kestra starts in standalone mode, then bootstraps and syncs all flow YAML files from `/app/flows`.
- Grafana auto-provisions the PostgreSQL datasource and the stock dashboard.

## Kestra Flows

The following flows are visible in the Kestra UI under the `stock_pipeline` namespace:

- `01_ingest_check`: checks the consumer health endpoint every 2 minutes.
- `02_dbt_run`: runs `dbt run` every 5 minutes.
- `03_dbt_test`: runs `dbt test` after successful standalone `02_dbt_run` executions.
- `04_full_pipeline`: runs the end-to-end sequence every 15 minutes and can also be triggered manually.

### Manually Trigger a Flow

1. Open http://localhost:8080.
2. Navigate to the `stock_pipeline` namespace.
3. Open the flow you want to run.
4. Click `Execute`.

For an end-to-end manual run, execute `04_full_pipeline`.

## Environment Variables

The stack reads all credentials and runtime settings from `.env`.

- `POSTGRES_USER`: PostgreSQL username used by the app, dbt, Kestra, and Grafana.
- `POSTGRES_PASSWORD`: PostgreSQL password.
- `POSTGRES_DB`: PostgreSQL database name.
- `KAFKA_TOPIC`: Kafka topic used for stock price events.
- `STOCKS`: comma-separated list of ticker symbols to track.
- `KESTRA_POSTGRES_SCHEMA`: PostgreSQL schema reserved for Kestra metadata tables.
- `KESTRA_ADMIN_USERNAME`: Kestra UI and API username.
- `KESTRA_ADMIN_PASSWORD`: Kestra UI and API password.
- `GRAFANA_ADMIN_USER`: Grafana admin username.
- `GRAFANA_ADMIN_PASSWORD`: Grafana admin password.
- `PGADMIN_DEFAULT_EMAIL`: pgAdmin login email.
- `PGADMIN_DEFAULT_PASSWORD`: pgAdmin login password.

## Folder Structure

```text
stock_pipeline/
|-- .env
|-- .env.example
|-- README.md
|-- docker-compose.yml
|-- consumer/
|   |-- Dockerfile
|   |-- consumer.py
|   `-- requirements.txt
|-- dbt_project/
|   |-- Dockerfile
|   |-- dbt_project.yml
|   |-- macros/
|   |   |-- generate_schema_name.sql
|   |   `-- positive_value.sql
|   |-- models/
|   |   |-- marts/
|   |   |   |-- moving_average.sql
|   |   |   |-- schema.yml
|   |   |   |-- top_gainers_losers.sql
|   |   |   `-- volume_spikes.sql
|   |   `-- staging/
|   |       |-- schema.yml
|   |       `-- stg_stock_prices.sql
|   |-- profiles.yml
|   `-- requirements.txt
|-- grafana/
|   `-- provisioning/
|       |-- dashboards/
|       |   |-- provider.yaml
|       |   `-- stock_dashboard.json
|       `-- datasources/
|           `-- postgres.yaml
|-- kestra/
|   |-- entrypoint.sh
|   `-- flows/
|       |-- 01_ingest_check.yml
|       |-- 02_dbt_run.yml
|       |-- 03_dbt_test.yml
|       `-- 04_full_pipeline.yml
|-- postgres/
|   `-- init/
|       `-- 01_create_kestra_schema.sh
`-- producer/
    |-- Dockerfile
    |-- producer.py
    `-- requirements.txt
```

## Dashboard Panels

The prebuilt Grafana dashboard contains:

- Live Price Line Chart: last-hour price movement by symbol.
- Top Gainers & Losers Table: latest versus one-hour baseline ranking.
- Volume Spike Bar Chart: spike counts per symbol inside the dashboard time range.
- Moving Average vs Actual Price: actual price, 5-minute MA, and 15-minute MA for the selected symbol.

## Operational Notes

- The pipeline uses Confluent Kafka `7.6.1` to satisfy the requested Kafka-plus-ZooKeeper deployment model.
- The consumer table is monthly partitioned by `event_timestamp`.
- Duplicate events are skipped with `ON CONFLICT (symbol, event_timestamp) DO NOTHING`.
- Because PostgreSQL partitioned tables require the partition key to participate in the primary key, `raw_stock_prices` uses an identity `id` plus a composite primary key that includes `event_timestamp`.
