# WeatherETL

An ETL pipeline that extracts current weather data from the [OpenWeatherMap API](https://openweathermap.org/current), transforms it into a star-schema model, and loads it into a PostgreSQL data warehouse. Orchestrated with **Apache Airflow** and fully containerized with **Docker Compose**.

## Architecture

```
OpenWeatherMap API  ──►  Extract (JSON)  ──►  S3 (raw/)
                                                  │
                                              Transform
                                                  │
                                            S3 (processed/ parquet)
                                                  │
                                               Load
                                                  │
                                            PostgreSQL ★ Star Schema
```

| Component | Role |
|---|---|
| **Airflow** | Schedules the DAG hourly, handles retries with exponential backoff |
| **LocalStack** | Emulates AWS S3 locally for raw JSON and processed Parquet storage |
| **Terraform** | Provisions the S3 bucket on LocalStack at startup |
| **PostgreSQL** | Stores the dimensional model (`weather_data` schema) |

## Data Model

Star schema under `weather_data` schema:

- **fact_weather** — temperature, pressure, humidity, wind speed, cloud cover, rain/snow volumes
- **dim_location** — city id, name, latitude, longitude
- **dim_date** — date, day of week, month, quarter, year
- **dim_time** — time, hour, minute, second
- **dim_weather_condition** — condition id, main category, description
- **ingest_metadata** — fetch id, timestamp, source

## Pipeline Steps

1. **Extract** — Calls OpenWeatherMap Geocoding API to resolve city coordinates, then fetches current weather. Each response is enriched with metadata and uploaded to S3 as JSON (`raw/{YYYY/MM/DD/HH}/{run_id}.json`).
2. **Transform** — Reads raw JSON files from S3, flattens nested fields, converts timestamps, deduplicates, and writes a single Parquet file to S3 (`processed/`).
3. **Load** — Reads the Parquet file, splits it into dimension and fact DataFrames, upserts dimensions (ON CONFLICT DO NOTHING), resolves surrogate keys, and bulk-inserts facts via `execute_values`.

## Tech Stack

- Python 3.11, Pandas, PyArrow, psycopg2, boto3, requests
- Apache Airflow 2.9
- PostgreSQL 13
- LocalStack Pro (S3)
- Terraform
- Docker & Docker Compose

## Getting Started

### Prerequisites

- Docker & Docker Compose
- An [OpenWeatherMap API key](https://openweathermap.org/appid)
- A LocalStack Auth Token (for Pro features)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repo-url> && cd WeatherEtl
   ```

2. **Create a `.env` file** in the project root:
   ```env
   API_KEY=<your_openweathermap_api_key>
   LOCALSTACK_AUTH_TOKEN=<your_localstack_token>
   LOCALSTACK_ENDPOINT_URL=http://localstack:4566
   AWS_WEATHER_BUCKET_NAME=weather-data
   DB_NAME=weather
   DB_USER=airflow
   DB_PASSWORD=airflow
   DB_HOST=postgres
   DB_PORT=5432
   AIRFLOW_USER=admin
   AIRFLOW_PASSWORD=admin
   CITIES_TO_FETCH=Kuźnia Raciborska,Racibórz,Warszawa,Toruń
   ```

3. **Start the stack**
   ```bash
   docker compose up -d
   ```

4. **Access Airflow UI** at [http://localhost:8080](http://localhost:8080) and enable the `weather_dag`.

### Running Locally (without Airflow)

```bash
pip install -r requirements.txt
python -m src.main
```

## Project Structure

```
├── dags/
│   └── weather_dag.py        # Airflow DAG definition
├── src/
│   ├── main.py                # Standalone entry point
│   ├── extract/
│   │   └── weather_api.py     # API extraction logic
│   ├── transform/
│   │   └── transform_weather.py  # JSON → Parquet transformation
│   ├── load/
│   │   ├── load_weather.py    # Dimensional loading logic
│   │   └── sql_queries.py     # SQL statements
│   ├── aws/
│   │   └── aws_s3.py          # S3 upload / download helpers
│   └── utils/
│       ├── db_utils.py        # PostgreSQL connection manager
│       ├── files_utils.py     # Path constants
│       └── requests_utils.py  # HTTP request helper with retries
├── terraform/
│   └── main.tf                # S3 bucket provisioning
├── init.sql                   # DDL for the star schema
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

