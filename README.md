<a name="readme-top"></a>

# Data Engineering Exam for OTA

> A modern data engineering pipeline for COVID-19 analytics using medallion architecture, demonstrating ETL best practices with dbt, Airflow, and Metabase.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Data Models](#data-models)
- [Deployment](#deployment)
- [Output Examples](#output-examples)

## Overview

This project implements a complete data engineering solution for COVID-19 data analysis, featuring:

- **Medallion Architecture**: Silver (cleaned) and Gold (analytics-ready) layers
- **Dimensional Modeling**: Fact tables with pre-calculated metrics and derived measures
- **Modern Data Stack**: dbt for transformation, Airflow for orchestration, Metabase for visualization
- **Infrastructure as Code**: Containerized deployment with Docker Compose

## Architecture

We are going to be using ELT Architecture or Medallion Architecture. The architecture works on small, and even large scale or terabytes, of datasets.

![Data Architecture](/.images/output_architecture.png)

```
┌─────────────────┐
│   Data Source   │  COVID-19 Raw Data (US & Global)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  Data Cleaning & Standardization
│  (dbt models)   │  - silver_covid_cases_us
└────────┬────────┘  - silver_covid_cases_global
         │
         ▼
┌─────────────────┐
│   Gold Layer    │  Business-Ready Analytics
│  (dbt models)   │  ├── fct_covid_daily (fact table)
│                 │  ├── agg_* (aggregations)
│                 │  ├── mart_* (data marts)
│                 │  └── rpt_* (reports)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Metabase      │  Interactive Dashboards & Analytics
└─────────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Warehouse** | PostgreSQL 15 | Central data storage |
| **Transformation** | dbt | Data modeling & transformation |
| **Orchestration** | Apache Airflow | Workflow automation |
| **Visualization** | Metabase | Business intelligence & dashboards |
| **Container Runtime** | Docker Compose | Service orchestration |
| **Task Automation** | Task (taskfile.yml) | Deployment automation |

## Prerequisites

- **[Docker](https://docs.docker.com/get-docker/)** - Container runtime for running all services (PostgreSQL, Metabase, Airflow) in isolated environments
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** - Fast Python package manager for managing dependencies and virtual environments.
- **[Task](https://taskfile.dev/installation/)** - Task runner for automating deployment commands and workflows
- **Git** - Version control for cloning the repository
- **8GB+ RAM** - Recommended for running multiple Docker containers simultaneously

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd de-exam-ota
   ```

2. **Install Python dependencies**
   ```bash
   # Sync dbt and its dependencies using uv
   uv sync --group dbt
   ```

3. **Deploy Airflow**
   ```bash
   task deploy_airflow
   ```

4. **Deploy PostgreSQL data warehouse**
   ```bash
   task deploy_postgres
   ```

5. **Run dbt transformations**
   ```bash
   task deploy_dbt
   ```

6. **Launch Metabase for visualization**
   ```bash
   task deploy_metabase
   ```

7. **Access Metabase**
   - Open browser: `http://localhost:30001` for Metabase
   - Open browser: `http://localhost:8080` for Airflow
   - Connect to PostgreSQL warehouse on port `50002`

## Current Setup (Personal Setup)

Now, the containers looks like.

![Docker Desktop Containers](/.images/output_containers.png)

Some of my containers are deployed inside a Portainer and server using TrueNAS.

![Docker Containers](/.images/output_server_containers_v1.png)

## Connection Details

In Airflow, to create connection amongst different resources, DBT Container and Postgres, you need to add the following:

![Airflow Connections](/.images/output_connections_v1.png)

## Project Structure

```
de-exam-ota/
├── dbt/
│   ├── models/
│   │   ├── 01-silver/          # Cleaned, standardized data
│   │   └── 02-gold/            # Analytics-ready models
│   │       ├── fct_covid_daily.sql        # Core fact table
│   │       ├── agg_*.sql                  # Pre-computed aggregations
│   │       ├── mart_*.sql                 # Production data marts
│   │       └── rpt_*.sql                  # Analytical reports
│   ├── macros/                 # Reusable SQL functions
│   └── tests/                  # Data quality tests
├── dags/                       # Airflow DAG definitions
├── compose.*.yml               # Docker Compose configurations
├── taskfile.yml                # Task automation definitions
└── README.md
```

## Usage

### Running the DAGs, Tasks or Pipelines

Inside Airflow, you have the following data pipeline. One is for ingesting the data, another is for transformating the data.

![Airflow Dags](/.images/output_dags_v1.png)

### Available Tasks

View all available tasks:
```bash
task --list
```

#### Core Deployment Commands

```bash
# Deploy individual services
task deploy_postgres      # PostgreSQL data warehouse
task deploy_dbt          # dbt transformations
task deploy_metabase     # Metabase BI platform
task deploy_airflow      # Airflow orchestration

# Reload Python dependencies (Airflow)
task reload_airflow_reqs
```

### Working with dbt

Run dbt commands locally after syncing dependencies:

```bash
# First time setup: Sync dbt and its dependencies
uv sync --group dbt

# Run all models
uv run dbt run

# Run specific layers
uv run dbt run --select 01-silver
uv run dbt run --select 02-gold

# Run specific model tags
uv run dbt run --select tag:fact      # Fact tables only
uv run dbt run --select tag:mart      # Data marts only

# Test data quality
uv run dbt test

# Generate documentation
uv run dbt docs generate
uv run dbt docs serve
```

**Note:** You can also run dbt inside Docker containers using `task deploy_dbt` which handles dependencies automatically.

### Service Endpoints

| Service | URL | Port | Credentials |
|---------|-----|------|-------------|
| Metabase | http://localhost:30001 | 30001 | Setup on first login |
| Metabase DB | localhost | 50001 | `metabase/metabase` |
| Warehouse | localhost | 50002 | `warehouse/warehouse` |
| Airflow (if deployed) | http://localhost:8080 | 8080 | See compose file |

## Data Models

### Gold Layer Architecture

The gold layer follows dimensional modeling best practices:

#### **Fact Table**
- `fct_covid_daily`: Central fact table with daily COVID-19 metrics
  - **Grain**: One row per location per day
  - Pre-calculated metrics (rolling averages, day-over-day changes)
  - Derived measures (CFR, test positivity, severity classifications)

#### **Data Marts** (Production)
- `mart_top_countries`: Top countries by cases with rankings
- `mart_covid_trends`: Time series trends with indicators
- `mart_correlation_metrics`: Statistical correlation analysis

#### **Aggregations** (Performance-Optimized)
- `agg_daily_summary`: Daily national-level metrics
- `agg_state_summary`: State-level comprehensive summaries
- `agg_country_summary`: Country-level global analysis

#### **Reports** (Analytical Exploration)
- `rpt_top_countries_by_cases`: Frequency analysis
- `rpt_covid_trends_over_time`: Temporal analysis
- `rpt_correlation_analysis`: Statistical relationships

**Naming Conventions:**
- `fct_*` = Fact tables (granular data)
- `mart_*` = Data marts (production-ready)
- `agg_*` = Aggregations (pre-computed summaries)
- `rpt_*` = Reports (ad-hoc analysis)

See [`dbt/models/02-gold/README.md`](dbt/models/02-gold/README.md) for detailed documentation.

## Deployment

### Production Deployment

The project uses Docker Compose with separate configurations for each service:

```bash
# Full stack deployment
task deploy_postgres    # 1. Deploy warehouse first
task deploy_dbt        # 2. Run transformations
task deploy_metabase   # 3. Launch BI platform
task deploy_airflow    # 4. Optional: orchestration
```

### Environment Variables

Services use default credentials for development. For production, create `.env` file:

```env
# Warehouse PostgreSQL
WAREHOUSE_POSTGRES_USER=warehouse
WAREHOUSE_POSTGRES_PASSWORD=<secure-password>
WAREHOUSE_POSTGRES_DB=warehouse

# Metabase PostgreSQL
MB_POSTGRES_USER=metabase
MB_POSTGRES_PASSWORD=<secure-password>
MB_POSTGRES_DB=metabase
```

For this example or repository, the environment variables is placed under the .env.example.

## Output Examples

### Data Processes

![Ingestion Process](/.images/output_ingestion_v1.png)

![Transformation Process](/.images/output_transformation_v1.png)

### Metabase Dashboard

Production-ready analytics dashboard connected to the PostgreSQL warehouse (gold layer tables only):

![Metabase Dashboard](/.images/output_v1.png)

Sample schemas and tables inside the Postgres.

![Bronze Data](/.images/output_bronze_layer_v1.png)

![Schema](/.images/output_schema_development_v1.png)

**Key Features:**
- Top countries by confirmed cases
- Time series trends with moving averages
- Correlation analysis between key metrics
- State-level and country-level summaries

---

<p align="right">(<a href="#readme-top">back to top</a>)</p>