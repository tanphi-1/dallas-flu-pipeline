# Dallas County Flu Surveillance Pipeline

A batch data pipeline that ingests Dallas County (DCHHS) and Texas state (DSHS) influenza surveillance data from weekly PDF reports, loads it into a cloud data warehouse, transforms it with dbt, and visualizes it in Power BI.

**The problem:** Dallas County Health and Human Services (DCHHS) publishes weekly flu surveillance data only as individual PDF files — no API, no CSV, no aggregated dashboard. This pipeline solves that by extracting, centralizing, and transforming the data for analysis.

Built as a capstone project for the [Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## Architecture

```
┌──────────────────┐     ┌──────────────────┐
│  DCHHS Website   │     │  DSHS Website    │
│  (PDF reports)   │     │  (PDF reports)   │
└────────┬─────────┘     └────────┬─────────┘
         │  curl scrape           │  curl scrape
         ▼                        ▼
┌──────────────────────────────────────────┐
│         Supabase Storage (Data Lake)     │
│   flu-pdfs-dchhs    │   flu-pdfs-dshs    │
└────────┬────────────┴───────────┬────────┘
         │  pdfplumber parse              │
         ▼                        ▼
┌──────────────────────────────────────────┐
│       Supabase PostgreSQL (Warehouse)    │
│  staging.stg_dchhs_weekly (59 rows)      │
│  staging.stg_dshs_weekly  (90 rows)      │
└────────────────────┬─────────────────────┘
                     │  dbt transform
                     ▼
┌──────────────────────────────────────────┐
│              Marts Layer (dbt)           │
│  mart_flu_weekly           (59 rows)     │
│  mart_flu_seasonal_summary (2 rows)      │
└────────────────────┬─────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────┐
│           Power BI Dashboard             │
│  4 charts + season slicer               │
└──────────────────────────────────────────┘

Orchestration: Kestra (3-flow DAG, weekly Friday schedule)
IaC: Terraform (Supabase buckets, staging tables, Kestra container)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud compute | GitHub Codespaces |
| IaC | Terraform (supabase + docker providers) |
| Data lake | Supabase Storage (2 buckets) |
| Orchestration | Kestra (Docker, port 8080) |
| Data warehouse | Supabase PostgreSQL |
| Transformations | dbt Core + dbt-postgres (5 models, 9 tests) |
| Dashboard | Power BI Desktop |
| PDF parsing | pdfplumber |

## Data Sources

### DCHHS — Dallas County (primary)
- Weekly flu surveillance PDFs from [dallascounty.org](https://www.dallascounty.org/departments/dchhs/data-reports/influenza-surveillance.php)
- Fields extracted: total tests, positive tests, % positive, flu A/B counts, hospitalizations, ICU admissions, pediatric deaths, school absenteeism/ILI rates

### DSHS — Texas State (supplemental)
- Weekly respiratory virus surveillance PDFs from [dshs.texas.gov](https://www.dshs.texas.gov/texas-respiratory-virus-surveillance-report)
- Fields extracted: ILI %, ILI baseline %, providers reporting, age group breakdown, flu A/B/H1N1/H3N2 counts

### Seasons covered
- 2023-2024: MMWR Week 40/2023 through Week 20/2024
- 2024-2025: MMWR Week 40/2024 through Week 20/2025
- ~120 PDFs total across both sources and seasons

## Data Warehouse

The warehouse uses Supabase PostgreSQL with two schemas:

- **`staging`** — raw extracted data (`stg_dchhs_weekly`, `stg_dshs_weekly`)
- **`marts`** — transformed tables for dashboards (`mart_flu_weekly`, `mart_flu_seasonal_summary`)

### Indexing strategy

Both staging tables have a composite index on `(flu_season, mmwr_week)`:

```sql
CREATE INDEX idx_dchhs_season_week ON staging.stg_dchhs_weekly (flu_season, mmwr_week);
CREATE INDEX idx_dshs_season_week  ON staging.stg_dshs_weekly  (flu_season, mmwr_week);
```

**Why this index:** Every downstream query — dbt models, Power BI charts, and the season slicer — filters by `flu_season` first, then orders or groups by `mmwr_week`. This composite index covers both the filter and sort in a single index scan. For example:

- **Chart 1** (% Positive by Week): filters `WHERE flu_season = '2023-2024'`, orders by week
- **Chart 3** (Weekly Hospitalizations): same filter + sort pattern
- **Season slicer**: filters all charts by `flu_season`
- **dbt window functions** (`ROW_NUMBER`, `LAG`, rolling avg): partition by `flu_season`, order by `mmwr_week`

Both staging tables also have a `UNIQUE` constraint on `report_week_end_date`, which supports the upsert logic (`ON CONFLICT DO UPDATE`) and the dbt JOIN between DCHHS and DSHS data.

## Dashboard

The Power BI dashboard connects to the `marts` schema and includes:

| Chart | Type | Description |
|---|---|---|
| Weekly % Positive | Line chart | Positivity rate by week with season comparison |
| Strain Distribution | 100% stacked bar | Flu A vs Flu B proportion by season |
| Weekly Hospitalizations | Clustered column | Hospitalization counts by week and season |
| Season Severity Scorecards | KPI cards | Peak %, weeks above baseline, dominant strain |

A season slicer allows interactive filtering between 2023-2024 and 2024-2025.

## dbt Models

```
Sources (Supabase staging tables)
├── stg_dchhs_weekly.sql     (staging view — cleans DCHHS data)
├── stg_dshs_weekly.sql      (staging view — cleans DSHS data)
├── int_flu_weekly_combined.sql (intermediate view — JOIN + window functions)
├── mart_flu_weekly.sql         (marts table — weekly detail for charts 1, 3)
└── mart_flu_seasonal_summary.sql (marts table — per-season summary for charts 2, 4)
```

Key transformations in the marts layer:
- `rolling_4wk_avg_pct` — 4-week moving average of % positive
- `wow_pct_change` — week-over-week change
- `is_peak_week` — flags the peak positivity week per season
- `severity_label` — categorizes season as Low/Moderate/High/Very High
- `dominant_strain` — Flu A or Flu B based on total counts

Tests: 7 schema tests (not_null, unique, accepted_values) + 2 custom SQL tests (pct_in_range, no_future_dates). All 9 pass.

## Reproducibility — Step-by-Step Setup

### Prerequisites
- GitHub account
- [Supabase](https://supabase.com) account (free tier works)

### 1. Create a Supabase project

1. Go to [supabase.com](https://supabase.com) and create a new project
2. Note your **Project URL**, **service_role key** (not anon key), **database password**, and **Project Ref ID**
3. Go to Settings > Database to find the **Session Pooler** connection string and host

### 2. Set up GitHub Codespaces secrets

Go to your fork's Settings > Secrets and variables > Codespaces, and add these secrets:

| Secret name | Value |
|---|---|
| `SUPABASE_URL` | `https://YOUR_REF.supabase.co` |
| `SUPABASE_KEY` | Your service_role key |
| `SUPABASE_DB_URL` | `postgresql://postgres.REF:PASSWORD@aws-0-REGION.pooler.supabase.com:5432/postgres` |
| `SUPABASE_DB_PASSWORD` | Your database password |
| `SUPABASE_DB_HOST` | `aws-0-REGION.pooler.supabase.com` (session pooler host) |
| `SUPABASE_PROJECT_REF` | Your project ref ID |
| `SUPABASE_ACCESS_TOKEN` | Your personal access token (`sbp_...`) |

### 3. Launch Codespace

Click **Code > Codespaces > Create codespace on main**. The devcontainer will:
- Set up Python 3.11, Docker-in-Docker, and Terraform
- Install all Python dependencies from `requirements.txt`
- Forward port 8080 for the Kestra UI

### 4. Create the `.env` file

```bash
cp .env.example .env
# Edit .env with your actual Supabase credentials
```

### 5. Create `terraform.tfvars`

```bash
cat > terraform/terraform.tfvars << 'EOF'
supabase_url         = "https://YOUR_REF.supabase.co"
supabase_key         = "your_service_role_key"
supabase_project_ref = "your_project_ref"
supabase_db_password = "your_db_password"
supabase_db_url      = "postgresql://postgres.REF:PASSWORD@pooler.supabase.com:5432/postgres"
supabase_db_host     = "aws-0-REGION.pooler.supabase.com"
EOF
```

### 6. Run Terraform

```bash
cd terraform
terraform init
terraform apply
```

This provisions:
- 2 Supabase Storage buckets (`flu-pdfs-dchhs`, `flu-pdfs-dshs`)
- 2 staging tables (`staging.stg_dchhs_weekly`, `staging.stg_dshs_weekly`)
- Kestra Docker container with secrets injected

### 7. Create dbt `profiles.yml`

```bash
cat > dbt/profiles.yml << 'EOF'
dallas_flu:
  target: prod
  outputs:
    prod:
      type: postgres
      host: '{{ env_var("SUPABASE_DB_HOST") }}'
      user: 'postgres.{{ env_var("SUPABASE_PROJECT_REF") }}'
      password: '{{ env_var("SUPABASE_DB_PASSWORD") }}'
      port: 5432
      dbname: postgres
      schema: marts
      threads: 4
EOF
```

### 8. Deploy Kestra flows

```bash
# Wait for Kestra to start (check http://localhost:8080)
curl -X POST http://localhost:8080/api/v1/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary @kestra/flows/01_scrape_all_pdfs.yml

curl -X POST http://localhost:8080/api/v1/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary @kestra/flows/02_parse_and_load.yml

curl -X POST http://localhost:8080/api/v1/flows \
  -H "Content-Type: application/x-yaml" \
  --data-binary @kestra/flows/03_run_dbt.yml
```

### 9. Run the pipeline

Trigger the first flow manually (subsequent flows chain automatically):

```bash
curl -X POST http://localhost:8080/api/v1/executions/dallas.flu/scrape_all_pdfs
```

This kicks off the full pipeline:
1. **Flow 01** — Scrapes ~120 PDFs from DCHHS and DSHS, uploads to Supabase Storage
2. **Flow 02** — Parses PDFs with pdfplumber, extracts metrics, upserts 59 + 90 rows to staging
3. **Flow 03** — Runs `dbt run` (5 models) + `dbt test` (9 tests) to build mart tables

Monitor progress in the Kestra UI at http://localhost:8080.

### 10. Verify results

```bash
# Check dbt models
cd dbt
source ../.env
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .

# View dbt docs (optional)
dbt docs generate --profiles-dir . --project-dir .
dbt docs serve --profiles-dir . --project-dir . --port 8081 --no-browser
```

### 11. Connect Power BI

1. Open Power BI Desktop
2. Get Data > PostgreSQL
3. Server: `aws-0-REGION.pooler.supabase.com` | Database: `postgres`
4. Under Advanced: set **Trust Server Certificate = true**
5. Authenticate with database credentials
6. Import tables from the `marts_marts` schema:
   - `mart_flu_weekly`
   - `mart_flu_seasonal_summary`

## Project Structure

```
dallas-flu-pipeline/
├── .devcontainer/devcontainer.json   ← Codespaces config
├── terraform/
│   ├── main.tf                       ← IaC: buckets, tables, Kestra
│   ├── variables.tf
│   └── outputs.tf
├── kestra/flows/
│   ├── 01_scrape_all_pdfs.yml        ← Scrape PDFs from both sources
│   ├── 02_parse_and_load.yml         ← Parse PDFs, load to staging
│   └── 03_run_dbt.yml                ← dbt run + dbt test
├── ingestion/
│   ├── dchhs/
│   │   ├── scrape_dchhs.py
│   │   └── parse_dchhs.py
│   ├── dshs/
│   │   ├── scrape_dshs.py
│   │   └── parse_dshs.py
│   └── load_staging.py
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                  ← 2 views + schema tests
│   │   ├── intermediate/             ← 1 combined view
│   │   └── marts/                    ← 2 tables (Power BI source)
│   └── tests/                        ← 2 custom SQL tests
├── .env.example                      ← Env var template
├── requirements.txt
└── README.md
```

## Known Issues and Workarounds

| Issue | Workaround |
|---|---|
| DCHHS/DSHS SSL failures with Python requests | Use `curl` subprocess instead |
| DSHS URL patterns inconsistent across years | Crawl index page, regex multiple patterns |
| DCHHS table column counts vary by report period | Content-based table finding, dynamic extraction |
| Supabase free tier has no IPv4 direct connect | Use session pooler connection string |
| Kestra open-source has no secrets API | Pass as `SECRET_*` env vars on container |
| numpy int64 causes psycopg2 errors | `to_python()` converter before insert |
| Kestra PROCESS runner needs `SECRET_*` env vars | Scripts read `SECRET_*` directly from environment |
