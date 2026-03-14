
# CLAUDE.md — Dallas County Flu Surveillance Pipeline
# This file is read automatically by Claude Code at the start of every session.
# Do not delete it. Update it as the project evolves.

## Project Identity
- **Name:** Dallas County Seasonal Flu Surveillance Pipeline
- **Course:** DE Zoomcamp 2026 Capstone
- **Repo:** github.com/tanphi-1/dallas-flu-pipeline
- **Target score:** 28/28 base + 9 bonus (peer reviews)
- **Owner:** Tan

## What This Project Does
Ingests Dallas County (DCHHS) and Texas state (DSHS) influenza surveillance PDFs
for two flu seasons (2023-24 and 2024-25), loads them into a Supabase PostgreSQL
data warehouse, transforms them with dbt, and visualizes them in Power BI.

The core problem: DCHHS publishes weekly flu data ONLY as individual PDF files —
no API, no CSV, no aggregated dashboard. This pipeline fixes that.

## Tech Stack (confirmed, do not change without asking Tan)
| Layer | Tool |
|---|---|
| Cloud compute | GitHub Codespaces |
| IaC | Terraform (supabase/supabase provider + kreuzwerker/docker) |
| Data lake | Supabase Storage — buckets: flu-pdfs-dchhs, flu-pdfs-dshs |
| Orchestration | Kestra (Docker container, port 8080) — 3 flows |
| Data warehouse | Supabase PostgreSQL (schemas: staging, marts) |
| Transformations | dbt Core + dbt-postgres — 5 models across 3 layers |
| Dashboard | Power BI Desktop — 4 confirmed charts |
| PDF parsing | pdfplumber |
| Pipeline type | Batch, weekly, Friday schedule |

## Data Sources (CONFIRMED from reading real PDFs)

### Source 1 — DCHHS (Dallas County — primary)
- URL pattern: dallascounty.org/Assets/uploads/docs/hhs/influenza-surveillance/YEAR/filename.pdf
- Index page: dallascounty.org/departments/dchhs/data-reports/influenza-surveillance.php
- Storage bucket: flu-pdfs-dchhs
- Scripts: ingestion/dchhs/scrape_dchhs.py, ingestion/dchhs/parse_dchhs.py

Confirmed extractable fields (printed as numbers in the PDF):
- report_week_end_date — narrative text "Week Ending MM/DD/YYYY"
- mmwr_week — narrative text "CDC Week NN"
- total_tests_performed — Table 1
- total_positive_tests — Table 1
- pct_positive — Table 1 (printed as decimal e.g. 0.7, multiply by 1 = already a percent)
- flu_a_count — Table 1
- flu_b_count — Table 1
- flu_hospitalizations — Table 2
- icu_admissions — Table 2
- pediatric_deaths — Table 2
- school_absenteeism_pct — narrative text bullet 4
- school_ili_pct — narrative text bullet 4

NOT in DCHHS PDFs (do not try to extract):
- ILI % (chart image only, not a printed number)
- Age group breakdown (does not exist)

### Source 2 — DSHS (Texas state — supplemental)
- URL pattern: dshs.texas.gov/sites/default/files/IDCU/disease/respiratory_virus_surveillance/YEAR/YEAR-weekNN-trvsreport.pdf
- Storage bucket: flu-pdfs-dshs
- Scripts: ingestion/dshs/scrape_dshs.py, ingestion/dshs/parse_dshs.py

Confirmed extractable fields:
- ili_pct — Table 4
- ili_baseline_pct — Table 4 (printed explicitly e.g. 4.40%)
- providers_reporting — Table 4
- age_0_4_ili, age_5_24_ili, age_25_49_ili, age_50_64_ili, age_65_plus_ili — Table 5
- total_ili_cases, total_patient_visits — Table 5
- flu_a_count, flu_b_count, h1n1_count, h3n2_count — Table 2

IMPORTANT: Table indices in parse_dchhs.py and parse_dshs.py MUST be verified
by running the --debug flag on real PDFs before running full batch.
Use a January PDF for debugging (richer data than October).

## Season Scope
- Season 1: 2023-2024 → MMWR Wk 40/2023 (Oct 1) through Wk 20/2024 (May 18)
- Season 2: 2024-2025 → MMWR Wk 40/2024 (Sep 29) through Wk 20/2025 (May 17)
- ~30 PDFs per source per season = ~120 PDFs total
- Build season 1 end-to-end first, then add season 2 via re-run (upsert handles it)

## Database Schema (4 tables — do not add or remove columns)

### staging.stg_dchhs_weekly
```sql
CREATE TABLE IF NOT EXISTS staging.stg_dchhs_weekly (
    id                     SERIAL PRIMARY KEY,
    report_week_end_date   DATE         NOT NULL UNIQUE,
    mmwr_week              INTEGER      NOT NULL,
    flu_season             VARCHAR(9)   NOT NULL,
    total_tests_performed  INTEGER,
    total_positive_tests   INTEGER,
    pct_positive           NUMERIC(5,2),
    flu_a_count            INTEGER,
    flu_b_count            INTEGER,
    flu_hospitalizations   INTEGER,
    icu_admissions         INTEGER,
    pediatric_deaths       INTEGER,
    school_absenteeism_pct NUMERIC(5,2),
    school_ili_pct         NUMERIC(5,2),
    source_pdf_filename    TEXT,
    ingested_at            TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dchhs_season_week
    ON staging.stg_dchhs_weekly (flu_season, mmwr_week);
```

### staging.stg_dshs_weekly
```sql
CREATE TABLE IF NOT EXISTS staging.stg_dshs_weekly (
    id                     SERIAL PRIMARY KEY,
    report_week_end_date   DATE         NOT NULL UNIQUE,
    mmwr_week              INTEGER      NOT NULL,
    flu_season             VARCHAR(9)   NOT NULL,
    ili_pct                NUMERIC(5,2),
    ili_baseline_pct       NUMERIC(5,2),
    above_baseline         BOOLEAN,
    providers_reporting    INTEGER,
    age_0_4_ili            INTEGER,
    age_5_24_ili           INTEGER,
    age_25_49_ili          INTEGER,
    age_50_64_ili          INTEGER,
    age_65_plus_ili        INTEGER,
    total_ili_cases        INTEGER,
    total_patient_visits   INTEGER,
    flu_a_count            INTEGER,
    flu_b_count            INTEGER,
    h1n1_count             INTEGER,
    h3n2_count             INTEGER,
    source_pdf_filename    TEXT,
    ingested_at            TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dshs_season_week
    ON staging.stg_dshs_weekly (flu_season, mmwr_week);
```

### marts.mart_flu_weekly (built by dbt)
Key columns: all DCHHS + DSHS fields joined on report_week_end_date, plus:
- season_week_num (ROW_NUMBER per season)
- rolling_4wk_avg_pct (4-week moving average of pct_positive)
- wow_pct_change (week-over-week change)
- is_peak_week (TRUE for max pct_positive in season)
Indexed on (flu_season, mmwr_week)

### marts.mart_flu_seasonal_summary (built by dbt)
One row per season. Key columns:
- flu_season, total_weeks_reported
- sum_total_tests, sum_positive_tests, sum_flu_a, sum_flu_b, sum_hospitalizations
- peak_pct_positive, peak_week_end_date, peak_mmwr_week
- ili_baseline_pct, weeks_above_baseline, pct_weeks_above_baseline
- severity_label (Low/Moderate/High/Very High), severity_color (hex)
- dominant_strain, flu_a_pct, flu_b_pct

## dbt Model Architecture (5 models, build in this order)
```
staging.stg_dchhs_weekly  (source) ──┐
staging.stg_dshs_weekly   (source) ──┤
                                      ▼
                    stg_dchhs_weekly.sql  (staging view — cleans DCHHS)
                    stg_dshs_weekly.sql   (staging view — cleans DSHS)
                                      ▼
                    int_flu_weekly_combined.sql  (intermediate view — JOIN + window functions)
                                      ▼
                    mart_flu_weekly.sql           (marts table — Power BI Charts 1, 3)
                    mart_flu_seasonal_summary.sql (marts table — Power BI Charts 2, 4)
```

## Kestra Flows (3 flows, chain automatically)
- Flow 01: scrape_all_pdfs — runs both scrapers, uploads to 2 buckets. Schedule: 0 9 * * 5
- Flow 02: parse_and_load — triggers after Flow 01 SUCCESS. Parses both sources, loads staging.
- Flow 03: run_dbt — triggers after Flow 02 SUCCESS. dbt run + dbt test.

## Power BI Dashboard (4 confirmed charts)
All fields verified from real PDFs. Connect to marts schema only.

| # | Chart | Type | Source table | Key fields |
|---|---|---|---|---|
| 1 | % Positive by Week | Line (dual season + baseline) | mart_flu_weekly | report_week_end_date, pct_positive, rolling_4wk_avg_pct, ili_baseline_pct, flu_season |
| 2 | Flu A vs Flu B | 100% Stacked bar | mart_flu_seasonal_summary | flu_season, sum_flu_a, sum_flu_b |
| 3 | Weekly Hospitalizations | Clustered bar | mart_flu_weekly | report_week_end_date, flu_hospitalizations, flu_season |
| 4 | Season Severity Scorecards | KPI cards | mart_flu_seasonal_summary | severity_label, peak_pct_positive, weeks_above_baseline, dominant_strain |

Age group charts were deliberately excluded — age data is Texas statewide (DSHS),
not Dallas County, so using it as a primary chart would create a data source mismatch.
Age columns are retained in mart_flu_weekly for future use only.

## Environment Variables (all 6 required)
```
SUPABASE_URL          — https://YOUR_REF.supabase.co
SUPABASE_KEY          — service_role key (not anon key)
SUPABASE_DB_URL       — postgresql://postgres:PASSWORD@db.REF.supabase.co:5432/postgres
SUPABASE_DB_PASSWORD  — database password
SUPABASE_PROJECT_REF  — short alphanumeric ref ID
SUPABASE_ACCESS_TOKEN — sbp_... personal access token
```
In Codespaces: stored as GitHub Codespaces Secrets, injected automatically.
Locally: stored in .env file (gitignored). Load with python-dotenv.

## Folder Structure
```
dallas-flu-pipeline/
├── .devcontainer/devcontainer.json
├── terraform/
│   ├── main.tf              ← provisions 2 buckets + 2 staging tables + Kestra
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars     ← gitignored (real values)
├── kestra/flows/
│   ├── 01_scrape_all_pdfs.yml
│   ├── 02_parse_and_load.yml
│   └── 03_run_dbt.yml
├── ingestion/
│   ├── dchhs/
│   │   ├── scrape_dchhs.py
│   │   └── parse_dchhs.py    ← run with --debug FILENAME to inspect table indices
│   ├── dshs/
│   │   ├── scrape_dshs.py
│   │   └── parse_dshs.py     ← run with --debug FILENAME to inspect table indices
│   └── load_staging.py       ← upserts both CSVs into Supabase staging tables
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml          ← gitignored
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_dchhs_weekly.sql
│   │   │   ├── stg_dshs_weekly.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   └── int_flu_weekly_combined.sql
│   │   └── marts/
│   │       ├── mart_flu_weekly.sql
│   │       └── mart_flu_seasonal_summary.sql
│   └── tests/
│       ├── assert_pct_in_range.sql
│       └── assert_no_future_dates.sql
├── data/                     ← gitignored
├── .env.example              ← committed (placeholders only)
├── .env                      ← gitignored (real values)
├── .gitignore
├── requirements.txt
└── README.md
```

## Rubric Checklist (target 28/28 + 9 bonus)
- [x] Problem description — 4 analytical questions, real gap documented
- [ ] Cloud + IaC — Codespaces + Terraform (terraform apply must provision everything)
- [ ] Batch orchestration — Kestra 3-flow DAG, uploads to data lake buckets
- [ ] Data warehouse — Supabase PostgreSQL, composite indexes, explanation in README
- [ ] Transformations — dbt 5 models, schema tests + 2 custom SQL tests, docs screenshot
- [ ] Dashboard — Power BI 4 charts (mix of temporal + categorical)
- [ ] Reproducibility — README step-by-step, .env.example committed, scripts run in order
- [ ] Peer reviews — review 3 classmates (+3 pts each = +9 bonus)

## Important Decisions (do not re-litigate these)
1. Age group breakdown excluded from dashboard charts — DCHHS has no age data
2. DSHS is supplemental, not primary — always mention both sources in README
3. Upsert (ON CONFLICT DO UPDATE) used throughout — re-running is safe
4. Build season 2023-2024 first end-to-end, then add 2024-2025 via re-run
5. Table indices in parsers MUST be verified with --debug before full batch run
6. pdfplumber chosen over PyMuPDF — better table extraction for these PDFs

## Current Status
- [x] Architecture finalized
- [x] Schema confirmed against real PDFs
- [x] Build guide v2 written (dallas_flu_build_guide_v2.docx)
- [x] GitHub repo created (tanphi-1/dallas-flu-pipeline)
- [ ] Folder structure committed to repo
- [ ] Supabase project created
- [ ] Terraform written and applied
- [ ] Ingestion scripts written and tested
- [ ] dbt models written and tested
- [ ] Kestra flows written and tested
- [ ] Power BI dashboard built
- [ ] README written
