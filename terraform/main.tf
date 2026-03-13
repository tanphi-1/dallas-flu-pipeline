terraform {
  required_providers {
    supabase = { source = "supabase/supabase"; version = "~> 1.0" }
    docker   = { source = "kreuzwerker/docker"; version = "~> 3.0" }
    null     = { source = "hashicorp/null"; version = "~> 3.0" }
  }
}

provider "supabase" { access_token = var.supabase_access_token }
provider "docker" {}

# ── DATA LAKE: Two storage buckets ───────────────────────
resource "supabase_storage_bucket" "dchhs_pdfs" {
  project_ref = var.supabase_project_ref
  id          = "flu-pdfs-dchhs"
  name        = "flu-pdfs-dchhs"
  public      = false
}

resource "supabase_storage_bucket" "dshs_pdfs" {
  project_ref = var.supabase_project_ref
  id          = "flu-pdfs-dshs"
  name        = "flu-pdfs-dshs"
  public      = false
}

# ── DATA WAREHOUSE: Schemas + Tables ─────────────────────
resource "null_resource" "db_schema" {
  provisioner "local-exec" {
    command = <<-EOF
      psql "${var.supabase_db_url}" <<SQL

      -- Schemas
      CREATE SCHEMA IF NOT EXISTS staging;
      CREATE SCHEMA IF NOT EXISTS marts;

      -- Table 1: DCHHS staging (Dallas lab data)
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

      -- Table 2: DSHS staging (Texas ILI + age groups + baseline)
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

      SQL
    EOF
  }
}

# ── ORCHESTRATION: Kestra Docker container ────────────────
resource "docker_image" "kestra" { name = var.kestra_image }

resource "docker_container" "kestra" {
  name    = "kestra"
  image   = docker_image.kestra.image_id
  restart = "unless-stopped"
  ports { internal = 8080; external = 8080 }
  volumes { host_path = "/tmp/kestra-data"; container_path = "/app/storage" }
}
