terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

# ── DATA LAKE: Two storage buckets via Supabase REST API ──
resource "null_resource" "storage_buckets" {
  provisioner "local-exec" {
    command = <<-EOF
      curl -s -X POST "${var.supabase_url}/storage/v1/bucket" \
        -H "Authorization: Bearer ${var.supabase_key}" \
        -H "Content-Type: application/json" \
        -d '{"id":"flu-pdfs-dchhs","name":"flu-pdfs-dchhs","public":false}'

      curl -s -X POST "${var.supabase_url}/storage/v1/bucket" \
        -H "Authorization: Bearer ${var.supabase_key}" \
        -H "Content-Type: application/json" \
        -d '{"id":"flu-pdfs-dshs","name":"flu-pdfs-dshs","public":false}'
    EOF
  }
}

# ── DATA WAREHOUSE: Schemas + Tables ─────────────────────
resource "null_resource" "db_schema" {
  provisioner "local-exec" {
    command = <<-EOF
      python3 -c "
import psycopg2
conn = psycopg2.connect('${var.supabase_db_url}')
conn.autocommit = True
cur = conn.cursor()
cur.execute('''
  CREATE SCHEMA IF NOT EXISTS staging;
  CREATE SCHEMA IF NOT EXISTS marts;

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
''')
cur.close()
conn.close()
print('Database schemas and tables created successfully.')
"
    EOF
  }
}

# ── ORCHESTRATION: Kestra Docker container ────────────────
resource "docker_image" "kestra" {
  name = var.kestra_image
}

resource "docker_container" "kestra" {
  name    = "kestra"
  image   = docker_image.kestra.image_id
  restart = "unless-stopped"
  ports {
    internal = 8080
    external = 8080
  }
  volumes {
    host_path      = "/tmp/kestra-data"
    container_path = "/app/storage"
  }
}
