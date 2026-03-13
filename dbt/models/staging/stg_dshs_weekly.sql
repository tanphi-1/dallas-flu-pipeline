WITH source AS (SELECT * FROM {{ source('staging','stg_dshs_weekly') }})
SELECT
    report_week_end_date,
    mmwr_week,
    flu_season,
    ili_pct,
    ili_baseline_pct,
    COALESCE(above_baseline,
        ili_pct > ili_baseline_pct)                AS above_baseline,
    providers_reporting,
    COALESCE(age_0_4_ili,   0)                    AS age_0_4_ili,
    COALESCE(age_5_24_ili,  0)                    AS age_5_24_ili,
    COALESCE(age_25_49_ili, 0)                    AS age_25_49_ili,
    COALESCE(age_50_64_ili, 0)                    AS age_50_64_ili,
    COALESCE(age_65_plus_ili,0)                   AS age_65_plus_ili,
    total_ili_cases,
    total_patient_visits,
    flu_a_count,
    flu_b_count,
    h1n1_count,
    h3n2_count,
    source_pdf_filename
FROM source
WHERE report_week_end_date IS NOT NULL
  AND flu_season IN ('2023-2024','2024-2025')
