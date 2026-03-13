WITH source AS (SELECT * FROM {{ source('staging','stg_dchhs_weekly') }})
SELECT
    report_week_end_date,
    mmwr_week,
    flu_season,
    EXTRACT(YEAR FROM report_week_end_date)::INT    AS report_year,
    NULLIF(total_tests_performed, 0)               AS total_tests_performed,
    total_positive_tests,
    -- Validate pct_positive is in range 0-100
    CASE WHEN pct_positive BETWEEN 0 AND 100
         THEN pct_positive ELSE NULL END            AS pct_positive,
    COALESCE(flu_a_count, 0)                       AS flu_a_count,
    COALESCE(flu_b_count, 0)                       AS flu_b_count,
    COALESCE(flu_hospitalizations, 0)              AS flu_hospitalizations,
    COALESCE(icu_admissions, 0)                    AS icu_admissions,
    COALESCE(pediatric_deaths, 0)                  AS pediatric_deaths,
    school_absenteeism_pct,
    school_ili_pct,
    source_pdf_filename
FROM source
WHERE report_week_end_date IS NOT NULL
  AND flu_season IN ('2023-2024','2024-2025')
