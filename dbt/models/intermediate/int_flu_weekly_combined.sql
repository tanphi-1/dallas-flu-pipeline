-- Joins DCHHS + DSHS on week date, adds window functions
WITH dchhs AS (SELECT * FROM {{ ref('stg_dchhs_weekly') }}),
     dshs  AS (SELECT * FROM {{ ref('stg_dshs_weekly')  }}),

joined AS (
    SELECT
        d.report_week_end_date,
        d.mmwr_week,
        d.flu_season,
        d.report_year,
        -- DCHHS fields (Dallas-specific)
        d.total_tests_performed,
        d.total_positive_tests,
        d.pct_positive,
        d.flu_a_count           AS dallas_flu_a,
        d.flu_b_count           AS dallas_flu_b,
        d.flu_hospitalizations,
        d.icu_admissions,
        d.pediatric_deaths,
        d.school_absenteeism_pct,
        d.school_ili_pct,
        -- DSHS fields (Texas context)
        s.ili_pct,
        s.ili_baseline_pct,
        s.above_baseline,
        s.providers_reporting,
        s.age_0_4_ili,
        s.age_5_24_ili,
        s.age_25_49_ili,
        s.age_50_64_ili,
        s.age_65_plus_ili,
        s.total_ili_cases,
        s.total_patient_visits,
        s.h1n1_count,
        s.h3n2_count
    FROM dchhs d
    LEFT JOIN dshs s USING (report_week_end_date)
),

with_windows AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY flu_season ORDER BY report_week_end_date
        ) AS season_week_num,

        ROUND(AVG(pct_positive) OVER (
            PARTITION BY flu_season
            ORDER BY report_week_end_date
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ),2) AS rolling_4wk_avg_pct,

        ROUND(pct_positive - LAG(pct_positive) OVER (
            PARTITION BY flu_season ORDER BY report_week_end_date
        ),2) AS wow_pct_change,

        CASE WHEN pct_positive = MAX(pct_positive) OVER (
            PARTITION BY flu_season) THEN TRUE ELSE FALSE
        END AS is_peak_week
    FROM joined
)
SELECT * FROM with_windows
