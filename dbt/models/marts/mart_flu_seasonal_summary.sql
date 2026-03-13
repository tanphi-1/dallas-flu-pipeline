WITH weekly AS (SELECT * FROM {{ ref('int_flu_weekly_combined') }}),

agg AS (
    SELECT
        flu_season,
        COUNT(*)                                     AS total_weeks_reported,
        SUM(total_tests_performed)                   AS sum_total_tests,
        SUM(total_positive_tests)                    AS sum_positive_tests,
        SUM(dallas_flu_a)                            AS sum_flu_a,
        SUM(dallas_flu_b)                            AS sum_flu_b,
        SUM(flu_hospitalizations)                    AS sum_hospitalizations,
        SUM(icu_admissions)                          AS sum_icu_admissions,
        MAX(flu_hospitalizations)                    AS peak_hospitalizations,
        MAX(pct_positive)                            AS peak_pct_positive,
        MIN(report_week_end_date)
            FILTER (WHERE is_peak_week)              AS peak_week_end_date,
        MIN(mmwr_week)
            FILTER (WHERE is_peak_week)              AS peak_mmwr_week,
        MAX(ili_baseline_pct)                        AS ili_baseline_pct,
        COUNT(*) FILTER (WHERE above_baseline)       AS weeks_above_baseline,
        COUNT(*) FILTER (WHERE ili_pct IS NOT NULL)  AS total_weeks_with_ili,
        ROUND(COUNT(*) FILTER (WHERE above_baseline)::NUMERIC
            / NULLIF(COUNT(*) FILTER (WHERE ili_pct IS NOT NULL),0)*100,1)
                                                     AS pct_weeks_above_baseline
    FROM weekly
    GROUP BY flu_season
),

with_labels AS (
    SELECT *,
        CASE
            WHEN peak_pct_positive < 5.0  THEN 'Low'
            WHEN peak_pct_positive < 15.0 THEN 'Moderate'
            WHEN peak_pct_positive < 30.0 THEN 'High'
            ELSE 'Very High'
        END AS severity_label,
        CASE
            WHEN peak_pct_positive < 5.0  THEN '#2ECC71'
            WHEN peak_pct_positive < 15.0 THEN '#F39C12'
            WHEN peak_pct_positive < 30.0 THEN '#E67E22'
            ELSE '#E74C3C'
        END AS severity_color,
        CASE
            WHEN ABS(sum_flu_a - sum_flu_b)::NUMERIC
                / NULLIF(sum_flu_a + sum_flu_b,0) < 0.15 THEN 'Co-dominant'
            WHEN sum_flu_a >= sum_flu_b THEN 'Flu A'
            ELSE 'Flu B'
        END AS dominant_strain,
        ROUND(sum_flu_a::NUMERIC
            / NULLIF(sum_flu_a+sum_flu_b,0)*100,1) AS flu_a_pct,
        ROUND(sum_flu_b::NUMERIC
            / NULLIF(sum_flu_a+sum_flu_b,0)*100,1) AS flu_b_pct
    FROM agg
)
SELECT * FROM with_labels ORDER BY flu_season
