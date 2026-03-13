{{
    config(
        materialized='table',
        indexes=[{'columns':['flu_season','mmwr_week'],'unique':false}]
    )
}}
SELECT * FROM {{ ref('int_flu_weekly_combined') }}
ORDER BY report_week_end_date
