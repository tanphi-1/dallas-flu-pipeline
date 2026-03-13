SELECT * FROM {{ ref('stg_dchhs_weekly') }}
WHERE report_week_end_date > CURRENT_DATE
