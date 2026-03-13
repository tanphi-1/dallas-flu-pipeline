SELECT * FROM {{ ref('stg_dchhs_weekly') }}
WHERE pct_positive < 0 OR pct_positive > 100
