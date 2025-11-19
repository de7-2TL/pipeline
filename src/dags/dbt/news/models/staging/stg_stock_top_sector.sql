{{ config(
    schema= generate_schema_name('staging_data', this)
) }}

WITH high_with_prev AS (
    SELECT  
        datetime,
        sector,
        high,
        LEAD(high) OVER (PARTITION BY sector ORDER BY datetime DESC) AS prev_high
    FROM {{ ref("stock_sector") }}
)

SELECT
    datetime,
    sector,
    high,
    prev_high,
    (high - prev_high) / prev_high AS diff_percent
FROM high_with_prev
WHERE prev_high IS NOT NULL
ORDER BY datetime DESC, diff_percent desc
LIMIT 1
