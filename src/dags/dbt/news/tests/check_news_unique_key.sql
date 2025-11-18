SELECT company_key,
       content_id,
       COUNT(*) AS cnt
FROM {{ source('staging', 'stg_news_external_table') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
