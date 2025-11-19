SELECT company_key,
       content_id,
       COUNT(*) AS cnt
FROM {{ ref('stg_news_external_table') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
