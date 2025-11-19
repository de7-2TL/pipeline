SELECT company_key,
       content_id,
       COUNT(*) AS cnt
FROM {{ ref('news') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
