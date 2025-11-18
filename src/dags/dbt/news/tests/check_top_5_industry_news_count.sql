SELECT COUNT(1)
FROM {{ source('analytics', 'top_5_industry_news_count') }}
WHERE news_count BETWEEN 0 AND 5
