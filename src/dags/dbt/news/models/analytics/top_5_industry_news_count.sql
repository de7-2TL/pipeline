SELECT COUNT(*) AS news_count,
       industry_key
FROM {{ref("fact_top_5_industry_news")}}
