WITH top_5_industry AS (SELECT *
                        FROM {{ref('stg_company_detail')}}
                        ORDER BY market_weight DESC
	LIMIT 5)
SELECT *
FROM {{ref("stg_news_external_table")}} AS news
	     JOIN top_5_industry ii
ON news.industry_key = ii.industry_key