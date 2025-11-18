WITH company_detail AS (SELECT *
                        FROM {{ref("company_info")}} AS C
	LEFT JOIN {{ref("industry_info")}} i
ON C.industry_key = i.industry_key
	LEFT JOIN {{ref("sector_info")}} s ON i.sector_key = s.sector_key)
SELECT *
FROM company_detail
