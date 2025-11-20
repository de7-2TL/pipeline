WITH news_dedup AS (
    SELECT
        title
        , summary
        , company_key
        , pubDate
        , ROW_NUMBER() OVER (
            PARTITION BY company_key
            ORDER BY pubdate DESC 
        ) AS rn
    FROM {{ ref("news") }}
)
SELECT 	
    b.pubDate,
    a.company_name,
    a.sector,
    a.industry_key,
    b.title,
    b.summary
FROM {{ ref("stg_company_sector_con") }} AS a
JOIN news_dedup b
    ON a.company_symbol = b.company_key 
WHERE b.rn <= 5
ORDER BY a.COMPANY_NAME, b.PUBDATE desc
