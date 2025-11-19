{{ config(
    schema= generate_schema_name('analytics_data', this)
) }}

WITH news_sector AS (
    SELECT 
        a.pubdate,
        a.title,
        a.summary,
        a.company_key,
        b.company_name,
        b.sector
    FROM {{ ref("stg_news_external_table") }} AS a
    JOIN {{ ref("stg_company_sector_con") }} AS b
    ON a.company_key = b.company_symbol
),
news_dedup AS (
    SELECT 
        pubdate,
        title,
        summary,
        company_key,
        company_name,
        sector,
        ROW_NUMBER() OVER (
            PARTITION BY sector
            ORDER BY pubdate DESC
        ) AS rn
    FROM news_sector
)
SELECT
    pubdate,
    title,
    summary,
    company_name,
    sector
FROM news_dedup
WHERE rn <= 5
ORDER BY pubdate DESC

