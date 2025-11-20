{{ config(
    schema= generate_schema_name('staging_data', this)
) }}

WITH company_sector AS (
    SELECT 
        a.company_symbol,
        a.company_name,
        a.industry_key,
        b.sector
    FROM {{ ref("stg_company_detail") }} AS a
    JOIN {{ ref("stg_stock_top_sector") }} AS b
    ON a.sector_key = b.sector
)
select * from company_sector