with connector as (
    SELECT
        a.DATETIME
        , a.open
        , a.high
        , a.low
        , a.close
        , a.volume
        , b.company_name
        , b.industry_name
        , b.sector_key as sector
    FROM
        {{ ref("stock_company") }} as a
    JOIN    
        {{ ref("stg_company_detail") }} as b
    on 
        a.company_symbol = b.company_symbol
)
select * from connector