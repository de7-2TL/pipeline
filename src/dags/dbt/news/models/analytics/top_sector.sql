with sector_inudstry_combined as (
	select
		a.sector,
		b.industry_key,
		b.industry_name,
		b.market_weight
	FROM {{ ref("stg_stock_top_sector") }} as a
	inner join {{ ref("industry_info") }} as b
	on a.sector = b.sector_key
)
select *
from sector_inudstry_combined
order by market_weight desc