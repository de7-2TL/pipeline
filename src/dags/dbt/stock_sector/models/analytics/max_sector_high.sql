WITH
sector_change AS (
		SELECT datetime,
	          sector,
	          high,
	          LAG(high) OVER (PARTITION BY sector ORDER BY datetime) AS prev_high,
	          (high - LAG(high) OVER (PARTITION BY sector ORDER BY datetime))
	              /
	          NULLIF(LAG(high) OVER (PARTITION BY sector ORDER BY datetime), 0)
	                                                                 AS high_change_rate
	   FROM {{ref('stock_sector')}}
),
latest_rank AS (
SELECT DATETIME, sector, high, prev_high, high_change_rate, ROW_NUMBER() OVER (PARTITION BY DATETIME ORDER BY high_change_rate DESC) AS rn
FROM sector_change
WHERE prev_high IS NOT NULL
) SELECT
       lr.datetime,
	   lr.sector      AS sector_key,
	   si.sector_name AS sector_name,
	   lr.high,
	   lr.prev_high,
	   lr.high_change_rate
FROM latest_rank lr
	JOIN {{ref("sector_info")}} si ON lr.sector = si.sector_key
WHERE rn = 1
ORDER BY DATETIME DESC