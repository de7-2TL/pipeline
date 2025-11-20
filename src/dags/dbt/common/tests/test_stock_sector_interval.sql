WITH target AS (SELECT
	                LAG(s.datetime, 1) OVER (PARTITION BY s.sector ORDER BY s.datetime) AS prev
                  , s.datetime                                                          AS curr
                  , DATEDIFF('minute', prev, curr)                                      AS diff
                FROM {{ref("stock_sector")}} AS S
                )
SELECT
	target.diff
FROM target
WHERE diff < 15
