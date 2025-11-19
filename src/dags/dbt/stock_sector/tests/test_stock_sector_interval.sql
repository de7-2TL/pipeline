WITH target
	     AS (SELECT lag(s.datetime, 1) OVER (partition BY s.sector ORDER BY s.datetime) AS prev, s.datetime AS curr,
	                datediff('minute', prev, curr) AS diff
	         FROM {{ref("stock_sector")}} AS s)
SELECT target.diff
FROM target
WHERE diff < 15