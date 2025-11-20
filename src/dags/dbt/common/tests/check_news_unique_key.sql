SELECT
	company_key
  , content_id
  , COUNT(*) AS cnt
FROM {{ ref('news') }}
GROUP BY
	1
  , 2
HAVING 1 > COUNT(*) AND content_id IS NOT NULL
