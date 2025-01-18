WITH cte AS (
SELECT ROW_NUMBER () OVER (PARTITION BY client_rk, effective_from_date) AS RN, * 
FROM dm.client
)
--DELETE
SELECT *
FROM cte c
WHERE RN > 1;


--SELECT *
DELETE 
FROM dm.client 
WHERE CTID NOT IN (
	SELECT MIN(CTID)
  	FROM dm.client
  	GROUP BY client_rk, effective_from_date
);

