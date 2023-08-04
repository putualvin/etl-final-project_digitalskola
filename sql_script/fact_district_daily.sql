SELECT
	dc.id
	,dc.district_id
	,c.id as case_id
	,dc.tanggal::date::varchar as "date"
	,SUM(dc.total) as total
FROM district dc
LEFT JOIN dim_case c
ON dc.status_covid = c.status
GROUP BY 1,2,3,4;








