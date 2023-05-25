
-- REPORT 1
SELECT d.department,j.job,
        COUNT(CASE WHEN EXTRACT(quarter FROM TO_DATE(he.datetime, 'YYYY-MM-DD')) = 1 THEN 1 END) AS Q1,
        COUNT(CASE WHEN EXTRACT(quarter FROM TO_DATE(he.datetime, 'YYYY-MM-DD')) = 2 THEN 1 END) AS Q2,
        COUNT(CASE WHEN EXTRACT(quarter FROM TO_DATE(he.datetime, 'YYYY-MM-DD')) = 3 THEN 1 END) AS Q3,
        COUNT(CASE WHEN EXTRACT(quarter FROM TO_DATE(he.datetime, 'YYYY-MM-DD')) = 4 THEN 1 END) AS Q4
FROM migration.hired_employees he
INNER JOIN migration.departments d on (d.id = he.department_id) 
INNER JOIN migration.jobs j on (j.id = he.job_id) 
WHERE EXTRACT(year FROM TO_DATE(he.datetime, 'YYYY-MM-DD')) = 2021
GROUP BY d.department, j.job
ORDER BY d.department asc, j.job asc

-- REPORT 2
WITH department_hires AS (
  SELECT he.department_id, COUNT(*) AS hire_count
  FROM migration.hired_employees he
  WHERE EXTRACT(year FROM TO_DATE(he.datetime, 'YYYY-MM-DD')) = 2021
  GROUP BY department_id
)
,department_avg AS (
  SELECT AVG(hire_count) AS avg_hires
  FROM department_hires
)
SELECT d.id, d.department, dh.hire_count AS hired
FROM migration.departments d
INNER JOIN department_hires dh ON dh.department_id = d.id
INNER JOIN department_avg da ON dh.hire_count > da.avg_hires
ORDER BY dh.hire_count DESC;