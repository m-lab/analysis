-- from .../analysis/inspector/summary_views/{{query}}
SELECT
  "{{Datasource}}" AS Source,
  EXTRACT(YEAR FROM date) AS year,
  "{{query}}" AS inspector,
  COUNT(*) AS tests,
  MIN(date) AS First, MAX(date) AS Last,
  MIN(ParseTime) as OldestParse,
  COUNT(DISTINCT file) AS files,
  COUNT(*) - COUNT(DISTINCT uuid) AS DupUUID,
FROM (
  SELECT
    DATE( log_time ) AS date,
    test_id AS uuid,
    task_filename AS file,
    parse_time AS ParseTime,
  FROM `{{Datasource}}`
)

GROUP BY year, Source
ORDER BY year, Source
