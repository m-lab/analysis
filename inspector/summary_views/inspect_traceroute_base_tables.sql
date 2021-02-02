-- from .../analysis/inspector/summary_views/{{query}}
SELECT
  "{{Datasource}}" AS Source,
  EXTRACT({{dates}} FROM date) AS dates,
  "{{query}}" AS inspector,
  COUNT(*) AS tests,
  MIN(date) AS First, MAX(date) AS Last,
  MIN(ParseTime) as OldestParse,
  COUNT(DISTINCT file) AS files,
  COUNT(*) - COUNT(DISTINCT uuid) AS DupUUID,
FROM (
  SELECT
    DATE(_partitiontime) AS date,
    uuid,
    Parseinfo.TaskFilename AS file,
    Parseinfo.ParseTime,
  FROM `{{Datasource}}`
)

GROUP BY dates, Source
ORDER BY dates, Source
