-- from .../analysis/inspector/summary_views/{{query}}
--

SELECT * FROM (
  (SELECT * FROM `{{ProjectID}}.{{Datasource}}`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.{{AdditionalArg}}`)
)
ORDER BY YEAR, Source
