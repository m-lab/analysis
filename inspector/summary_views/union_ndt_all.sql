-- from .../analysis/inspector/summary_views/{{query}}
--

SELECT * FROM (
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_prod_ndt7_downloads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_prod_ndt7_uploads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_prod_ndt5_downloads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_prod_ndt5_uploads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_prod_web100_downloads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_prod_web100_uploads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_{{Datasource}}_ndt7_downloads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_{{Datasource}}_ndt7_uploads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_{{Datasource}}_ndt5_downloads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_{{Datasource}}_ndt5_uploads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_{{Datasource}}_web100_downloads`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_ndt_{{Datasource}}_web100_uploads`)
)
ORDER BY YEAR, Source
