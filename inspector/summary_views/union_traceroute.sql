-- from .../analysis/inspector/summary_views/{{query}}
--
-- This view is not particularly useful and needlessly expensive.  Make a copy
-- and comment out all views that you don't actually need.
--
-- It is most useful to confirm that all of the different inspection views have
-- conforming columns.  Simply confirming that it compiles is normally sufficient.
--

SELECT * FROM (
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_traceroute_prod`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_traceroute_prod_legacy`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_traceroute_oti`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_traceroute_staging`)
UNION ALL
  (SELECT * FROM `{{ProjectID}}.inspector.summarize_traceroute_sandbox`)
)
ORDER BY YEAR, Source
