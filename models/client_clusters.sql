# This is a kmeans clustering of clients, to discover the clusters of clients,
# by metro, based on time of day and day of the week.
# This will only be meaningful for clients that test a significant number of
# times over the interval of interest, so it is beneficial to use fairly
# large intervals.  We will try 13 week intervals, to get consistent number
# of days of the week.

# bq query --use_legacy_sql=false < models/client_clusters.sql

CREATE OR REPLACE MODEL
  `mlab-sandbox.gfr.client_clusters_model_intervals_30` OPTIONS(model_type='kmeans',
    num_clusters=30) AS

WITH linear AS (
SELECT LOG10(tests) AS logTests,
days, hours,
downloadInterval, downloadIntervalVariability, # interval should be loosely related to number of tests (and start and end date)
duBalance, # balance between downloads (+1) and uploads (-1)
sunday/tests AS sunday,
monday/tests AS monday,
tuesday/tests AS tuesday,
wednesday/tests AS wednesday,
thursday/tests AS thursday,
friday/tests AS friday,
saturday/tests AS saturday,
t00/tests AS t00,
t03/tests AS t03,
t06/tests AS t06,
t09/tests AS t09,
t12/tests AS t12,
t15/tests AS t15,
t18/tests AS t18,
t21/tests AS t21,
FROM `mlab-sandbox.gfr.client_stats_interval`
WHERE tests > 10
)

SELECT * FROM linear