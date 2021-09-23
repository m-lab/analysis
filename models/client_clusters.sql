# This is a kmeans clustering of clients, to discover the clusters of clients,
# by metro, based on time of day and day of the week, and other testing behaviors.
# This will only be meaningful for clients that test a significant number of
# times over the interval of interest, so it is beneficial to use fairly
# large intervals.  We will try 13 week intervals, to get consistent number
# of days of the week.

# bq query --use_legacy_sql=false < models/client_clusters.sql

CREATE OR REPLACE MODEL
  `mlab-sandbox.gfr.client_clusters_model_alt_30` OPTIONS(model_type='kmeans',
    num_clusters=30) AS

alternate AS (
  SELECT 
    training_stats.* EXCEPT(uploads, downloads, days, hours),
    SAFE.LOG10(training_stats.tests) AS logTests,
  FROM `mlab-sandbox.gfr.client_stats`
  WHERE training_stats.tests > 5
)

SELECT * FROM alternate