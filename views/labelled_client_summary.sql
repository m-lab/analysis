# Uses the clusters from client_clusters_model to label client groups, and compute aggregate group stats.

CREATE OR REPLACE VIEW `mlab-sandbox.gfr.client_cluster_summaries_30`
AS 

WITH
alternate AS (
SELECT * EXCEPT(NEAREST_CENTROIDS_DISTANCE) FROM ML.PREDICT(MODEL `mlab-sandbox.gfr.client_clusters_model_alt_30`,
  (SELECT metro, ClientIP, clientName, clientOS, wscale1, wscale2, performance_stats.*,
    training_stats.*,
    SAFE.LOG10(training_stats.tests) AS logTests,
  FROM `mlab-sandbox.gfr.client_stats`
  WHERE training_stats.tests > 10))
)

SELECT metro, CENTROID_ID, clientName, clientOS, wscale1, wscale2, COUNT(*) AS clients, SUM(tests) AS tests, SUM(downloads) AS downloads, 
  SUM(tests*dlFraction)/SUM(tests) AS dlFraction, # 0 to 1 indicating what fraction of tests are downloads
  SUM(tests*sunday)/SUM(tests) AS sunday,
  SUM(tests*monday)/SUM(tests) AS monday,
  SUM(tests*tuesday)/SUM(tests) AS tuesday,
  SUM(tests*wednesday)/SUM(tests) AS wednesday,
  SUM(tests*thursday)/SUM(tests) AS thursday,
  SUM(tests*friday)/SUM(tests) AS friday,
  SUM(tests*saturday)/SUM(tests) AS saturday,
  # Use mean speed per client, so that each client contributes equal weight to the average.
  ROUND(EXP(AVG(SAFE.LN(meanSpeed))),3) AS debiasedSpeed,
  # Speed deviation across clients in a cluster.  TODO - is this STDDEV computation valid?
  ROUND(100*SAFE_DIVIDE(EXP(STDDEV(SAFE.LN(meanSpeed))), EXP(AVG(SAFE.LN(meanSpeed)))),1) AS speedDev,
  ROUND(EXP(AVG(SAFE.LN(meanMinRTT))),2) AS debiasedMinRTT,
  FROM alternate
GROUP BY metro, CENTROID_ID, clientName, clientOS, wscale1, wscale2

