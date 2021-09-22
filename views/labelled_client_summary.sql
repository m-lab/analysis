# Uses the clusters from client_clusters_model to label client groups, and compute aggregate group stats.

CREATE OR REPLACE VIEW `mlab-sandbox.gfr.client_cluster_summaries`
AS 

WITH labelled AS (
SELECT * EXCEPT(NEAREST_CENTROIDS_DISTANCE) FROM ML.PREDICT(MODEL `mlab-sandbox.gfr.client_clusters_model`,
 (SELECT metro, ClientIP, clientName, clientOS, wscale1, wscale2, downloads, meanSpeed, meanMinRTT, LOG10(tests) AS logTests,
days, hours,
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
FROM `mlab-sandbox.gfr.client_stats`
WHERE tests > 10 
)))

SELECT metro, CENTROID_ID, clientName, clientOS, wscale1, wscale2, COUNT(*) AS clients, SUM(downloads) AS downloads,
ROUND(EXP(SUM(downloads*SAFE.LN(meanSpeed))/SUM(downloads)),3) AS meanSpeed,
ROUND(EXP(AVG(SAFE.LN(meanSpeed))),3) AS debiasedSpeed,
ROUND(100*SAFE_DIVIDE(EXP(STDDEV(SAFE.LN(meanSpeed))), EXP(AVG(SAFE.LN(meanSpeed)))),1) AS speedDev,
ROUND(EXP(AVG(SAFE.LN(meanMinRTT))),2) AS debiasedMinRTT,
FROM labelled
GROUP BY metro, CENTROID_ID, clientName, clientOS, wscale1, wscale2
