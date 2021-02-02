-- from .../analysis/inspector/summary_views/{{query}}
SELECT
  CONCAT("{{Datasource}}_unified_", dir) AS Source,
  EXTRACT({{dates}} from date) AS dates,
  node._instruments AS SubSource,
  "{{query}}" AS inspector,

  MIN(date) as first, MAX(date) as last,

  COUNT(*) AS tests,
  COUNT(*) - COUNT(DISTINCT a.UUID) AS DupUUID,
  COUNT(DISTINCT server.Site) AS Ssites,
  COUNT(DISTINCT REGEXP_EXTRACT(server.Site, '[a-z]{3}')) AS Siata,
  COUNT(DISTINCT CONCAT(server.Geo.latitude, server.Geo.longitude)) AS SLatLong,
  COUNT(DISTINCT CONCAT(server.Site,Server.Machine)) AS Servers,
  COUNTIF(Server.Machine = 'mlab4') AS Smlab4,
  COUNTIF(REGEXP_CONTAINS(Server.Site, '[0-9]t')) AS S01t,

-- a.MeanThroughputMbps, a.MinRTT, a.LossRate
  ROUND(100*COUNTIF(0.1 <= a.MeanThroughputMbps AND a.MeanThroughputMbps < 500.0) / count(*)) AS OkRate,
  ROUND(100*COUNTIF(1.0 <= a.MinRTT AND a.MinRTT < 500.0) / count(*)) AS OkRTT,
  ROUND(100*COUNTIF(0.0 <= a.LossRate AND a.LossRate < 0.5) / count(*)) AS OkLoss,
  ROUND(100*COUNTIF( LENGTH(client.Geo.country_code)>1 ) / count(*)) AS OkClientCountry,
  ROUND(100*COUNTIF( client.Geo.latitude != 0.0 OR client.Geo.longitude != 0.0 ) / count(*)) AS OkLatLong,
  ROUND(100*COUNTIF( Server.Network.ASnumber IS NOT NULL ) / count(*)) AS OkASN
-- ROUND(100*COUNTIF( TRUE ) / count(*)) AS OkNews,
FROM (
  (SELECT *, 'downloads' AS dir, node._instruments FROM `{{Datasource}}.unified_downloads`)
UNION ALL
  (SELECT *, 'uploads' AS dir, node._instruments FROM `{{Datasource}}.unified_uploads`)
)
GROUP BY Source, dates, SubSource
ORDER BY Source, dates, SubSource
