-- from .../analysis/inspector/summary_views/{{query}}
-- This is the same as inspect_ndt_components, except it uses archaic column names:
-- test_date instead of date
-- (Potentially others in the future)
SELECT
  EXTRACT({{dates}} from test_date) AS dates,
  "{{AdditionalArg}}" AS SubSource,
  "{{query}}" AS inspector,
  "{{Datasource}}_{{AdditionalArg}}" AS Source,

  MIN(test_date) as first, MAX(test_date) as last,

  -- a.MeanThroughputMbps, a.MinRTT, a.LossRate
  COUNT(*) AS tests,
  COUNT(*) - COUNT(DISTINCT a.UUID) AS DupUUID,
  COUNT(DISTINCT server.Site) AS Ssites,
  COUNT(DISTINCT REGEXP_EXTRACT(server.Site, '[a-z]{3}')) AS Siata,
  COUNT(DISTINCT CONCAT(server.Geo.latitude, server.Geo.longitude)) AS SLatLong,
  COUNT(DISTINCT CONCAT(server.Site,Server.Machine)) AS Servers,
  COUNTIF(Server.Machine = 'mlab4') AS Smlab4,
  COUNTIF(REGEXP_CONTAINS(Server.Site, '[0-9]t')) AS S01t,
  ROUND(100*COUNTIF(0.1 <= a.MeanThroughputMbps AND a.MeanThroughputMbps < 500.0) / count(*)) AS OkRate,
  ROUND(100*COUNTIF(1.0 <= a.MinRTT AND a.MinRTT < 500.0) / count(*)) AS OkRTT,
  ROUND(100*COUNTIF(0.0 <= a.LossRate AND a.LossRate < 0.5) / count(*)) AS OkLoss,
  ROUND(100*COUNTIF( client.Geo.latitude != 0.0 OR client.Geo.longitude != 0.0 ) / count(*)) AS OkLatLong,
  ROUND(100*COUNTIF( LENGTH(client.Geo.country_code)>1 ) / count(*)) AS OkCountry,
  0.0 AS OKRegion,
#  ROUND(100*COUNTIF( LENGTH(client.Geo.Subdivision1Name)>1 ) / count(*)) AS OkSub1Name,
  ROUND(100*COUNTIF( LENGTH(client.Geo.postal_code)>1 ) / count(*)) AS OkPostalC,
  ROUND(100*COUNTIF( client.Network.ASnumber IS NOT NULL ) / count(*)) AS OkASN,
#  ROUND(100*COUNTIF( client.Network.ASName IS NOT NULL ) / count(*)) AS OkASName,
-- ROUND(100*COUNTIF( TRUE ) / count(*)) AS OkNews,
FROM `{{Datasource}}_{{AdditionalArg}}`
WHERE test_date > '2008-01-01'
      AND filter.IsValidBest

GROUP BY dates, Source
ORDER BY dates, Source
