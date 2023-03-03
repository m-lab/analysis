-- from .../analysis/inspector/summary_views/{{query}}
SELECT
  EXTRACT({{dates}} from date) AS dates,
  "{{dates}}" AS binSize,
  "{{AdditionalArg}}" AS SubSource,
  "{{query}}" AS inspector,
  "{{Datasource}}_{{AdditionalArg}}" AS Source,

  MIN(date) as first, MAX(date) as last,

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
  ROUND(100*COUNTIF( LENGTH(client.Geo.continent_code)=2 ) / count(*)) AS OkContinent,
  ROUND(100*COUNTIF( LENGTH(client.Geo.country_code)=2 ) / count(*)) AS OkCountry,
  ROUND(100*COUNTIF( LENGTH(client.Geo.country_code3)>0 ) / count(*)) AS OkCountry3,
  ROUND(100*COUNTIF( LENGTH(client.Geo.country_name)>0 ) / count(*)) AS OkCntryName,
  ROUND(100*COUNTIF( LENGTH(client.Geo.region)>0 ) / count(*)) AS OkRegion,
  ROUND(100*COUNTIF( LENGTH(client.Geo.Subdivision1ISOCode)>0 ) / count(*)) AS OkSub1Code,
  ROUND(100*COUNTIF( LENGTH(client.Geo.Subdivision1Name)>0 ) / count(*)) AS OkSub1Name,
  ROUND(100*COUNTIF( LENGTH(client.Geo.Subdivision2ISOCode)>0 ) / count(*)) AS OkSub2Code,
  ROUND(100*COUNTIF( LENGTH(client.Geo.Subdivision2Name)>0 ) / count(*)) AS OkSub2Name,
  ROUND(100*COUNTIF( client.Geo.metro_code>0 ) / count(*)) AS OkMetroCode,
  ROUND(100*COUNTIF( LENGTH(client.Geo.city)>0 ) / count(*)) AS OkCity,
  ROUND(100*COUNTIF( client.Geo.area_code>0 ) / count(*)) AS OkAreaCode,
  ROUND(100*COUNTIF( LENGTH(client.Geo.postal_code)>0 ) / count(*)) AS OkPostalCode,
  ROUND(100*COUNTIF( client.Geo.radius > 0.0 ) / count(*)) AS OkRadius,
  ROUND(100*COUNTIF( client.Geo.missing = True ) / count(*)) AS OkMissing,
  ROUND(100*COUNTIF( client.Geo.missing = False ) / count(*)) AS OkNotMissing,
  ROUND(100*COUNTIF( client.Network.ASnumber IS NOT NULL ) / count(*)) AS OkASN,
  ROUND(100*COUNTIF( LENGTH(client.Network.ASName)>0 ) / count(*)) AS OkASName,
  -- ROUND(100*COUNTIF( TRUE ) / count(*)) AS OkNews,
FROM `{{Datasource}}_{{AdditionalArg}}`
WHERE date > '2008-01-01'
      AND filter.IsValidBest

GROUP BY dates, Source
ORDER BY dates, Source
