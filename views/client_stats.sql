# Create a view that tracks the count of tests by hour and day of the week.
# Uses server latitude to adjust the time of day and day of week.
# TODO add stats for intertest interval

# bq query --use_legacy_sql=false < views/client_stats.sql

CREATE OR REPLACE VIEW
`mlab-sandbox.gfr.client_stats`
--PARTITION BY metro
OPTIONS(description = 'per metro client test stats by day of week and hour of the day')
  --      enable_refresh = true)
AS 

# Select ndt7 downloads (for now)
# Since this is client characterization, we count uploads and downloads, and don''t
# care whether the tests are completely valid

WITH tests AS (
  SELECT
  date, ID, raw.ClientIP, a,
  IFNULL(raw.Download, raw.Upload).ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.WScale & 0x0F AS WScale1,
  IFNULL(raw.Download, raw.Upload).ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.WScale >> 4 AS WScale2,
  IFNULL(raw.Download.StartTime, raw.Upload.StartTime) AS startTime,
  server.Geo.Longitude,  # TODO should this be client or server?
  LEFT(server.Site, 3) AS metro, server.Site AS site, server.Machine AS machine,
  REGEXP_EXTRACT(ID, "(ndt-?.*)-.*") AS NDTVersion,
  IF(raw.Download IS NULL, false, true) AS isDownload,
  IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata) AS tmpClientMetaData,
  FROM `measurement-lab.ndt.ndt7`
),

# This join is quite expensive - about 3 slot hours for 2 months of data, even if the clientName field is never used.
add_client_name AS (
  SELECT tests.*, clientName
  FROM tests LEFT JOIN (
    SELECT * EXCEPT(tmpClientMetadata, Name, Value), Value AS clientName
    FROM tests, tests.tmpClientMetadata
    WHERE Name = "client_name") USING (date, ID)
),

add_client_os AS (
  SELECT add_client_name.*, clientOS
  FROM add_client_name LEFT JOIN (
    SELECT * EXCEPT(tmpClientMetadata, Name, Value), Value AS clientOS
    FROM add_client_name, add_client_name.tmpClientMetadata
    WHERE Name = "client_os") USING (date, ID)
),

solar AS (
  SELECT *,
    TIMESTAMP_ADD(startTime, INTERVAL CAST(-60*Longitude/15 AS INT) MINUTE) AS solarTime,
  FROM add_client_os
),

day_hour AS (
  SELECT
    # TODO correct for latitude. 
   metro, ClientIP, clientName, clientOS, wscale1, wscale2,
   EXP(AVG(IF(isDownload,SAFE.LN(a.MeanThroughputMBPS),NULL))) AS meanSpeed,
   EXP(AVG(IF(isDownload,SAFE.LN(a.MinRTT),NULL))) AS meanMinRTT,
   EXTRACT(DAYOFWEEK FROM solarTime) AS day, EXTRACT(HOUR FROM solarTime) AS hour,
   COUNTIF(isDownload) AS downloads,
   COUNT(*) AS tests
   FROM solar
   WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 93 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
   GROUP BY metro, clientIP, clientName, clientOS, day, hour, wscale1, wscale2
)


SELECT 
    metro, ClientIP, clientName, clientOS, wscale1, wscale2,
    SUM(downloads) AS downloads,
    EXP(SUM(downloads*SAFE.LN(meanSpeed))/SUM(downloads)) AS meanSpeed,
    EXP(SUM(downloads*SAFE.LN(meanMinRTT))/SUM(downloads)) AS meanMinRTT,
    SUM(tests) AS tests,
    COUNT(DISTINCT day) AS days,
    COUNT(DISTINCT hour) AS hours,
    SUM(IF(day = 1,tests,0)) AS sunday,
    SUM(IF(day = 2,tests,0)) AS monday,
    SUM(IF(day = 3,tests,0)) AS tuesday,
    SUM(IF(day = 4,tests,0)) AS wednesday,
    SUM(IF(day = 5,tests,0)) AS thursday,
    SUM(IF(day = 6,tests,0)) AS friday,
    SUM(IF(day = 7,tests,0)) AS saturday,
    SUM(IF(hour BETWEEN 0 AND 2,tests,0)) AS t00,
    SUM(IF(hour BETWEEN 3 AND 5,tests,0)) AS t03,
    SUM(IF(hour BETWEEN 6 AND 8,tests,0)) AS t06,
    SUM(IF(hour BETWEEN 9 AND 11,tests,0)) AS t09,
    SUM(IF(hour BETWEEN 12 AND 14,tests,0)) AS t12,
    SUM(IF(hour BETWEEN 15 AND 17,tests,0)) AS t15,
    SUM(IF(hour BETWEEN 18 AND 20,tests,0)) AS t18,
    SUM(IF(hour BETWEEN 21 AND 23,tests,0)) AS t21,
FROM day_hour
GROUP BY metro, ClientIP, clientName, clientOS, wscale1, wscale2
HAVING tests > 5
