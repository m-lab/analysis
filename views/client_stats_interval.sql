# Create a view that tracks the count of tests by hour and day of the week.
# Uses server latitude to adjust the time of day and day of week.
# Client id is based on IP address, clientName, clientOS, and wscale.
# TODO add stats for intertest interval

# NOTES:
# Anything that takes less than a couple slot hours we can probably just
# do in gardener after processing incoming data each day.

# bq query --use_legacy_sql=false < views/client_stats_interval.sql

CREATE OR REPLACE VIEW
`mlab-sandbox.gfr.client_stats_2`
OPTIONS(description = 'per metro client test stats - tests by day of week and hour of the day')
AS 

# Select ALL ndt7 tests
# Since this is client characterization, we count uploads and downloads, and don''t
# care whether the tests are completely valid
WITH tests AS (
  SELECT
  date, ID, raw.ClientIP, a,
  IFNULL(raw.Download, raw.Upload).ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.WScale & 0x0F AS WScale1,
  IFNULL(raw.Download, raw.Upload).ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.WScale >> 4 AS WScale2,
  a.TestTime,
  server.Geo.Longitude,  # TODO should this be client or server?
  LEFT(server.Site, 3) AS metro,
  IF(raw.Download IS NULL, false, true) AS isDownload,
  # This is used later for extracting the client metadata.
  IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata) AS tmpClientMetaData,
  FROM `measurement-lab.ndt.ndt7`
),

# These metadata joins are quite expensive - about 3 slot hours for 2 months of data, even if the field is never used.
add_client_name AS (
  SELECT tests.*, clientName
  FROM tests LEFT JOIN (
    SELECT * EXCEPT(tmpClientMetadata, Name, Value), Value AS clientName
    FROM tests, tests.tmpClientMetadata
    WHERE Name = "client_name") USING (date, ID)
),

add_client_os AS (
  SELECT add_client_name.* EXCEPT(tmpClientMetaData), clientOS
  FROM add_client_name LEFT JOIN (
    SELECT * EXCEPT(tmpClientMetadata, Name, Value), Value AS clientOS
    FROM add_client_name, add_client_name.tmpClientMetadata
    WHERE Name = "client_os") USING (date, ID)
),

# This adds the solar time, which is more useful for global clustering than UTC time.
solar AS (
  SELECT * EXCEPT(Longitude),
    TIMESTAMP_ADD(testTime, INTERVAL CAST(-60*Longitude/15 AS INT) MINUTE) AS solarTime,
    # Compute the time, in seconds, since the previous test of the same type (upload or download)
    TIMESTAMP_DIFF(testTime, LAG(testTime, 1) OVER sequence, SECOND)AS testInterval,
  FROM add_client_os
  WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 93 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  WINDOW
    sequence AS (PARTITION BY isDownload, metro, ClientIP, clientName, clientOS, wscale1, wscale2
    ORDER BY a.TestTime)
),

# This adds the inter-test interval mean and stdev, for downloads only, to ALL tests
some_client_stats AS (
  SELECT * EXCEPT(a), a.MeanThroughputMBPS, a.MinRTT,
    STRUCT( 
      COUNT(*) AS tests,
      COUNTIF(isDownload) AS downloads,
      COUNTIF(NOT isDownload) AS uploads,
      EXP(AVG(IF(isDownload,SAFE.LN(a.MeanThroughputMBPS),NULL)) OVER client_win) AS meanSpeed,  # Downloads only.
      EXP(AVG(IF(isDownload,SAFE.LN(a.MinRTT),NULL)) OVER client_win) AS meanMinRTT,             # Downloads only.
      AVG(IF(isDownload,testInterval,NULL)) OVER client_win  AS downloadInterval,
      STDDEV(IF(isDownload,testInterval,NULL)) OVER client_win AS downloadIntervalVariability
    ) AS client_stats,
  FROM solar
  GROUP BY date, TestTime, solarTime, testInterval, ID, isDownload, metro, clientIP, clientName, clientOS, wscale1, wscale2,
    MeanThroughputMBPS, MinRTT
  WINDOW
    client_win AS (PARTITION BY metro, ClientIP, clientName, clientOS, wscale1, wscale2)
),

# This is intended to identify each test by the hour of the day, and the day of the week.
# It is currently grouping by both, whereas it really should group by each independently.
# This is ok, as later we sum by day, and sum by 3 hour interval, but this means that there
# are 24 * 7 groupings here, instead of fewer.
day_hour_counts AS (
  SELECT
   metro, ClientIP, clientName, clientOS, wscale1, wscale2,
   COUNT(*) AS tests,
   ANY_VALUE(client_stats) AS client_stats,
   EXTRACT(DAYOFWEEK FROM solarTime) AS day, EXTRACT(HOUR FROM solarTime) AS hour,
   FROM some_client_stats
   GROUP BY metro, clientIP, clientName, clientOS, day, hour, wscale1, wscale2
)

SELECT 
    metro, ClientIP, clientName, clientOS, wscale1, wscale2,
    ANY_VALUE(client_stats) AS client_stats,
    STRUCT(
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
      SUM(IF(hour BETWEEN 21 AND 23,tests,0)) AS t21
    ) AS timing_stats
FROM day_hour_counts
GROUP BY metro, ClientIP, clientName, clientOS, wscale1, wscale2
HAVING client_stats.tests > 5
