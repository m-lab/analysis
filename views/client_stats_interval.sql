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

# This adds all of the client aggregations.
# This adds the inter-test interval mean and stdev, for downloads only, to ALL tests
some_client_stats AS (
  SELECT * EXCEPT(a), a.MeanThroughputMBPS, a.MinRTT,
  EXTRACT(DAYOFWEEK FROM solarTime) AS day, EXTRACT(HOUR FROM solarTime) AS hour,
    STRUCT( 
      EXP(AVG(IF(isDownload,SAFE.LN(a.MeanThroughputMBPS),NULL)) OVER client_win) AS meanSpeed,  # Downloads only.
      EXP(AVG(IF(isDownload,SAFE.LN(a.MinRTT),NULL)) OVER client_win) AS meanMinRTT             # Downloads only.
    ) AS performance_stats,
    STRUCT (
      COUNT(*) AS tests,
      COUNTIF(isDownload) AS downloads,
      COUNTIF(NOT isDownload) AS uploads,
      (COUNTIF(isDownload) OVER client_win  - COUNTIF(NOT isDownload) OVER client_win)/COUNT(*) OVER client_win AS duBalance,
      AVG(IF(isDownload,testInterval,NULL)) OVER client_win  AS downloadInterval,
      STDDEV(IF(isDownload,testInterval,NULL)) OVER client_win AS downloadIntervalVariability,

      COUNT(DISTINCT EXTRACT(DAYOFWEEK FROM solarTime)) AS days,
      COUNT(DISTINCT EXTRACT(HOUR FROM solarTime)) AS hours,

      COUNTIF(EXTRACT(DAYOFWEEK FROM solarTime) = 1) OVER client_win AS sunday,
      COUNTIF(EXTRACT(DAYOFWEEK FROM solarTime) = 2) OVER client_win AS monday,
      COUNTIF(EXTRACT(DAYOFWEEK FROM solarTime) = 3) OVER client_win AS tuesday,
      COUNTIF(EXTRACT(DAYOFWEEK FROM solarTime) = 4) OVER client_win AS wednesday,
      COUNTIF(EXTRACT(DAYOFWEEK FROM solarTime) = 5) OVER client_win AS thursday,
      COUNTIF(EXTRACT(DAYOFWEEK FROM solarTime) = 6) OVER client_win AS friday,
      COUNTIF(EXTRACT(DAYOFWEEK FROM solarTime) = 7) OVER client_win AS saturday,
      
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 0 AND 2) OVER client_win AS t00,
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 3 AND 5) OVER client_win AS t03,
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 6 AND 8) OVER client_win AS t06,
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 9 AND 10) OVER client_win AS t09,
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 12 AND 14) OVER client_win AS t12,
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 15 AND 17) OVER client_win AS t15,
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 18 AND 20) OVER client_win AS t18,
      COUNTIF(EXTRACT(HOUR FROM solarTime) BETWEEN 21 AND 23) OVER client_win AS t21
    ) AS training_stats
  FROM solar
  GROUP BY date, TestTime, solarTime, testInterval, ID, isDownload, metro, clientIP, clientName, clientOS, wscale1, wscale2,
    MeanThroughputMBPS, MinRTT
  WINDOW
    client_win AS (PARTITION BY metro, ClientIP, clientName, clientOS, wscale1, wscale2)
)

SELECT 
    metro, ClientIP, clientName, clientOS, wscale1, wscale2,
    ANY_VALUE(performance_stats) AS performance_stats,
    ANY_VALUE(training_stats) AS training_stats,
FROM some_client_stats
GROUP BY metro, ClientIP, clientName, clientOS, wscale1, wscale2
HAVING training_stats.tests > 5
