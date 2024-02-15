
WITH ndtLegacy AS (
  SELECT NET.SAFE_IP_FROM_STRING(raw.connection.client_ip) AS ip,
  a.MeanThroughputMbps AS mbps
  FROM `measurement-lab.ndt.web100`
  WHERE DATE BETWEEN ("{YEAR}-01-01") AND DATE("{YEAR}-12-31")
  AND a.MeanThroughputMbps IS NOT NULL
),
ndt5 AS (
  SELECT NET.SAFE_IP_FROM_STRING(raw.ClientIP) AS ip,
  a.MeanThroughputMbps AS mbps
  FROM `measurement-lab.ndt.ndt5`
  WHERE DATE BETWEEN ("{YEAR}-01-01") AND DATE("{YEAR}-12-31")
    AND a.MeanThroughputMbps IS NOT NULL
),
ndt7 AS (
  SELECT NET.SAFE_IP_FROM_STRING(raw.ClientIP) AS ip,
  a.MeanThroughputMbps AS mbps
  FROM `measurement-lab.ndt.ndt7`
  WHERE DATE BETWEEN ("{YEAR}-01-01") AND DATE("{YEAR}-12-31")
    AND a.MeanThroughputMbps IS NOT NULL
),
unifiedndt AS (
  SELECT * FROM ndt5
  UNION ALL
  SELECT * FROM ndtLegacy
  UNION ALL
  SELECT * FROM ndt7
),
ndtv4 AS (
SELECT NET.IP_TRUNC(ip, 24) AS netblock, ip, mbps
FROM unifiedndt
WHERE ip IS NOT NULL AND BYTE_LENGTH(ip) = 4
)
SELECT NET.IP_TO_STRING(netblock) as block, COUNT(*) as count, APPROX_QUANTILES(mbps, 101)[ORDINAL(50)] as median_mbps
FROM ndtv4
GROUP BY netblock
ORDER BY netblock
