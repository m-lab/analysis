WITH ndtLegacy AS (
  SELECT NET.SAFE_IP_FROM_STRING(connection_spec.client_ip) AS ip,   8 * (web100_log_entry.snap.HCThruOctetsAcked / (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd)) AS mbps
  FROM `measurement-lab.ndt.downloads`
  WHERE partition_date BETWEEN DATE("{YEAR}-01-01") AND DATE("{YEAR}-12-31")
),
ndt5 AS (
  SELECT NET.SAFE_IP_FROM_STRING(result.S2C.ClientIP) AS ip, result.S2C.MeanThroughputMbps AS mbps
  FROM `measurement-lab.ndt.ndt5`
  WHERE partition_date BETWEEN DATE("{YEAR}-01-01") AND DATE("{YEAR}-12-31")
    AND result.S2C IS NOT NULL
),
unifiedndt AS (
  SELECT * FROM ndt5
  UNION ALL
  SELECT * FROM ndtLegacy
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
