SELECT 
    web100_log_entry.connection_spec.local_ip           AS server_ip,
    INTEGER(web100_log_entry.log_time/86400)*86400      AS day_timestamp,
    COUNT(web100_log_entry.log_time)                    AS test_count, 

    NTH(90,QUANTILES(WEB100VAR,101)) AS quantile_90,
    NTH(10,QUANTILES(WEB100VAR,101)) AS quantile_10,
    NTH(50,QUANTILES(WEB100VAR,101)) AS med_web100,
    -- uncomment STDDEV() if you would like to enable error bars
    -- STDDEV(WEB100VAR)                AS std_web100,
FROM 
    DATETABLE
WHERE
        IS_EXPLICITLY_DEFINED(project)
    AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip)
    AND IS_EXPLICITLY_DEFINED(WEB100VAR)
    AND project = 0
    AND connection_spec.data_direction = 1
    AND web100_log_entry.is_last_entry = True
    AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 
    AND web100_log_entry.snap.HCThruOctetsAcked < 1000000000
    AND web100_log_entry.snap.CongSignals > 0
    AND (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd) >= 9000000
    AND (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd) < 3600000000
    AND web100_log_entry.connection_spec.local_ip = 'ADDRESS'
GROUP BY    
    server_ip, day_timestamp
ORDER BY 
    server_ip, day_timestamp;

