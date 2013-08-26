SELECT 
    web100_log_entry.connection_spec.local_ip           AS server_ip,
    INTEGER(web100_log_entry.log_time/86400)*86400      AS day_timestamp,
    COUNT(web100_log_entry.log_time)                    AS test_count, 
    -- RATE
    NTH(90,QUANTILES(8*web100_log_entry.snap.HCThruOctetsAcked/(
                     web100_log_entry.snap.SndLimTimeRwin +
                     web100_log_entry.snap.SndLimTimeCwnd +
                     web100_log_entry.snap.SndLimTimeSnd),101)) as quantile_90,
    NTH(10,QUANTILES(8*web100_log_entry.snap.HCThruOctetsAcked/(
                     web100_log_entry.snap.SndLimTimeRwin +
                     web100_log_entry.snap.SndLimTimeCwnd +
                     web100_log_entry.snap.SndLimTimeSnd),101)) as quantile_10,
    NTH(50,QUANTILES(8*web100_log_entry.snap.HCThruOctetsAcked/(
                     web100_log_entry.snap.SndLimTimeRwin +
                     web100_log_entry.snap.SndLimTimeCwnd +
                     web100_log_entry.snap.SndLimTimeSnd),101)) as med_rate,
    STDDEV(8*web100_log_entry.snap.HCThruOctetsAcked/(
                     web100_log_entry.snap.SndLimTimeRwin +
                     web100_log_entry.snap.SndLimTimeCwnd +
                     web100_log_entry.snap.SndLimTimeSnd)) as std_rate,
FROM 
    DATETABLE
WHERE
    --  IP for ndt.iupui.mlab3.sea01
        IS_EXPLICITLY_DEFINED(project)
    AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip)
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
    AND web100_log_entry.snap.MinRTT < 1e7
GROUP BY    
    server_ip, day_timestamp
ORDER BY 
    server_ip, day_timestamp;

