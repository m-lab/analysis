SELECT 
    -- day_timestamp is the default timestamp column
    INTEGER(web100_log_entry.log_time/86400)*86400  AS day_timestamp,

    -- test_count provides a count that can be plotted below the main graph
    COUNT(web100_log_entry.log_time)                AS test_count,
    
    -- some values that will produce graphs as lines 
    AVG(web100_log_entry.snap.MinRTT)                   AS avg,
    NTH(50,QUANTILES(web100_log_entry.snap.MinRTT,101)) AS median,
    NTH(10,QUANTILES(web100_log_entry.snap.MinRTT,101)) AS quantile_10,
    NTH(90,QUANTILES(web100_log_entry.snap.MinRTT,101)) AS quantile_90
FROM 
    -- The data table to query
    [m_lab.2013_04],[m_lab.2013_05] 
WHERE
        IS_EXPLICITLY_DEFINED(project)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time)
    AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
    AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.MinRTT)
    AND project = 0
    AND connection_spec.data_direction = 1
    AND web100_log_entry.is_last_entry = True
    AND web100_log_entry.connection_spec.local_ip = '64.9.225.167'
    AND web100_log_entry.snap.MinRTT < 1e7
GROUP BY    
    day_timestamp
ORDER BY 
    day_timestamp;
