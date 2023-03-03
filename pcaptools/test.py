#!/usr/bin/python3
import pandas as pd
# import pandas-gbq

def main():
   print ("Hello World")
   query = '''
# Get inflight from snaps

WITH

getInflight AS (
    SELECT UUID, TestTime,
    ARRAY ( SELECT AS STRUCT
                Timestamp,
                TCPinfo.BytesSent - TCPInfo.BytesAcked - TCPInfo.SndMSS*(TCPinfo.TotalRetrans + TCPInfo.Sacked) AS InFlight,
                # TCPInfo
            FROM unnest(Snapshots)
    ) AS xSnapshots
    FROM `measurement-lab.ndt.tcpinfo`
    WHERE
        ARRAY_LENGTH(Snapshots) > 50
        AND partition_date ='2021-02-02'
        AND server.IATA = 'lga'
#       AND server.Machine = 'mlab2'
        AND UUID between 'ndt-nnwk2_1611335823_00000000000C2D9F' AND 'ndt-nnwk2_1611335823_00000000000C2EFC'
#        AND ( UUID ='ndt-nnwk2_1611335823_00000000000C2DF3' OR UUID ='ndt-nnwk2_1611335823_00000000000C2DAE' )
)

SELECT * FROM getInFlight
'''
   df = pd.read_gbq(query, project_id='measurement-lab')

   df.describe(datetime_is_numeric=True)

main()
