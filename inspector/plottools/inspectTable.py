#!/usr/bin/env python3

# Minimal install
# apt-get install python3-regex
# apt-get install python3-pandas

import sys
import re
from optparse import OptionParser
import BQhelper as bq
import pandas as pd

# Infer subexpressions to access indicator columns
# Column Matcher
Patterns={
    'TestDate':[  # NB: Row dates are converted to TIMESTAMP to facilitate comparsions, etc
        ('[dD]ate', 'TIMESTAMP({c})'),
        ('[tT]est.*[dD]ate', 'TIMESTAMP({c})'),
        ('[pP]artition.*[dD]ate','TIMESTAMP({c})'),
        ('log_time', 'TIMESTAMP({c})')
    ],
    'fileName':[
        ('Machine', 'CONCAT(server.Machine,"-",server.Site)'),
        ('[fF]ile[nN]ame', '{c}'),
        ('.','"No Server Name"'),   # default to an unreasonable name
    ],
    'parseTime':[
        ('[pP]arse.*[tT]ime', '{c}'),
        ('.', "TIMESTAMP('1970-01-01')")   # default to an unreasonable time
    ],
    'UUID':[
        ('id', '{c}'),
        'a.UUID',
        'result.Control.UUID',
        ('UUID', '{c}'),
        'test_id',
        ('.', '"ERROR_DISCOVERING_UUID"')  # default to an errored UUID
    ]
}

def columnMatcher(patterns, cols, needed=None, verbose=False):
    """Infer BQ expressions to extract required columns
    """
    def matchHelper(var, matches, cols, verbose):
        for m in matches:
            try:
                r, v = m
            except ValueError:  # shortcut, single item match
                if verbose:
                    print('Simple:', m)
                if m in cols:
                    return {var:m}
                continue
            if verbose:
                print ("Re:", r, v)
            for c in cols:
                if re.search(r, c):
                    return {var:v.format(c=c)}
        print("Warning no mapping for", var)
        return {var:None}

    res={}
    for var, matches in patterns.items():
      if needed and var not in needed:
        continue
      if verbose:
        print ('VAR:', var, matches)
      res.update(matchHelper(var, matches, cols, verbose))

    return res

def UnitTestColumnMatcher():
    tests=['test__date', 'TestDate', 'PartitionDate', 'ParseInfo.TaskFileName' ]
    for t in tests:
        print ("   Test", t)
        print ('===  Result:', t, columnMatcher(Patterns, [t]))

def inferColumns(table, needed=None):
    return columnMatcher(Patterns, bq.getColumns(table), needed)

mainQ="""
WITH

canonic AS (
  SELECT
    {TestDate} AS TestDate,
    REGEXP_EXTRACT({fileName}, 'mlab[1-4]-[a-z][a-z][a-z][0-9][0-9t]') AS ShortName,
    {parseTime} AS ParseTime,
    {UUID} AS UUID,
  FROM `{fullTableName}`
),

HotUUIDs AS (
  SELECT UUID, count(*) AS cnt
  FROM canonic
  WHERE UUID NOT IN ( 'ERROR_DISCOVERING_UUID', '' )
  GROUP BY UUID
  ORDER BY cnt desc
  limit 1
),

ServerSet AS (
  SELECT ShortName AS UniqueName FROM canonic GROUP BY ShortName
),

ServerDays AS (
  SELECT
    ShortName,
    TIMESTAMP_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 AS cnt,
    TIMESTAMP_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 - COUNT( DISTINCT TestDate) AS missing,
    MAX(TestDate) AS EndDate,
  FROM canonic
  GROUP BY ShortName
),

# Since some messages need to be strings, we force all to be strings
RawReport AS (
  SELECT 10 AS seq, "Total Days" AS name, CAST(TIMESTAMP_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 AS STRING) AS val FROM canonic UNION ALL
  SELECT 12, "First Day", CAST(MIN(TestDate) AS STRING) FROM canonic UNION ALL
  SELECT 13, "Last Day", CAST(MAX(TestDate) AS STRING) FROM canonic UNION ALL

  SELECT 20, "Total Rows", CAST(COUNT (*) AS STRING) FROM canonic UNION ALL

  SELECT 30, "Total Servers", CAST(COUNT ( DISTINCT ShortName ) AS STRING) FROM canonic UNION ALL

  SELECT 50, "Oldest Parse Time", CAST(MIN(ParseTime) AS STRING) FROM canonic UNION ALL
  SELECT 51, "Newest Parse Time", CAST(MAX(parseTime) AS STRING) FROM canonic UNION ALL
  {expandedReport}
  SELECT 99, "End-of-Report", ""
)

select * FROM RawReport ORDER BY seq

"""

defaultQ="""
  SELECT 11, "Missing Days", CAST(TIMESTAMP_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 - COUNT( DISTINCT TestDate) AS STRING) FROM canonic UNION ALL


  SELECT 21, "Missing UUIDs (ERROR_DISCOVERING_UUID)", CAST(COUNTIF (UUID is Null OR UUID = 'ERROR_DISCOVERING_UUID') AS STRING) FROM canonic UNION ALL
  SELECT 22, "Duplicated UUIDs",CAST( COUNTIF(UUID IS NOT NULL AND UUID != 'ERROR_DISCOVERING_UUID') - COUNT( DISTINCT UUID ) AS STRING) FROM canonic UNION ALL
  SELECT 24, "Total unique UUIDs", CAST(COUNT( DISTINCT UUID ) AS STRING) FROM canonic UNION ALL

  SELECT 31, "Rows Missing Servers", CAST(COUNTIF ( ShortName IS Null ) AS STRING) FROM canonic UNION ALL
  SELECT 32, "Test Servers (0t, 1t)", CAST(COUNTIF ( UniqueName like '%t' ) AS STRING) FROM ServerSet UNION ALL
  SELECT 33, "Mlab4's", CAST(COUNTIF ( UniqueName like 'mlab4%' ) AS STRING) FROM ServerSet UNION ALL

  SELECT 52, "Span of Parse dates", CAST(TIMESTAMP_DIFF(MAX(parseTime),  MIN(ParseTime), day) AS STRING) FROM canonic UNION ALL"""

extendedQ="""
  SELECT 23, CONCAT("Top dup:", UUID), CAST(cnt AS STRING) FROM HotUUIDs UNION ALL

  SELECT 40, "Currently Active Servers", CAST(COUNTIF ( TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), EndDate, DAY) < 4) AS STRING) FROM ServerDays UNION ALL
  SELECT 41, "Total Server-days", CAST(SUM(cnt) AS STRING) FROM ServerDays UNION ALL
  SELECT 42, "Missing Server-days", CAST(SUM(missing) AS STRING) FROM ServerDays UNION ALL
"""

def resourceReport(rows=0):
    print('Resource consumption')
    fmt="%20s: %s"
    print (fmt%('Rows (M)', rows/1000000))
    print (fmt%('slot_milli', bq.jobInfo.slot_millis))
    if rows>0 and bq.jobInfo.slot_millis>0:
      print (fmt%('slot_milli/row', bq.jobInfo.slot_millis/rows))
    print (fmt%('bytes processed', bq.jobInfo.total_bytes_processed))
    if rows>0 and bq.jobInfo.total_bytes_processed>0:
      print (fmt%('bytes processed/row', bq.jobInfo.total_bytes_processed/rows))

def inventoryTable(fullTableName, **args):
    print ('Inferred Column Mappings')
    expansion=inferColumns(fullTableName)
    for v, e in expansion.items():
        print ("%40s AS %s"%(e, v))
    if args.get('quick'):
      expandedReport = ''
    elif args.get('extended'):
      expandedReport = defaultQ + extendedQ
    else:
      expandedReport = defaultQ
    print ('Data Statistics')
    res=bq.IndexedDataFrameQuery(mainQ, fullTableName=fullTableName, expandedReport=expandedReport, index='seq', **expansion, **args)
    for i in res['seq']:
        print ("%50s %s"%(res['name'][i], res['val'][i]))

    totalRows=int(res.loc[20]['val'])  # seq of "Total Rows"
    resourceReport(totalRows)
    return

def UnitTestInventoryTable():
    inventoryTable('measurement-lab.ndt.ndt5')

def inventoryDataSet(dataSet, **args):
    tables=bq.getTables(dataSet)
    for t in tables:
        table = dataSet+'.'+t
        print('')
        print('==================================================')
        print ('Table Statistics for', table)
        try:
            inventoryTable(table, **args)
        except Exception as e:
            print ("   Crashed: ", type(e))

def inspectDataSetMappings(dataSet, needed=None):
    tables=bq.getTables(dataSet)
    for t in tables:
        table = dataSet+'.'+t
        print('')
        print ('Table column mappings for', table)
        expansion=inferColumns(table, needed)
        for v, e in expansion.items():
            print ("%40s AS %s"%(e, v))

def UnitTestAll():
  UnitTestColumnMatcher()
  UnitTestInventoryTable()

if __name__ == "__main__":
  try:
    arg = sys.argv[1]
  except IndexError:
    print ('Requires a table or dataset name')
    exit(-2)
  if len(arg.split('.')) == 3:
      inventoryTable(arg)
  elif len(arg.split('.')) == 2:
    inventoryDataSet(arg)
  else:
    print ("Must be either project.dataset or project.dataset.table")
