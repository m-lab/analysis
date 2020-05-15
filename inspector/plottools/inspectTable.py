#!/usr/bin/env python3

# Minimal install
# apt-get install python3-regex
# apt-get install python3-pandas

import sys
import re
import BQhelper as bq
import pandas as pd

# Infer subexpressions to access indicator columns
# Column Matcher
Patterns={
    'TestDate':[
        ('[tT]est.*[dD]ate', '{c}'),
        ('[pP]artition.*[dD]ate','{c}'),
    ],
    'fileName':[
        ('[fF]ile[nN]ame', '{c}'),
        ('.','"No Server Name"'),   # default to an unreasonable name
    ],
    'parseTime':[
        ('[pP]arse.*[tT]ime', '{c}'),
        ('.', 'Null')
    ],
    'UUID':[
        'a.UUID',
        'result.Control.UUID',
        ('UUID', '{c}'),
        ('.', '"ERROR_DISCOVERING_UUID"')  # default to an errored UUID
    ]
}

def columnMatcher(patterns, cols, verbose=False):
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
        return {var:None}

    res={}
    for var, matches in patterns.items():
        res.update({var:None})
        if verbose:
            print ('VAR:', var, matches)
        res.update(matchHelper(var, matches, cols, verbose))

    return res

def UnitTestColumnMatcher():
    tests=['test__date', 'TestDate', 'PartitionDate', 'ParseInfo.TaskFileName' ]
    for t in tests:
        print ("   Test", t)
        print ('===  Result:', t, columnMatcher(Patterns, [t], verbose=False))


def inferColumns(table):
    return columnMatcher(Patterns, bq.getColumns(table))


mainQ="""
WITH

canonic AS (
  SELECT
    {TestDate} AS TestDate,
    REGEXP_EXTRACT({fileName}, 'mlab[1-4]-[a-z][a-z][a-z][0-9][0-9t]') AS ShortName,
    {parseTime} AS _ParseTime,
    {UUID} AS UUID,
  FROM `{fullTableName}`
),

HotUUIDs AS (
  SELECT UUID, count(*) AS cnt
  FROM canonic
  WHERE UUID NOT IN ( 'ERROR_DISCOVERING_UUID' )
  GROUP BY UUID
  ORDER BY cnt desc
  limit 1
),

ServerSet AS (
  SELECT ShortName AS _UniqueName FROM canonic GROUP BY ShortName
),

ServerDays AS (
  SELECT
    ShortName,
    DATE_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 AS cnt,
    DATE_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 - COUNT( DISTINCT TestDate) AS missing,
    MAX(TestDate) AS EndDate,
  FROM canonic
  GROUP BY ShortName
),

RawReport AS (
  SELECT 10 AS seq, "Total Days" AS name, DATE_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 AS val FROM canonic UNION ALL
  SELECT 11, "Missing Days", DATE_DIFF(MAX(TestDate), MIN(TestDate), DAY)+1 - COUNT( DISTINCT TestDate) FROM canonic UNION ALL

  SELECT 20, "Total Rows", COUNT (*) FROM canonic UNION ALL
  SELECT 21, "Missing UUIDs (ERROR_DISCOVERING_UUID)", COUNTIF (UUID is Null OR UUID = 'ERROR_DISCOVERING_UUID') FROM canonic UNION ALL
  SELECT 22, "Duplicated UUIDs", COUNTIF(UUID IS NOT NULL AND UUID != 'ERROR_DISCOVERING_UUID') - COUNT( DISTINCT UUID ) FROM canonic UNION ALL
  SELECT 23, CONCAT("Top dup:", UUID), cnt FROM HotUUIDs UNION ALL

  SELECT 30, "Total Servers", COUNT ( DISTINCT ShortName ) FROM canonic UNION ALL
  SELECT 31, "Rows Missing Servers", COUNTIF ( ShortName IS Null ) FROM canonic UNION ALL
  SELECT 32, "Test Servers (0t, 1t)", COUNTIF ( _UniqueName like '%t' ) FROM ServerSet UNION ALL
  SELECT 33, "Mlab4's", COUNTIF ( _UniqueName like 'mlab4%'  ) FROM ServerSet UNION ALL

  SELECT 40, "Currently Active Servers", COUNTIF ( DATE_DIFF(CURRENT_DATE(), EndDate, DAY) < 4) FROM ServerDays UNION ALL
  SELECT 41, "Total Server-days", SUM(cnt) FROM ServerDays UNION ALL
  SELECT 42, "Missing Server-days", SUM(missing) FROM ServerDays UNION ALL

  SELECT 99, "End-of-Report", 0.0
)

select seq, name, safe_cast(val AS string) AS val FROM RawReport ORDER BY seq
# select * FROM RawReport ORDER BY seq
"""

def inventoryTable(fullTableName):
#    project, dataset, table = fullTableName.split('.')

    print ('Inferred Column Mappings')
    expansion=inferColumns(fullTableName)
    for v, e in expansion.items():
        print ("%40s AS %s"%(e, v))
    print ('Data Statistics')
    res=bq.IndexedDataFrameQuery(mainQ, fullTableName=fullTableName, index='seq', **expansion)
    for i in res['seq']:
        print ("%50s %s"%(res['name'][i], res['val'][i]))

    print('Resource consumption')
    totalRows=int(res.loc[20]['val'])  # seq of "Total Rows"
    fmt="%20s: %s"
    print (fmt%('Rows (M)', totalRows/1000000))
    print (fmt%('slot_milli', bq.jobInfo.slot_millis))
    print (fmt%('slot_milli/row', bq.jobInfo.slot_millis/totalRows))
    print (fmt%('bytes processed', bq.jobInfo.total_bytes_processed))
    print (fmt%('bytes processed/row', bq.jobInfo.total_bytes_processed/totalRows))
    return

def UnitTestInventoryTable():
    inventoryTable('measurement-lab.ndt.ndt5')

def inventoryDataSet(dataSet):
    tables=bq.getTables(dataSet)
    for t in tables:
        table = dataSet+'.'+t
        print('')
        print('==================================================')
        print ('Table Statistics for', table)
        try:
            inventoryTable(table)
        except Exception as e:
            print ("   Crashed: ", type(e))

def inspectDataSetMappings(dataSet):
    tables=bq.getTables(dataSet)
    for t in tables:
        table = dataSet+'.'+t
        print('')
        print ('Table column mappings for', table)
        expansion=inferColumns(table)
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
