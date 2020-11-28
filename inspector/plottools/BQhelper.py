""" BigQuery helper functions
Ported from 2018 Beacons study Jupyter Notebook
"""

import time
import collections
import pandas as pd
from google.cloud import bigquery

global project, dataset
project='mlab-sandbox'
dataset='mattmathis'

DefaultArguments = {
    'verbose_errors':True,
    'maxdepth':4,
    'project':project,
    'dataset':dataset
}

def expand_query(query,
              DefaultArgs = {},
              **kwargs):
    # Merge and expand arguments
    args = DefaultArguments.copy()
    args.update(DefaultArgs)
    args.update(kwargs)
    for i in range(args['maxdepth']):
      query=query.format(**args)
    if '{' in query:
      raise ValueError('Unexpanded substitutions: Set maxdepth larger than %d'%args['maxdepth'])

    # Leave global crumbs incase we need a postmortem
    global DebugQuery, NumberedQuery
    DebugQuery = query
    NumberedQuery = ""
    for i, l in enumerate(query.split('\n'), 1):
          NumberedQuery += "%3d %s\n"%(i, l)
    return query, args

def run_query(query,
              **kwargs):
    """ run_query
        Accepts nested {parameter} substitutions.
    """

    # do the work
    query, args = expand_query(query, kwargs)
    client = bigquery.Client(project=args['project'])
    job = client.query(query)  # All errors are delayed

    # Marshal the results, catching async errors
    try:
        results = collections.defaultdict(list)
        for row in job.result(timeout=600):
            for key in row.keys():
                results[key].append(row.get(key))
    except Exception as err:
        print(err)
        if 'verbose_errors' in args and args['verbose_errors']:
          global NumberedQuery
          print(NumberedQuery)
        raise
    global jobInfo
    jobInfo=job
    # Beware: defaultdict is not very useful directly
    return results

# Convert run_query() into some useful pandas DataFrames
def QueryTimestampTimeseries(query, column='TestTime', **args):
    results = run_query(query, **args)
    return pd.DataFrame(results,
            index=pd.DatetimeIndex(results[column]))
# These have not been tested after porting
#def QueryDateTimeseries(query, **args):
#    return QueryTimestampTimeseries(query, 'partition_date', **args)
#def QueryMonthTimeseries(query, **args):
#    return QueryTimestampTimeseries(query, 'partition_month', **args)
#def XQueryTestTimeserise(query, **args):
#    results = run_query(query, **args)
#    return pd.DataFrame(results,
#            index=pd.DatetimeIndex(results['test_time']*1000000000))
# Patch to run_query to work aroung test_time conversion problem
#def QueryTestTimeserise(query, **args):
#    results = run_query(query, **args)
#    ixx = results['test_time']
#    ix = [ixx[i]*1000000000 for i in range(len(ixx))]
#    ix = pd.DatetimeIndex(ix)
#    return pd.DataFrame(results,
#            index=ix)

def DataFrameQuery(query, **args):
    results = run_query(query, **args)
    return pd.DataFrame(results)

def IndexedDataFrameQuery(query, index, **args):
    results = run_query(query, **args)
    return pd.DataFrame(results, index=results[index])

def QueryDataFrame(q, i=None, **ignored):
    print ("Update QueryDataFrame() to IndexedDataFrameQuery() or DataFrameQuery()")

def UnitTestRunQuery():
    DefaultArgs = {}
    testQ="""
    SELECT
        count(*) AS CNT
    FROM
        `{dataset}.master_annotations`
    """
    # This reports 2108554 rows as of Nov 2018
    start = time.clock() # cheap %%time
    QueryDataFrameTest = DataFrameQuery(testQ)
    print(QueryDataFrameTest)
    print("Query Done", int(time.clock()-start), 'Seconds', time.asctime())

def write_query_table(otable,  # Beware, reversed from the Jupyter Notebook
                      query,
                      **kwargs):
    """ write_query_table
        Write otable from query.
        Accepts nested {parameter} substitutions.
    """
    global NumberedQuery
    query, args = expand_query(query, **kwargs)

    # do the work
    client = bigquery.Client(args['project'])
    table_ref = client.dataset(args['dataset']).table(otable)
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.query(query, location='US', job_config=job_config)

    # Marshal the results, catching async errors
    try:
        res = job.result()  # Get the first row to make sure it starts
        while not job.done():
            print ('tick')
            time.sleep(5)
        assert job.state == 'DONE'
    except Exception as err:
        print (err)
        if 'verbose_errors' in args and args['verbose_errors']:
            print (NumberedQuery)
        raise
    return

# test code
def UnitTestWriteQuery():
    testQ="""
    SELECT *
    FROM `{dataset}.master_annotations`
    LIMIT 1000
    """
    start = time.clock() # cheap %%time
    write_query_table('test_results', testQ)
    print("Write Done", int(time.clock()-start), 'Seconds', time.asctime())

###################################################################
# Misc helper functions
# These are genericly useful

# This is misnamed: it should be "getViews". getTables is different.
def getTables(dataSet):
  """Get all of the tables and views in a BQ dataset
  """
  Q='SELECT table_name FROM `{dataSet}.INFORMATION_SCHEMA.VIEWS`'
  r=sorted(run_query(Q, dataSet=dataSet)['table_name'])
  return r

def UnitTestGetTables():
  ds='measurement-lab.ndt'
  print ('Production tables & viwes in',ds)
  ret= getTables(ds)
  print (ret)
########

columnQuery="""
SELECT field_path FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE table_name = '{table}'
"""
def getColumns(table):
  """Get all column names in a table or view
  """
  project, dataset, table = table.split('.')
  r=run_query(columnQuery, project=project, dataset=dataset, table=table)['field_path']
  return r

def UnitTestGetColumns():
  t='measurement-lab.ndt.unified_uploads'
  print ('Columns in',t)
  ret = getColumns(t)
  print (ret)

###########
def UnitTestAll():
  UnitTestRunQuery()
  UnitTestWriteQuery()
  UnitTestGetTables()
  UnitTestGetColumns()
