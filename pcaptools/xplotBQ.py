#!/bin/env python3

# Drive xplot from BQ

# Read UUID and archive coordinaes from BQ and plot the traces

# Future: facilities for labelng traces

# Normal setup
import time
import os
from pathlib import Path
from pathlib import PurePath
from google import auth
from google.cloud import bigquery

import pandas as pd

import plotly.express as px
import plotly.graph_objects as go

import matplotlib.pyplot as plt

print (dir(auth))
print (auth.__package__)

# Application setup
defaultCache = "gcsCache"
defaultTarget = "pcapCache"
defaultDataset = "mlab-collaboraations.mm-preproduction"
# gsutil = "/tools/google-cloud-sdk/bin/gsutil"
gsutil = "/home/mattmathis/google-cloud-sdk/bin/gsutil"

# Google cloud setup
def getAuth():
  auth.authenticate_user()
  # Do BIGquery
  # client = bigquery.Client()

# get a table (or view) from BQ
# TODO: richer queries and alternate sources
def getTable(table, dataset=defaultDataset):
  '''Get list of TCP traces to display
    Must include columns id, ArchiveURL and Filename
  '''
  q = F"SELECT * FROM `{table}`"
  r = pd.read_gbq(q, project_id="measurement-lab")
  return (r)

# Fetch GCS objects into a local cache
# TODO: smarter about files already present
def getGCS(objects, cache=defaultCache):
  '''Efficiently fetch a list of GCS objects into a local cache
  - Use multi threaded copy
  objects - List of full paths to objects in GCS
  cache - local cache directory
  '''
  os.system( f'mkdir -p {cache}' )
  os.system( f'{gsutil} -m cp {list(objects)} {cache}/')
  # os.system( f'ls -l {defaultCache}')

# fetch/unpack a pcap file
def getPcap(row, cache=defaultCache, target=defaultTarget):
  '''Do all of the work needed to get a pcap file
  row must include .id, .ArchiveURL and .Filename
  '''
  gf = Path(cache) / PurePath(r.ArchiveURL).name
  d = Path(target) / row.id 
  fp = d / PurePath(row.Filename).name
  
  if fp.exists():
    print (f'Have {fp}')
    return
  if not gf.exists():
    print (f'Fetching {r.ArchiveURL}')
    getGCS(r.ArchiveURL) # no prefetch or threading
  print (f'Getting {fp} from {gf}')
  os.system(f'mkdir -p {d}')
  os.system(f'/bin/bash -c tar -xzf {gf} --to-stdout --warning=no-unknown-keyword {r.Filename} > {fp}')

# test main

def testMain():

  # Authenticat
  getAuth()

  # Get a table
  try: pcap_examples
  except UnboundLocalError:
    pcap_examples = getTable("mlab-collaboration.mm_preproduction.20230616 Detect WiFi testing in diagnostic TCPinfo")
  print (list(pcap_examples))
  print (pcap_examples.clientIP.count(), "rows")
  print (len(pcap_examples.ArchiveURL.unique()), "Unique ArchiveURLs")

  # prefetcjGCS files (mutithread)
  getGCS(pcap_examples.ArchiveURL[:3])

  # Extract them
  for i, r in pcap_examples.T.items():  
    getPcap(r)
    if i > 2:
      break

if __name__ == "__main__":
  testMain()
