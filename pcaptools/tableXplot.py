#!/bin/ev python3


defaultCache = "~/gcsCache"
defaultTarget = "~/pcapCache"
defaultDataset = "mlab-collaboraations.mm-preproduction"

def getTable(table, dataset=defaultDataset):
  '''Get list of TCP traces to display
  Must include columns ArchiveURL and Filename
  The UUIDs are inferred from the Filenames
  '''

def getGCS(objects, cache=defaultCache):
  '''Efficiently fetch a list of GCS objects into a local cache
  - Don't fetch objects that are already present
  - Use multi threaded copy
  objects - List of full paths to objects in GCS
  cache - local cache directory
  '''

def extractPcaps(filelist, object, cache=defaultCache, target=defaultTarget):
  '''Extract listed files from the GCS object into individual directorys
     <target>/<UUID>/<filename>
     Returns a list of UUID-filename pairs
     Uses <target>/tmp as a staging area
  '''

def getPcap(row, cache=defaultCache, target=defaultTarget):
