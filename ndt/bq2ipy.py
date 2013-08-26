#!/usr/bin/python

import csv
import os
import tempfile
import time

def bigquery(query):
	tmpname="/tmp/tmpbq.%s" % time.time()
	# load query
	l = open("/tmp/log.txt", 'a')
	if query is None:
		os.system("""echo "count,ips\n80090,34745\n80091,34746" > %s """ % tmpname)
	else:
		cmd = "bq -q --format=csv query --max_rows=10000000 \"%s\" > %s" % (query, tmpname)
		print >>l, cmd
		l.write(cmd+"\n")
		l.flush()
		l.close()
		os.system(cmd)
	
	# load csv output
	rows = csv.reader(open(tmpname, 'r'), delimiter=",")
	header=rows.next()
	values = {}
	values.update(dict(zip(header,[ [] for i in range(0,len(header)) ])))
	# parse into dict
	for line in rows:
		for i,h in enumerate(header):
			try:
				values[h].append(float(line[i]))
			except:
				try:
					values[h].append(line[i])
				except Exception, e:
					raise Exception(cmd + str(e))
	return values

if __name__ == "__main__":
	bigquery(None)
