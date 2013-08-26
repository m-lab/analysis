"""Visualize usage stats for each server on M-Lab.

Generates frames of a movie, each of which visualizes a single day of activity
on the M-Lab platform. Each row is a tool running on the platform, and each
column is one of the servers. The cell is coloured according to the relative
usage.
"""

__author__ = 'dominich@google.com (Dominic Hamon)'

import gflags
import math
import numpy
import os
import sys

from scipy.misc import pilutil

FLAGS = gflags.FLAGS

gflags.DEFINE_string('input', 'usage_stats_servers.txt', 'Input filename', short_name = 'i')
gflags.DEFINE_string('output', 'usage', 'Output prefix', short_name = 'o')
gflags.MarkFlagAsRequired('input')
gflags.MarkFlagAsRequired('output')


stats = {}
max_test_count = {}
num_tools = 0
num_servers = 0

tools = []
servers = []

def read_counts():
  global stats
  global max_test_count
  global num_tools
  global num_servers

  rows = [row.strip() for row in open(FLAGS.input)]
  rows.pop(0)

  for row in rows:
    # print 'row: %s' % row
    parts = row.split(',');
    date = parts[2]
    tool_name = parts[0]
    server_name = parts[1]
    test_count = float(parts[3])

    if not date in stats:
      stats[date] = {}
    if not tool_name in stats[date]:
      stats[date][tool_name] = {}
    stats[date][tool_name][server_name] = test_count

    if not tool_name in tools:
      # print 'Adding tool %s' % tool_name
      tools.append(tool_name)
    if not server_name in servers:
      # print 'Adding server %s' % server_name
      servers.append(server_name)

    if date in max_test_count and tool_name in max_test_count[date]:
      max_test_count[date][tool_name] = max(test_count, max_test_count[date][tool_name])
    else:
      if not date in max_test_count:
        max_test_count[date] = {}
      max_test_count[date][tool_name] = test_count

def main():
  global stats

  try:
    FLAGS(sys.argv)
  except gflags.FlagsError, err:
    print '%s\nUsage: %s ARGS\n%s' % (err, sys.argv[0], FLAGS)
    sys.exit(1)

  read_counts()
  print 'Completed reading input'

  for date in stats.keys():
    # note: number of rows is first.
    # Initialize with ones so we start all white
    array = numpy.ones((len(tools), len(servers), 3), dtype = numpy.float)

    y = 0
    x = 0
    for tool in tools:
      if tool in stats[date].keys() and max_test_count[date][tool] != 0:
        for server in servers:
          if server in stats[date][tool].keys():
            color = stats[date][tool][server] / max_test_count[date][tool]
            array[y, x] = [1.0, 1.0 - color, 1.0 - color]
          x += 1
      x = 0
      y += 1
    if not os.path.exists('out'):
      os.makedirs('out')
    output = 'out/' + FLAGS.output + '.' + date + '.bmp'
    sys.stdout.write('Saving usage to %s\r' % output)
    sys.stdout.flush()
    resized_array = pilutil.imresize(array, (10*len(tools), 10*len(servers)),
                                     'nearest')
    pilutil.imsave(output, resized_array)
  print '\nDone'

if __name__ == '__main__':
  main()
