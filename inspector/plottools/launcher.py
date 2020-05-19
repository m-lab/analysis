#!/usr/bin/env python3

# Launch python tools inside of continer pyrunner from within dockerPython.sh

import argparse
import inspectTable

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Insepct MLab views or tables.',
                                   epilog="Action must be one of: inventory dataset",
                                   allow_abbrev=True)
  parser.add_argument('action', metavar='action', type=str, help='Test to per performed')
  parser.add_argument('tables', metavar='names', type=str, nargs='+', help='Tables or other objects to be inspected')

  args = parser.parse_args()
  if args.action == 'inventory' :
    for t in args.tables:
      inspectTable.inventoryTable(t)
  elif args.action == 'dataset' :
    for t in args.tables:
      inspectTable.inventoryDataSet(t)
  else:
    parser.print_help()
