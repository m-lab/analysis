#!/usr/bin/env python3

# Launch python tools inside of continer pyrunner from within dockerPython.sh

import argparse
import inspectTable
# inspectTable = None # Need a better stub

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Inspect MLab Tables and views')
  parser.add_argument("command",
                      help="Currently only inspect")
  parser.add_argument("table", nargs='+',
                      help='Table or datasets to inspect')
  parser.add_argument('--dataset', action="store_true", dest="dataset", default=False,
                    help='Inspect all of the tables or views in a BQ dataset')
  parser.add_argument('--minimal', action="store_true", dest="quick", default=False,
                    help='Minimal (quick) inspection')
  parser.add_argument('--extended', action="store_true", dest="extended", default=False,
                    help='Extended (slower) inspection')

  args = parser.parse_args()

  # Insist that the first arg is a command name
  if args.command != 'inspect':
    print ("Currently the only supported command is 'inspect'")
    parser.print_help()
    exit (2)

  if args.dataset:
    for t in args.table:
      inspectTable.inventoryDataSet(t, **vars(args))
  else:
    for t in args.table:
      inspectTable.inventoryTable(t, **vars(args))
