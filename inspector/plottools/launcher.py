#!/usr/bin/env python3

# Launch python tools inside of continer pyrunner from within dockerPython.sh

from optparse import OptionParser
import inspectTable
#  inspectTable = None # Need a better stub

if __name__ == "__main__":
  parser = OptionParser()

  parser.add_option('--dataset', action="store_true", dest="dataset", default=False,
                    help='List of tables, views or datasets to inspect')
# these don't work yet
#  parser.add_option('--quick', action="store_true", dest="quick", default=False,
#                    help='Minimal quick inspection')
#  parser.add_option('--extended', action="store_true", dest="extended", default=False,
#                    help='Extended inspection')

  (options, pargs) = parser.parse_args()
  # Insist that the first arg is a command name
  if len(pargs) < 1 or "inspect".find(pargs[0]) != 0:
    print ("Currently the only supported command is 'inspect'")
    parser.print_help()
    exit (2)

  if options.dataset:
    for t in pargs[1:]:
      inspectTable.inventoryDataSet(t)
  else:
    for t in pargs[1:]:
      inspectTable.inventoryTable(t)
