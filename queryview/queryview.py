#!/usr/bin/env python

import csv
import httplib2
import logging
import os
import pprint
import statvfs
import subprocess
import sys
import time

try:
    import gflags
    import matplotlib
    import pylab 
    from apiclient.discovery import build
    from apiclient.errors import HttpError
    from oauth2client.client import AccessTokenRefreshError
    from oauth2client.client import flow_from_clientsecrets
    from oauth2client.file import Storage
    from oauth2client.tools import run
except:
    import traceback
    print "Error: Failed to import a dependency:"
    print "This is a common package in most Linux distros:"
    print "     python-matplotlib"
    print "     py-matplotlib"
    print "     python-gflags"
    print "     google-api-python-client"
    print "Or, try: "
    print "     easy_install --upgrade google-api-python-client"
    print "     easy_install -U matplotlib"
    traceback.print_exc()
    sys.exit(1)


gflags.DEFINE_string('query', None,
                     "Required: name of SQL query in $PWD/sql/",
                     short_name='q')
gflags.DEFINE_string('csvfile', None,
                     "Optional: name of CSV file instead of --query",
                     short_name='c')
gflags.DEFINE_multistring('mergefiles', [],
                     "Optional: merge multiple CSV files",
                     short_name='m')
gflags.DEFINE_string('timestamp', None,
                     "Required: X-axis column name with timestamps.",
                     short_name='t')

gflags.DEFINE_multistring('columns', [],
                     ("Required: Y-axis column name to plot as a line. "+
                      "Can be specified multiple times. "+
                      "To add an error bar to the line, add a second "+
                      "column with a comma, such as: column1,column2. "),
                     short_name='l')


gflags.DEFINE_bool("refresh", False,
                   ("By default, query results are cached and reused "+
                    "on subsequent calls. The cache file is refreshed"+
                    " automatically when either the query file mtime "+
                    "is greater or a day has passed from "+
                    "the cache file's mtime."),
                   short_name='r')

gflags.DEFINE_string("count_column", None,
                     ("Create a second plot below the main axis. Use "+
                      "the given column name for values."))
gflags.DEFINE_multistring("date_vline", [],
                     ("Draw a vertical line at given date: YYYY-MM-DD"))

gflags.DEFINE_multistring("color_list", [],
                     "Colors are applied to lines in order specified.",
                     short_name='C')

gflags.DEFINE_multistring("define_values", [],
                     ("Pseudo macros translate SQL query by replacing "+
                      "all instances of KEY with VALUE."),
                     short_name='D')
gflags.DEFINE_string("output", None,
                     "Output filename. Used for plot image or merged csv.")

gflags.DEFINE_float("ymax", None, "YMAX on graph")

gflags.DEFINE_string("ylabel", None, "Y-Label on graph.")
gflags.DEFINE_string("title",  None, "Title on graph.")

gflags.DEFINE_bool("plot", True,
                   "Enable/disable plotting of the resulting data.")

gflags.DEFINE_bool("verbose", False,
                   "Verbose mode: print extra details.",
                   short_name='v')
gflags.DEFINE_bool("api", True,
                   ("Use http API rather than 'bq' executable, say --noapi "+
                    "to use 'bq' command-line tool instead."))

gflags.DEFINE_string('priority', 'INTERACTIVE',
                     'Priority at which to run the query',
                     short_name = 'p')
gflags.RegisterValidator('priority',
                         lambda value: value in [ 'BATCH', 'INTERACTIVE' ],
                         message='--priority must be "BATCH" or "INTERACTIVE"')

def cmd_exists(cmd):
    """ Returns: bool, True if 'cmd' is in PATH, False otherwise."""
    return subprocess.call("type " + cmd, shell=True, 
                           stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE) == 0

def bigquery_exec(query_string, output_filename, options):
    """ A wrapper for the 'bq' command line tool.
        Args:
            query_string - a bigquery-SQL query. Since this must be passed to
                          'bq' via the command line, mind your use of quotes.
                          TODO: find a way to call the bq python function.
            output_filename - filename to save results.  If None, a temporary
                            file is used to save bq output, then deleted after
                            parsed.
            verbose - if True, print bq command before executing.
        Raises:
            Exception - on command execution error.
        Returns:
            True on success
    """
    verbose = options.verbose
    if not cmd_exists("bq"):
        print "Error: Could not find 'bq' (big query command line tool) in path"
        print "Look here: https://code.google.com/p/google-bigquery-tools/"
        sys.exit(1)

    # load query
    if query_string is None:
        cmd = """echo "count,ips\n80090,34745\n80091,34746" > %s """ % (
               output_filename)
    else:
        # TODO: find a way to call this via python
        cmd = "bq -q --format=csv query --max_rows 100000000 \"%s\" > %s" % (
                query_string, output_filename)
    
    if verbose: print cmd
    r = os.system(cmd)
    if r != 0:
        # NOTE: read content of outfile for error message.
        msg = open(output_filename, 'r').read()
        print query_string
        os.unlink(output_filename)
        raise Exception("Non-zero exit-status: %s" % msg)

    return True

PROJECT_ID = 'measurement-lab'
DATASET_ID = 'm_lab'
FLOW_OBJ   = flow_from_clientsecrets('.client_secrets.json',
                 scope='https://www.googleapis.com/auth/bigquery')

def authorize_and_build_bigquery_service():
    storage = Storage('.bigquery_credentials.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run(FLOW_OBJ, storage)

    http = httplib2.Http()

    logging.info('Authorizing...')
    http = credentials.authorize(http)

    logging.info('Building BQ service...')
    return build('bigquery', 'v2', http = http)

def write_reply_to_csv(query_reply, output_fd, header=True):
    header_cols = query_reply['schema']['fields']
    header_names = [ h['name'] for h in header_cols ]
    writer = csv.DictWriter(output_fd, header_names)

    if header:
        writer.writeheader()

    for row_vals in query_reply['rows']:
        vals = [ v['v'] for v in row_vals['f'] ]
        writer.writerow(dict(zip(header_names, vals)))

    output_fd.flush()

def bigquery_api(query_string, output_filename, options):
    BQ_SERVICE = authorize_and_build_bigquery_service()

    output_fd = open(output_filename, 'w')

    try:
        logging.info('Running %s', query_string)
        job_collection = BQ_SERVICE.jobs()
        job_data = {
            'configuration': {
                'query': {
                    'query': query_string,
                    'priority': options.priority
                }
            }
        }

        insert_response = job_collection.insert(projectId = PROJECT_ID,
                                                body = job_data).execute()

        current_status = 'INVALID'
        while current_status != 'DONE':
            logging.info("sleeping 3")
            time.sleep(3)
            status = job_collection.get(
                projectId = PROJECT_ID,
                jobId = insert_response['jobReference']['jobId']).execute()
            current_status = status['status']['state']
            logging.info(status['status'])
            logging.info('%s', current_status)

        current_row = 0
        logging.info('getQueryResults %d', current_row)
        query_reply = job_collection.getQueryResults(
            projectId = PROJECT_ID,
            jobId = insert_response['jobReference']['jobId'],
            startIndex = current_row).execute()

        total_rows = int(query_reply['totalRows'])
        show_header = True
        while ('rows' in query_reply) and current_row < total_rows:
            logging.info('Received rows from %d / %d [%.2f%%]',
                         current_row,
                         total_rows,
                         100.0 * float(current_row) / float(total_rows))
            write_reply_to_csv(query_reply, output_fd, show_header)
            show_header=False
            current_row += len(query_reply['rows'])
            logging.info('getQueryResults %d', current_row)
            query_reply = job_collection.getQueryResults(
                projectId = PROJECT_ID,
                jobId = query_reply['jobReference']['jobId'],
                startIndex = current_row).execute()

    except HttpError as err:
        logging.error('Error running query: %s', pprint.pprint(err.content))
        sys.exit(1)

    except AccessTokenRefreshError:
        logging.error('Credentials have been revoked or expired. Please re-run '
                      'the application to re-authorize.')
        sys.exit(1)

    output_fd.close()

def usage():
    return """
Summary:
    Queryview performs a BigQuery request, caches the result, and graphs the
    result data.

    At a minimum, every row in the query result should include two columns: 
        a 'timestamp' column (x-axis), and 
        a 'data' column (y-axis).
    All values are interpreted as floats.

    Queryview looks for SQL files in $PWD/sql/*.sql. Query files are specified
    on the command line without the .sql extension (see EXAMPLES).

    Query results are cached in $PWD/cache/*.csv. Cache files will be reused
    (rather than re-running the query) for one day, until the query file is
    modified, or until you manually specifiy "--refresh".

    NOTE/TODO: 
    Queryview only uses your default bigquery data set.

Examples:
    ./queryview.py -q example -t day_timestamp -l avg 

    ./queryview.py -q example -t day_timestamp -l avg -C red 

    ./queryview.py -q example -t day_timestamp -l avg -C red \\
                          --date_vline 2013-05-08

    ./queryview.py -q example -t day_timestamp -l avg -C red \\
                          --date_vline 2013-05-08 \\
                          --count_column test_count

    ./queryview.py -q example -t day_timestamp -l avg -C red \\
                          -l median -C blue \\
                          -l quantile_90 -C green \\
                          -l quantile_10 -C green \\
                          --date_vline 2013-05-08 \\
                          --count_column test_count    \\
                          --ylabel Milliseconds \\
                          --title "web100.MinRTT on one Machine" \\
                          --output minrtt_image.png """

def parse_args():
    try:
        gflags.FLAGS(sys.argv)
    except gflags.FlagsError, err:
        print usage()
        print '%s\nUsage: %s ARGS\n%s' % (err, sys.argv[0], gflags.FLAGS)
        sys.exit(1)

    if len(sys.argv) == 1:
        print usage()
        print 'Usage: %s ARGS\n%s' % (sys.argv[0], gflags.FLAGS)
        sys.exit(1)

    options = gflags.FLAGS

    if options.verbose:
        level = logging.DEBUG
    else:
        level = logging.WARNING

    logging.basicConfig(format = '[%(asctime)s] %(levelname)s: %(message)s',
                        level = level)

    if (options.query is None and
        options.csvfile is None and
        len(options.mergefiles) == 0):
        print "Error: Please provide --query <filename>, or"
        print "Error:                --csv   <filename>, or"
        print "Error:                --merge <filename>"
        sys.exit(1)

    if len(options.mergefiles) == 1:
        print "Error: please specify at least 2 csv files to merge."
        sys.exit(1)

    if options.timestamp is None:
        print "Error: Please provide --timestamp <columname>"
        sys.exit(1)

    if len(options.columns) == 0 and len(options.mergefiles) == 0:
        print ("Error: Provide at least one line, --column <columname>")
        sys.exit(1)

    return (options, [])

SQL_PATH="sql"
CSV_PATH="cache"
def get_filenames(query_name, define_values=[]):
    if not os.path.exists(SQL_PATH):
        os.makedirs(SQL_PATH)
    if not os.path.exists(CSV_PATH):
        os.makedirs(CSV_PATH)
    extension = ""

    # NOTE: replace unfriendly characters
    for val in define_values:
        val = val.replace("=","_")
        val = val.replace("/", "_")
        val = val.replace("'","")
        val = val.replace("(","_")
        extension += "-"+val

    # NOTE: truncate extension to max filename length
    max_len = os.statvfs(CSV_PATH)[statvfs.F_NAMEMAX]
    extension = extension[:max_len-len(query_name)-4]
 
    query_filename = os.path.join(SQL_PATH, query_name+'.sql')
    cache_filename = os.path.join(CSV_PATH, query_name+extension+'.csv')
    return (query_filename, cache_filename)

def read_csvfile(cache_file, return_dicts):
    """
        Args:
        cache_file - name of a csv file.
        return_dicts - if True, return a csv.DictReader(), 
                       else plain csv.reader()
                         if csv.DictReader() then each row will be a dict,
                            with column names for keys and values,
                         if csv.reader() every row is a list of values.  This
                            includes the row for column names.
        Returns:
            A csv_reader object, the type determined by return_dicts.
            NOTE: All values are STRINGS. 
    """
    input_fd = open(cache_file,'r')
    if return_dicts:
        reader = csv.DictReader(input_fd)
    else:
        # NOTE: also returns headers as a row.
        reader = csv.reader(input_fd)
    return reader

def read_file(filename):
    """ If file does not exist, function exists.
        Returns:
            content of filename as string.
    """
    try:
        query_str = open(filename,'r').read()
    except:
        print "Error: could not read content of %s" % filename
        sys.exit(1)
    return query_str

def process_macros(query, define_values):
    for key_val in define_values:
        key,val = key_val.split("=")
        query = query.replace(key,val)
    return query

def ts2d(xl):
    """convert a list of unix timestamps and to matplotlib numbers suitable
       for plotting.
    """
    return matplotlib.dates.epoch2num(xl)

def plot_data(x_list, y_lists, y_errs, c_list, options):

    if len(x_list) == 0:
        print "WARNING: zero-length data set."
        print "WARNING: nothing to plot."
        return

    # Constants
    textsize = 9
    left, width = 0.1, 0.8
    axescolor  = '#f6f6f6'  # the axes background color

    # NOTE: default size fills everything
    rect1 = [left, 0.1, width, 0.75] #left, bottom, width, height
    if options.count_column:
        # if the count image is included, then resize.
        rect1 = [left, 0.25, width, 0.6] #left, bottom, width, height
        rect2 = [left, 0.1,  width, 0.15]

    fig = pylab.figure(figsize=(8,4), facecolor='white')
    ax1  = fig.add_axes(rect1, axisbg=axescolor)  
    ax1.grid()
    if options.ylabel:
        ax1.set_ylabel(options.ylabel)
    if options.title:
        ax1.set_title(options.title)

    # NOTE: if color list is empty, fill it.
    if options.color_list == []:
        for i in range(len(options.columns)):
            options.color_list.append(ax1._get_lines.color_cycle.next())

    if options.count_column:
        # NOTE: if we have two axes, then 'sharex' and disable x labels on top
        ax2  = fig.add_axes(rect2, axisbg=axescolor, sharex=ax1)
        [ label.set_visible(False) for label in ax1.get_xticklabels() ]
        for label in ax2.get_xticklabels():
            #label.set_rotation(10)   
            label.set_horizontalalignment('center')
        ax2.set_ylabel("Count")
        ax2.grid()
        ylocator = matplotlib.ticker.MaxNLocator(5, prune='both')
        ax2.yaxis.set_major_locator(ylocator)
    else:
        for label in ax1.get_xticklabels():
            #label.set_rotation(10)   
            label.set_horizontalalignment('center')

    date_ts = None
    if options.date_vline:
        for date_str in options.date_vline:
            _tup = time.strptime(date_str, "%Y-%m-%d")
            date_ts = int(time.mktime(_tup))
            ax1.axvline(ts2d(date_ts), color='brown', linewidth=2, 
                        linestyle='--', label="Update")

    ymax=0
    split_column_names = map(split_column_name, options.columns)
    for i,(y_col,y_err_col) in enumerate(split_column_names):
        if options.verbose: print "Column:", y_col

        if options.ymax is not None:
            ymax = options.ymax
        else:
            if len(y_errs[y_err_col]) > 0:
                ymax = max(y_lists[y_col]+[ymax])+max(y_errs[y_err_col])*2
            else:
                ymax = max(y_lists[y_col]+[ymax])
          
        if len(y_errs[y_err_col]) == 0:
            y_err=None
        else:
            y_err=y_errs[y_err_col]

        ax1.axis([ts2d(min(x_list)),ts2d(max(x_list)),0,ymax])

        color = options.color_list[i%len(options.color_list)]
        p, = ax1.plot_date(ts2d(x_list), y_lists[y_col], 
              xdate=True, ydate=False, marker='.', markersize=3,
              color=color, linewidth=(1 if y_err is None else 1.5),
              linestyle='-', figure=fig, label=y_col)

        if y_err is not None:
            ax1.errorbar(ts2d(x_list), y_lists[y_col], 
                        yerr=y_err, errorevery=4, 
                        ecolor=color, 
                        fmt=None,
                        figure=fig, 
                        label=y_err_col)

    ncols = len(options.columns)
    if options.date_vline:
        ncols+=len(options.date_vline)
    if ncols > 1: # if == 1, causes divide-by-zero error in library.
        # NOTE: some versions support fontsize here, others don't
        leg = ax1.legend(bbox_to_anchor=(0., 1.15, 1., .08), loc=1,
               ncol=ncols, mode="expand", 
               borderaxespad=0.) # , fontsize=10)
        # This always works.
        for t in leg.get_texts():
            t.set_fontsize('small')

    ax = ax1     # by default, use the first axis
    if options.count_column:
        ax = ax2 # but if the count plot is enabled, use ax2
        ax.plot_date(ts2d(x_list), c_list, linestyle='-', 
                     marker=None, drawstyle='steps-mid')

    # NOTE: when this is set earlier, somehow it's reset.
    # TODO: may want to make the date format configurable.
    xformatter = matplotlib.dates.DateFormatter("%b %d")
    ax.xaxis.set_major_formatter(xformatter)
    ax.xaxis.set_minor_formatter(xformatter)

    if options.output:
        pylab.savefig(options.output)
    else:
        pylab.show()

def split_column_name(column):
    y_fields = column.split(",")
    if len(y_fields) == 1:
        y_col = column
        y_err_col = None
    elif len(y_fields) == 2:
        (y_col,y_err_col) = y_fields
    else:
        print "Error: wrong column specification: %s" % column
        print "Error: we only accept <column> or <column>,<column_err>"
        sys.exit(1)
    return (y_col, y_err_col)
    
ONE_DAY=(60*60*24)
def has_recent_mtime(filename, compare_to, threshold=ONE_DAY):
    """ 
    Args:
        filename - string, the file we're deciding has a recent mtime or not.
        compare_to - string, compare filename's mtime to compare_to's mtime.
        threshold - seconds, window within which mtime is considered 'recent'
    Returns:
        True - if filename mtime is within 'threshold' of current time and 
               greater than compare_to's mtime.
        False - otherwise.
    """
    if not os.path.exists(filename) or not os.path.exists(compare_to):
        return False

    s = os.stat(filename)
    c = os.stat(compare_to)

    if s.st_mtime < c.st_mtime:
        return False

    if s.st_mtime + threshold < time.time():
        return False

    return True

def merge_rows(row_list, options):
    """ Merges all DictReader() rows in row_list and return a single dict.  It
    is expected that options.timestamp is present in each row, and that the
    value there is equal.  If they are not all equal, an AssertionError is
    raised.

    Args:
        row_list - list of rows from DictReader().next()
        options - result of parse_args()

    Raises:
        AssertionError - if the timestamps for the row values do not match, an
                        assertion is raised.
    Returns:
        merged dict.
    """
    ts=None
    merge_dict = {}
    for d in row_list:
        if ts is None:
            ts = d[options.timestamp]
        else:
            assert(ts == d[options.timestamp])
        merge_dict.update(d)
    return merge_dict

def merge_csvfiles(options):
    """ Think of this as a 'join' across options.mergefiles on equal values of
    the column options.timestamp.  This function takes each file in
    options.mergefiles, reads them, and combines their columns in
    options.output. The only common column should be options.timestamp. The
    results are undefined if the mergefiles share other column names.

    Args:
        options.mergefiles - list of csv filenames
        options.output - filename of merged csv file from this operation
    Returns:
        bool - True if success
    Raises:
        AssertionError - if merging encounters an error.
    """

    records = {}
    all_header_names = []
    records_list = []

    # collect all header fields from mergefiles
    for filename in options.mergefiles:
        records = read_csvfile(filename, True)
        records_list.append(records)
        all_header_names += records.fieldnames
    all_header_names = sorted(set(all_header_names))

    # eliminate duplicate $header
    output_fd = open(options.output,'w')
    writer = csv.DictWriter(output_fd, all_header_names)
    writer.writeheader()

    try:
        # read all values until StopIteration is reached.
        while True:
            merge_list = [ records.next() for records in records_list ]
            merge_dict = merge_rows(merge_list, options)
            writer.writerow(merge_dict)

    except StopIteration:
        pass

    output_fd.close()
    return True

def main(options):
    """
    The primary modes of operation are:
       * run query & save output, read cache, plot results
         --query <foo>
       * read cache or csv file, plot results
         --csvfile <foo.csv>
       * merge csv files, noplot
         --mergefiles <foo1.csv> --mergefiles <foo2.csv> --output <merge.csv>
    """

    cache_file = None
    if options.query:
        # RUN QUERY
        (query_file,cache_file) = get_filenames(options.query,
                                                options.define_values)
        query_str = process_macros(read_file(query_file), options.define_values)

        if options.refresh or not has_recent_mtime(cache_file, query_file):
            if options.api:
                bigquery_api(query_str, cache_file, options)
            else:
                bigquery_exec(query_str, cache_file, options)

    elif len(options.mergefiles) >= 2:
        # HANDLE MERGE (if applicable)
        success = merge_csvfiles(options)
        return
    elif options.csvfile is not None:
        # USE EXPLICIT CSV FILE (instead of running a query first)
        cache_file = options.csvfile
    else:
        print "Error: failed to identify operation."
        sys.exit(1)

    if not options.plot:
        return

    # READ RECORDS
    records = read_csvfile(cache_file, True)

    # INITIALIZE
    x_list  = []
    y_lists = {}
    y_errs  = {}
    c_list  = []
    split_column_names = map(split_column_name, options.columns)
    for y_col,y_err_col in split_column_names:
        y_lists[y_col] = []
        y_errs[y_err_col] = []

    # SORT DATA
    # TODO/NOTE: expects 'timestamp' values are sorted in ascending order
    for row in records:
        if options.verbose: print row

        try:
            # NOTE: First convert all values in this row.
            x = float(row[options.timestamp])
            if options.count_column: c = float(row[options.count_column])
            y = {col:None for col,err in split_column_names}
            e = {err:None for col,err in split_column_names if err is not None}
            for y_col,y_err_col in split_column_names:
                y[y_col] = float(row[y_col])
                if y_err_col: e[y_err_col] = float(row[y_err_col])

            # NOTE: only save values if all converted successfully
            x_list.append(x)
            if options.count_column: c_list.append(c)
            for y_col,y_err_col in split_column_names:
                y_lists[y_col].append(y[y_col])
                if y_err_col: y_errs[y_err_col].append(e[y_err_col])

        except ValueError:
            # NOTE: a conversion failed. and, b/c conversion & save are 
            #       separate, the data saved so far is still valid.
            continue

    # VISUALIZE
    plot_data(x_list, y_lists, y_errs, c_list, options)

if __name__ == "__main__":
    (options, args) = parse_args()
    main(options)
