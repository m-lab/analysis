#!/usr/bin/env python

# Initialize database
import pandas.io.sql as psql
import pandas
import MySQLdb
import matplotlib

# To run, additional parameters may be necessary here depending upon how and
# where the local database is setup.  See
# http://mysql-python.sourceforge.net/MySQLdb.html#mysqldb for a description of
# all the parameters.
db=MySQLdb.connect(db="march2012")

def d2n(x): return matplotlib.dates.date2num(x)

# Every panelist is a subscriber of some ISP package
# This function collects all packages tested against a given host
def get_packages_for_host(hostname, table, valid_criteria):
    # group by name & package.id b/c names are not unique.
    query="""
    SELECT 
          package.id, name
    FROM 

          package, %(table)s as t 
            inner join unit_location as ul on ul.id=t.location_id

    WHERE
          ul.package_id = package.id and 
          target like '%%ispmon.samknows.%(hostname)s.measurement-lab.org' 
          %(valid)s 
    GROUP BY 
          name, package.id ; """ % { 
                                    'table'    : table, 
                                    'hostname' : hostname,
                                    'valid'    : valid_criteria }
    package = psql.read_frame(query, db)
    return { i:package.name[e] for e,i in enumerate(package.id) }

# collect averages of a given table & column value binned by date.
def get_avg_tests_for_host(hostname, column, table, valid_criteria, count=10):
    query="""
    SELECT 
        date(dtime) as date, ul.package_id as package_id, 
        avg( %s ) as avg_val, stddev( %s ) as std_val, 
        count(*) as samples
    FROM    
        package, %s t inner join unit_location ul on ul.id = t.location_id 
    WHERE 
        ul.package_id = package.id and 
        target like '%%ispmon.samknows.%s.measurement-lab.org'
        %s
    GROUP BY 
        ul.package_id, date(dtime), target 
    HAVING 
        count(*) >= %s ;
    """ % (column, column, table, hostname, valid_criteria, count) 
    return psql.read_frame(query, db)

# Return the raw values with no averages nor binning, package_id is part of the 
# output to allow association of individual measurements with specific packages
def get_raw_tests_for_host(hostname, table, column, valid_criteria):
    query="""
    SELECT 
            dtime as date, date(dtime) as day, ul.package_id as package_id, %s
    FROM    
            package, %s t inner join unit_location ul on ul.id = t.location_id 
    WHERE 
            ul.package_id = package.id and 
            target like '%%ispmon.samknows.%s.measurement-lab.org' 
            %s ; """ % (column, table, hostname, valid_criteria) 
    return psql.read_frame(query, db)

# Count all tests from a given table to the given host that pass the
# 'valid_criteria'
def get_raw_count_for_host(hostname, table, valid_criteria):
    query="""
    SELECT 
            count(*) as count
    FROM    
            package, %s t inner join unit_location ul on ul.id = t.location_id 
    WHERE 
            ul.package_id = package.id and 
            target like '%%ispmon.samknows.%s.measurement-lab.org' 
            %s ; """ % (table, hostname, valid_criteria) 
    return psql.read_frame(query, db)

# Count all tests of a given type to all hosts that pass the 'valid_criteria'
def get_raw_count_for_all(table, valid_criteria):
    query="""
    SELECT 
            count(*) as count
    FROM    
            package, %s t inner join unit_location ul on ul.id = t.location_id 
    WHERE 
            ul.package_id = package.id and 
            target like '%%ispmon.samknows.%%.measurement-lab.org' 
            %s ; """ % (table, valid_criteria) 
    return psql.read_frame(query, db)

# Uses 'good_host' as a point of reference for 'affected_host_list' and splits
# the raw tests from affected_host_list into two groups: pass & fail. samples
# that pass meet the following constraints: sample S, on the same day from the
# good_host, is within the avg+/-stddev of good_host If this is true, the sample
# passes If this is false, the sample fails

def query_by_parameter(db, table, column, good_host, affected_host_list, 
                    valid_criteria, smaller_is_better=True, show_graphs=True):

    ### COLLECTION
    package_list = {}
    for hostname in [good_host] + affected_host_list:
        package_list[hostname] = get_packages_for_host(hostname, table, 
                                                        valid_criteria)

    shared_packages = {}
    shared_packages["all"] = set()
    for affected_host in affected_host_list:
        affected_packages = package_list[affected_host].keys()
        good_packages     = package_list[good_host].keys()
        shared_packages[affected_host] = (set(affected_packages)
                                            .intersection(good_packages))
        shared_packages["all"] = (shared_packages["all"]
                                    .union(shared_packages[affected_host]))
        print "%s shares %d of %d packages with %s" % ( affected_host, 
                   len(shared_packages[affected_host]), 
                   len(package_list[affected_host].keys()), 
                   good_host)

        l = set(package_list[affected_host]).difference(package_list[good_host])
        print "%s missing %d packages" % ( affected_host, len(l) ) 

    data_count = {}
    for hostname in [good_host] + affected_host_list:
        data_count[hostname] = get_raw_count_for_host(hostname, table, 
                                                        valid_criteria)

    data_raw = {}
    for hostname in [good_host] + affected_host_list:
        data_raw[hostname] = get_raw_tests_for_host(hostname, table, 
                                                    column, valid_criteria)
    
    # Select the average and stddev per day per package_id when valid_criteria
    # is true
    result_avg=get_avg_tests_for_host(good_host, column, table, valid_criteria)

    ### DATA SORTING: separate results by package_id
    date_list_ok = {}
    date_list_raw = {}
    val_list_ok = {}
    std_list_ok  = {}
    for package_id in shared_packages["all"]:
        date_list_ok[package_id]  = (
                   d2n(result_avg[result_avg.package_id == package_id].date))
        date_list_raw[package_id] = (
                    result_avg[result_avg.package_id == package_id].date)
        val_list_ok[package_id]   = (
                    result_avg[result_avg.package_id == package_id].avg_val)
        std_list_ok[package_id]   = (
                    result_avg[result_avg.package_id == package_id].std_val)

    ### FILTERING: filter samples based on the same package_id
    # Filter data by package_id, if it is within the avg+/- stddev 
    lt_date_list = {}
    lt_rate_list = {}
    gt_date_list = {}
    gt_rate_list = {}
    for affected_host in affected_host_list:
        lt_date_list[affected_host] = {}
        lt_rate_list[affected_host] = {}
        gt_date_list[affected_host] = {}
        gt_rate_list[affected_host] = {}

    for affected_host in affected_host_list:
        print affected_host
        for package_id in shared_packages[affected_host]:
            print ".",  # this section is slow, so indicate some progress

            # initialize empty versions for appending later.
            lt_date_list[affected_host][package_id] = []
            lt_rate_list[affected_host][package_id] = []
            gt_date_list[affected_host][package_id] = []
            gt_rate_list[affected_host][package_id] = []
        
            for j,day in enumerate(date_list_raw[package_id]):                
                if (len(val_list_ok[package_id]) == 0 or 
                    len(std_list_ok[package_id]) == 0):
                    continue
                if smaller_is_better:
                    # calculate top of threshold
                    val_filter = (val_list_ok[package_id].iget(j)+
                                  std_list_ok[package_id].iget(j))
                else:
                    # calculate bottom of threshold
                    val_filter = (val_list_ok[package_id].iget(j)-
                                  std_list_ok[package_id].iget(j))
                
                # Add values that are either less than or greater than the
                # threshold

                lt_date_list[affected_host][package_id].extend(
                    list(matplotlib.dates.date2num(
                        data_raw[affected_host][
                           (data_raw[affected_host].package_id == package_id) & 
                           (data_raw[affected_host].day == day) & 
                           (data_raw[affected_host][column] <= val_filter)
                        ].date)))
                lt_rate_list[affected_host][package_id].extend(
                    list(data_raw[affected_host][
                           (data_raw[affected_host].package_id == package_id) & 
                           (data_raw[affected_host].day == day) & 
                           (data_raw[affected_host][column] <= val_filter)
                        ][column]))
                    
                gt_date_list[affected_host][package_id].extend(
                    list(matplotlib.dates.date2num(
                        data_raw[affected_host][ 
                           (data_raw[affected_host].package_id == package_id) & 
                           (data_raw[affected_host].day == day) & 
                           (data_raw[affected_host][column] > val_filter)
                        ].date)))
                gt_rate_list[affected_host][package_id].extend(
                    list(data_raw[affected_host][
                           (data_raw[affected_host].package_id == package_id) & 
                           (data_raw[affected_host].day == day) & 
                           (data_raw[affected_host][column] > val_filter)
                        ][column]))
            if smaller_is_better:
                # when smaller is better, then the lt_* values are passes
                pass_date_list = lt_date_list
                pass_rate_list = lt_rate_list
                fail_date_list = gt_date_list
                fail_rate_list = gt_rate_list
            else:
                # when bigger is better, the gt_* values are passes
                pass_date_list = gt_date_list
                pass_rate_list = gt_rate_list
                fail_date_list = lt_date_list
                fail_rate_list = lt_rate_list
        print ""

    if show_graphs:
        ## NOTE: This section requires an ipython environment.
        for affected_host in affected_host_list:
            print affected_host
            for i,package_id in enumerate(list(shared_packages[affected_host])):
                pos = i%2
                if pos == 0: f=figure(figsize=(12,3))           
                subplot(1,2,pos+1)
                grid()
                # set ymax to be a little higher than the max
                ylim(ymin=0,
                     ymax=1.1*max( data_raw[good_host]
                                  [data_raw[good_host].package_id == package_id]
                                  [column]
                                 )
                    )
                xmin = datetime.datetime.strptime("2012-03-01", "%Y-%m-%d") 
                xmax = datetime.datetime.strptime("2012-04-01", "%Y-%m-%d") 
                xlim(xmin=xmin,xmax=xmax)
                total_samples = (len(pass_date_list[affected_host][package_id])+
                                 len(fail_date_list[affected_host][package_id]))
                if total_samples == 0: 
                    percent_pass = 0.0
                else: 
                    l = float(len(pass_date_list[affected_host][package_id]))
                    t = float(total_samples)
                    percent_pass  = 100*l/t

                title("%s) Package %s, %s samples from %s, %2.1f%% pass" % (i, 
                                   package_list[affected_host][package_id], 
                                   total_samples, affected_host, percent_pass))
                ylabel("y-label") # this depends on the data set
            
                # Plot raw-good, pass-bad, fail-bad, and average-good
                plot_date(d2n(data_raw[good_host][data_raw[good_host]
                                        .package_id == package_id].date),
                          data_raw[good_host][data_raw[good_host]
                                        .package_id == package_id][column],
                          xdate=True, ydate=False, 
                          color=(.2,.8,.2,1), mec=(.2,.8,.2,1),
                          marker='.', figure=f, markersize=2, 
                          label="%s raw" % good_host)
                subplot(1,2,pos+1)
                plot_date(pass_date_list[affected_host][package_id], 
                          pass_rate_list[affected_host][package_id],
                          xdate=True, ydate=False,
                          color='blue', mec="blue",
                          marker='.', 
                          figure=f, markersize=2, 
                          label="%s pass" % affected_host) 
                subplot(1,2,pos+1)
                plot_date(fail_date_list[affected_host][package_id], 
                          fail_rate_list[affected_host][package_id],
                          xdate=True, ydate=False,
                          color="red",
                          mec="red",
                          marker='.', 
                          figure=f, markersize=2, 
                          label="%s fail" % affected_host) 
                subplot(1,2,pos+1)
                plot_date(date_list_ok[package_id]+.5, 
                          val_list_ok[package_id],
                          xdate=True, ydate=False, 
                          color='black',
                          marker='.', linestyle='-', linewidth=2,
                          figure=f, markersize=6, label="average") 

    passes = {}
    for affected_host in affected_host_list:
        passes[affected_host] = 0

    for affected_host in affected_host_list:
        for i,package_id in enumerate(list(shared_packages[affected_host])):
            l = len(pass_date_list[affected_host][package_id])
            passes[affected_host] += l
    return (passes, data_count)

def main():
    # Table name,    column name,  criteria for valid sample, smaller_is_better
    all_tables = [   
    ("curr_httpgetmt",  ["bytes_sec"], "and sequence=5 and successes=1", False),
    ("curr_httppostmt", ["bytes_sec"], "and sequence=5 and successes=1", False),
    ("curr_udplatency", ["rtt_avg", 
                         "failures/(successes+failures)"], 
                            "and successes+failures>50 and successes>0", True),
    ("curr_udpjitter",  ["latency"],   "and successes=1",                True),
    ("curr_videostream",["latency"],   "and successes=1",                True),
    ("curr_ping",       ["rtt_avg"],   "and successes=5",                True),
    ]

                   # unaffected    # affected nodes
    all_nodes = [ ('mlab2.nuq01', ['mlab3.lax01'] ),
                  ('mlab2.lga02', ['mlab1.lga02', 'mlab3.lga02'] ), 
                ]

    total_all_tests=0
    total_node_tests=0
    total_affected_tests=0

    for table,columns,criteria,smaller_is_better in all_tables:
        c = get_raw_count_for_all(table, criteria)
        mlab_total = float(c['count'][0])
        total_all_tests += mlab_total
        for i,column in enumerate(columns):
            for unaffected,affected_list in all_nodes:
                print (table, column, "'%s'"%criteria, 
                       unaffected, affected_list)
                (passes,data_count) = query_by_parameter(db, table, 
                                            column, unaffected, affected_list, 
                                            criteria, smaller_is_better, 
                                            show_graphs=False)
                for host in passes:
                    node_total = float(data_count[host]['count'][0])
                    print (host, node_total, passes[host], 
                                (node_total-passes[host]), mlab_total)
                    print (host, "node affected:", 
                                (node_total-passes[host])/node_total),
                    print ("mlab affected:", 
                                (node_total-passes[host])/mlab_total)
                    if i==0:  #only count these once per node
                        total_affected_tests += (node_total-passes[host])
                        total_node_tests += node_total
                  
    print "tests to all nodes:", total_all_tests
    print "tests to affected nodes:", total_node_tests
    print "affected tests:", total_affected_tests

if __name__ == "__main__":
    main()
