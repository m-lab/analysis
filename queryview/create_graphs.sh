#!/usr/bin/env bash

set -x
host=${1?Error: First argument should be abbreviated hostname: mlab1.nuq01}
date=${2?Error: Second argument should be YYYY-MM-DD date of update.}
fqdn="ndt.iupui.$host.measurement-lab.org"
addr=`python -c 'import socket; print socket.gethostbyname("'$fqdn'");'`

echo $host
echo $date
echo $addr

GRAPHDIR=graphs
mkdir -p $GRAPHDIR

for web100var in MinRTT SndLimTimeSnd ; do
    echo $web100var
    ./queryview.py -q ndt-tmpl-web100 \
        -D DATETABLE=[m_lab.2013_03],[m_lab.2013_04],[m_lab.2013_05] \
        -D ADDRESS=$addr \
        -D WEB100VAR=web100_log_entry.snap.$web100var \
        -t day_timestamp \
        -l med_web100 -C blue \
        -l quantile_10 -C green \
        -l quantile_90 -C green \
        --date_vline $date \
        --count_column test_count \
        --title "Daily web100.$web100var for NDT on $host" \
        --ylabel "$web100var" \
        --output $GRAPHDIR/$host.$web100var.png
done

echo "Rate"
./queryview.py -q ndt-tmpl-rate \
        -D DATETABLE=[m_lab.2013_03],[m_lab.2013_04],[m_lab.2013_05] \
        -D ADDRESS=$addr \
        -t day_timestamp \
        -l med_rate -C blue \
        -l quantile_10 -C green \
        -l quantile_90 -C green \
        --date_vline $date \
        --count_column test_count \
        --title "Daily web100.RATE for NDT on $host" \
        --ylabel "Mbps" \
        --ymax 110 \
        --output $GRAPHDIR/$host.rate.png

