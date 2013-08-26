/*
 This is a temporary table used in joins 
 by other queries. There is one row
 for each test (NOT a row for each 
 sequence of a test!) based on the 
 unit id, dtime and target grouping.

 u: unit id
 d: dtime of the test.
 t: target of the test.
 s: the maximum sequence number of the test. (this is important!)
*/
DROP TABLE IF EXISTS tmp_httpjoin;
CREATE TABLE tmp_httpjoin (
   u INT(11),
   d DATETIME,
   t VARCHAR(255),
   s INT(11),
   INDEX (u,d,t,s)
);
INSERT tmp_httpjoin
SELECT unit_id AS u, dtime AS d, target as t, MAX(sequence) AS s
FROM curr_httpgetmt GROUP BY 1,2,3 HAVING MIN(sequence) = 0;

/*
 This is a temporary table that stores the "sustained"
 throughput for a particular test.

 u: the unit id
 d: the dtime of the test
 s: the sustained throughput (in bytes/sec)
*/
DROP TABLE IF EXISTS tmp_httpsustained;
CREATE TABLE tmp_httpsustained (
   u INT(11),
   d DATETIME,    
   s DOUBLE,
   INDEX (u,d)
);    
INSERT tmp_httpsustained
SELECT unit_id, dtime,
/*
 * NOTE A:
 * based on the join (below), we assume that 
 * there are three rows (sequences) to be considered 
 * here. max() gets the largest of those three, min()
 * gets the minimum of those three, and median() gets
 * the one in the middle! 
 *
 * Because we are considering only the final three
 * sequences, 
 * min() corresponds to 20 secs after the test start
 * median() corresponds to 25 secs after the test start
 * max() corresponds to 30 seconds after the test start
 * in normal situations. This corresponds with the FCC's
 * description that they calculate sustained speed from
 * the final 5 second interval of a test.
 * 
 * We only consider bytes_sec (from the table) valid
 * if the diff. btwn the max fetch time and the min
 * fetch time is less than 3 seconds.
 */
       /* if the max fetch time is more than 3 seconds ahead 
        * of the median time, */
       if(max(fetch_time)-median(fetch_time)<=3000000,
          /* if the max fetch time is more than 3 seconds ahead
	   * of the min time, */
          if(max(fetch_time)-min(fetch_time)<=3000000, 
	     /* return bytes_sec */
	      bytes_sec, 
	     /* return an actual calculated bytes/sec.*/
	      (max(bytes_total)-min(bytes_total))/((max(fetch_time)-min(fetch_time))/1000000)),
	  /* return an actual calculated bytes/sec. */
          (max(bytes_total)-median(bytes_total))/((max(fetch_time)-median(fetch_time))/1000000)) as sustained
FROM curr_httpgetmt
INNER JOIN tmp_httpjoin a ON a.d = dtime AND a.u = unit_id
/* consider only the last three sequences. */
AND (a.s = sequence OR a.s-1 = sequence OR a.s-2 = sequence) 
AND a.t = target
/* this group by implies that a test is uniquely id'd by
 * the unit id and the time. This does NOT seem entirely 
 * reasonable, especially when there is only 1-second 
 * time resolution.
 */
GROUP BY unit_id, dtime;

/*
 * this table holds the calculated percentage 
 * values. For each of the units, this table
 * holds the bottom and top 1% for throughput
 * and burst.
 *
 * burst is defined as the bytes_sec from the
 * 0th sequence of a test.
 *
 * sustained is defined from the tmp_httpsustained.
 */
DROP TABLE IF EXISTS unit_httpgetmt_pct99;
CREATE TABLE unit_httpgetmt_pct99 (
  unit_id INT(11),
  burst_perc1 DOUBLE,
  sustained_perc1 DOUBLE,
  burst_perc99 DOUBLE,
  sustained_perc99 DOUBLE,
  INDEX (unit_id)
);
INSERT unit_httpgetmt_pct99
SELECT t.unit_id, burst1, sustained1, burst99, sustained99 FROM (SELECT DISTINCT unit_id FROM curr_httpgetmt) t
LEFT JOIN (SELECT unit_id, MEDIAN(bytes_sec,2,1) AS burst1, MEDIAN(bytes_sec,2,99) AS burst99 FROM curr_httpgetmt WHERE sequence=0 GROUP BY unit_id) b ON b.unit_id = t.unit_id
/* median(column, decimal, percent) is a special udf function 
 * that calculates the perctiles. See the SQL README file 
 * for more information about how to get/install this function
 * into mysql. 
 */
LEFT JOIN (SELECT u, MEDIAN(s,2,1) AS sustained1, MEDIAN(s,2,99) AS sustained99 FROM tmp_httpsustained GROUP BY u) s ON s.u = t.unit_id;

SELECT u.unit_id,
       a.period, a.burst_min, a.burst_max, a.burst_mean, a.burst_trimmed_mean, a.burst_99_pct, a.burst_median, a.burst_stddev, a.burst_trimmed_stddev, a.sustained_min,
       a.sustained_max, a.sustained_mean, a.sustained_trimmed_mean,
       a.sustained_10_pct, a.sustained_90_pct, a.sustained_8_pct, a.sustained_92_pct, a.sustained_5_pct, a.sustained_95_pct, a.sustained_3_pct, a.sustained_97_pct, 
       a.sustained_99_pct, a.sustained_median, a.sustained_stddev, a.sustained_trimmed_stddev, a.samples,

       b.period, b.burst_min, b.burst_max, b.burst_mean, b.burst_trimmed_mean, b.burst_99_pct, b.burst_median, b.burst_stddev, b.burst_trimmed_stddev, b.sustained_min,
       b.sustained_max, b.sustained_mean, b.sustained_trimmed_mean,
       b.sustained_10_pct, b.sustained_90_pct, b.sustained_8_pct, b.sustained_92_pct, b.sustained_5_pct, b.sustained_95_pct, b.sustained_3_pct, b.sustained_97_pct, 
       b.sustained_99_pct, b.sustained_median, b.sustained_stddev, b.sustained_trimmed_stddev, b.samples,

       c.period, c.burst_min, c.burst_max, c.burst_mean, c.burst_trimmed_mean, c.burst_99_pct, c.burst_median, c.burst_stddev, c.burst_trimmed_stddev, c.sustained_min,
       c.sustained_max, c.sustained_mean, c.sustained_trimmed_mean,
       c.sustained_10_pct, c.sustained_90_pct, c.sustained_8_pct, c.sustained_92_pct, c.sustained_5_pct, c.sustained_95_pct, c.sustained_3_pct, c.sustained_97_pct, 
       c.sustained_99_pct, c.sustained_median, c.sustained_stddev, c.sustained_trimmed_stddev, c.samples,

       d.period, d.burst_min, d.burst_max, d.burst_mean, d.burst_trimmed_mean, d.burst_99_pct, d.burst_median, d.burst_stddev, d.burst_trimmed_stddev, d.sustained_min,
       d.sustained_max, d.sustained_mean, d.sustained_trimmed_mean,
       d.sustained_10_pct, d.sustained_90_pct, d.sustained_8_pct, d.sustained_92_pct, d.sustained_5_pct, d.sustained_95_pct, d.sustained_3_pct, d.sustained_97_pct, 
       d.sustained_99_pct, d.sustained_median, d.sustained_stddev, d.sustained_trimmed_stddev, d.samples,

       e.period, e.sustained_trimmed_mean, e.sustained_trimmed_stddev, e.samples,
       f.period, f.sustained_trimmed_mean, f.sustained_trimmed_stddev, f.samples,
       g.period, g.sustained_trimmed_mean, g.sustained_trimmed_stddev, g.samples,
       h.period, h.sustained_trimmed_mean, h.sustained_trimmed_stddev, h.samples,
       i.period, i.sustained_trimmed_mean, i.sustained_trimmed_stddev, i.samples,
       j.period, j.sustained_trimmed_mean, j.sustained_trimmed_stddev, j.samples,
       k.period, k.sustained_trimmed_mean, k.sustained_trimmed_stddev, k.samples,
       l.period, l.sustained_trimmed_mean, l.sustained_trimmed_stddev, l.samples,
       m.period, m.sustained_trimmed_mean, m.sustained_trimmed_stddev, m.samples,
       n.period, n.sustained_trimmed_mean, n.sustained_trimmed_stddev, n.samples,
       o.period, o.sustained_trimmed_mean, o.sustained_trimmed_stddev, o.samples,
       p.period, p.sustained_trimmed_mean, p.sustained_trimmed_stddev, p.samples

FROM (SELECT DISTINCT unit_id FROM curr_httpgetmt) u
LEFT JOIN (
   SELECT t.unit_id, '24hr Mon-Sun' AS period,
          MIN(bytes_sec) AS burst_min, MAX(bytes_sec) AS burst_max, AVG(bytes_sec) AS burst_mean,
          AVG(IF(bytes_sec < burst_perc1 OR bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_mean, MEDIAN(bytes_sec,2,99) AS burst_99_pct,
          MEDIAN(bytes_sec) AS burst_median, STDDEV(bytes_sec) AS burst_stddev, STDDEV(IF(bytes_sec < burst_perc1 OR bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_stddev, 
          MIN(s.s) AS sustained_min, MAX(s.s) AS sustained_max, AVG(s.s) AS sustained_mean,
          AVG(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, MEDIAN(s.s,2,99) AS sustained_99_pct,
          MEDIAN(s.s,2,10) AS sustained_10_pct, MEDIAN(s.s,2,90) AS sustained_90_pct, MEDIAN(s.s,2,8) AS sustained_8_pct, MEDIAN(s.s,2,92) AS sustained_92_pct,
          MEDIAN(s.s,2,5) AS sustained_5_pct, MEDIAN(s.s,2,95) AS sustained_95_pct, MEDIAN(s.s,2,3) AS sustained_3_pct, MEDIAN(s.s,2,97) AS sustained_97_pct, 
          MEDIAN(s.s) AS sustained_median, STDDEV(s.s) AS sustained_stddev, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev, 
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   GROUP BY t.unit_id
) a ON u.unit_id = a.unit_id
LEFT JOIN (
   SELECT t.unit_id, '24hr Sat-Sun' AS period,
          MIN(bytes_sec) AS burst_min, MAX(bytes_sec) AS burst_max, AVG(bytes_sec) AS burst_mean,
          AVG(IF(bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_mean, MEDIAN(bytes_sec,2,99) AS burst_99_pct,
          MEDIAN(bytes_sec) AS burst_median, STDDEV(bytes_sec) AS burst_stddev, STDDEV(IF(bytes_sec < burst_perc1 OR bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_stddev, 
          MIN(s.s) AS sustained_min, MAX(s.s) AS sustained_max, AVG(s.s) AS sustained_mean,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, MEDIAN(s.s,2,99) AS sustained_99_pct,
          MEDIAN(s.s,2,10) AS sustained_10_pct, MEDIAN(s.s,2,90) AS sustained_90_pct, MEDIAN(s.s,2,8) AS sustained_8_pct, MEDIAN(s.s,2,92) AS sustained_92_pct,
          MEDIAN(s.s,2,5) AS sustained_5_pct, MEDIAN(s.s,2,95) AS sustained_95_pct, MEDIAN(s.s,2,3) AS sustained_3_pct, MEDIAN(s.s,2,97) AS sustained_97_pct, 
          MEDIAN(s.s) AS sustained_median, STDDEV(s.s) AS sustained_stddev, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev, 
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (1,7)
   GROUP BY t.unit_id
) b ON u.unit_id = b.unit_id
/* calculate the peak throughput usage! 
 */
LEFT JOIN (
   SELECT t.unit_id, '1900-2200 Mon-Fri' AS period,
          MIN(bytes_sec) AS burst_min, MAX(bytes_sec) AS burst_max, AVG(bytes_sec) AS burst_mean,
	  /* burst trimmed mean is calculated after trimming
	   * off the top 1%
	   */
          AVG(IF(bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_mean, MEDIAN(bytes_sec,99) AS burst_99_pct,
          MEDIAN(bytes_sec) AS burst_median, STDDEV(bytes_sec) AS burst_stddev,
	  /* burst trimmed stddev is calculated after
	   * trimming off the top AND bottom 1%
	   */
	  STDDEV(IF(bytes_sec < burst_perc1 OR bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_stddev, 
          MIN(s.s) AS sustained_min, MAX(s.s) AS sustained_max, AVG(s.s) AS sustained_mean,
	  /* sustained trimmed mean is calculated after 
	   * trimming off the top 1%.
	   */
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, MEDIAN(s.s,2,99) AS sustained_99_pct,
          MEDIAN(s.s,2,10) AS sustained_10_pct, MEDIAN(s.s,2,90) AS sustained_90_pct, MEDIAN(s.s,2,8) AS sustained_8_pct, MEDIAN(s.s,2,92) AS sustained_92_pct,
          MEDIAN(s.s,2,5) AS sustained_5_pct, MEDIAN(s.s,2,95) AS sustained_95_pct, MEDIAN(s.s,2,3) AS sustained_3_pct, MEDIAN(s.s,2,97) AS sustained_97_pct, 
          MEDIAN(s.s) AS sustained_median, STDDEV(s.s) AS sustained_stddev, 
	  /* sustained trimmed stddev is calculated after
	   * trimming off the bottom AND top 1%
	   */
	  STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev, 
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   /*
    * dst kicked in about 1/2 way through the month! 
    * That has to be factored into the calculation 
    * of local time.
    */
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) 
   /* Monday, Tuesday, Wednesday, Thursday and Friday are 2-6 in MySQL. */
   IN (2,3,4,5,6) AND HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) 
   /* 7pm, 8pm, 9pm, 10pm are 19, 20, 21, and 22 in military time. */
   IN (19,20,21,22)
   GROUP BY t.unit_id
) c ON u.unit_id = c.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0900-1600 Mon-Fri' AS period,
          MIN(bytes_sec) AS burst_min, MAX(bytes_sec) AS burst_max, AVG(bytes_sec) AS burst_mean,
          AVG(IF(bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_mean, MEDIAN(bytes_sec,99) AS burst_99_pct,
          MEDIAN(bytes_sec) AS burst_median, STDDEV(bytes_sec) AS burst_stddev, STDDEV(IF(bytes_sec < burst_perc1 OR bytes_sec > burst_perc99, NULL, bytes_sec)) AS burst_trimmed_stddev, 
          MIN(s.s) AS sustained_min, MAX(s.s) AS sustained_max, AVG(s.s) AS sustained_mean,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, MEDIAN(s.s,2,99) AS sustained_99_pct,
          MEDIAN(s.s,2,10) AS sustained_10_pct, MEDIAN(s.s,2,90) AS sustained_90_pct, MEDIAN(s.s,2,8) AS sustained_8_pct, MEDIAN(s.s,2,92) AS sustained_92_pct,
          MEDIAN(s.s,2,5) AS sustained_5_pct, MEDIAN(s.s,2,95) AS sustained_95_pct, MEDIAN(s.s,2,3) AS sustained_3_pct, MEDIAN(s.s,2,97) AS sustained_97_pct, 
          MEDIAN(s.s) AS sustained_median, STDDEV(s.s) AS sustained_stddev, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev, 
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (9,10,11,12,13,14,15,16)
   GROUP BY t.unit_id
) d ON u.unit_id = d.unit_id

LEFT JOIN (
   SELECT t.unit_id, '0000-0200 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (0,1)
   GROUP BY t.unit_id
) e ON u.unit_id = e.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0200-0400 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (2,3)
   GROUP BY t.unit_id
) f ON u.unit_id = f.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0400-0600 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (4,5)
   GROUP BY t.unit_id
) g ON u.unit_id = g.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0600-0800 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (6,7)
   GROUP BY t.unit_id
) h ON u.unit_id = h.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0800-1000 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (8,9)
   GROUP BY t.unit_id
) i ON u.unit_id = i.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1000-1200 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (10,11)
   GROUP BY t.unit_id
) j ON u.unit_id = j.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1200-1400 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (12,13)
   GROUP BY t.unit_id
) k ON u.unit_id = k.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1400-1600 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (14,15)
   GROUP BY t.unit_id
) l ON u.unit_id = l.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1600-1800 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (16,17)
   GROUP BY t.unit_id
) m ON u.unit_id = m.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1800-2000 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (18,19)
   GROUP BY t.unit_id
) n ON u.unit_id = n.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2000-2200 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (20,21)
   GROUP BY t.unit_id
) o ON u.unit_id = o.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2200-0000 Mon-Sun' AS period,
          AVG(IF(s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_mean, STDDEV(IF(s.s < sustained_perc1 OR s.s > sustained_perc99, NULL, s.s)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httpgetmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN tmp_httpsustained s ON s.u = t.unit_id AND t.dtime = s.d
   LEFT JOIN unit_httpgetmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2011-03-13',dst,tz)) HOUR) IN (22,23)
   GROUP BY t.unit_id
) p ON u.unit_id = p.unit_id;

