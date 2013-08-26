#!/bin/bash

MAXF="(max(fetch_time))"
MINF="(min(fetch_time))"
MEDIANF="(sum(fetch_time)-max(fetch_time)-min(fetch_time))"
MAXB="(max(bytes_total))"
MINB="(min(bytes_total))"
MEDIANB="(sum(bytes_total)-max(bytes_total)-min(bytes_total))"

#Time variables!
BARE_DTIME="(dtime-28800)"
DST_START="1299981600"
DTIME="(${BARE_DTIME}+((if(${BARE_DTIME}<=${DST_START},tz,dst))*3600))"
UDTIME="(${DTIME}*1000000)"
PEAK_HOUR_START="(3600000000*(19.0))"
PEAK_HOUR_END="(3600000000*(23.0))"
PEAK_WEEK_START="(86400000000*(1))"
PEAK_WEEK_END="(86400000000*(6))"

unit_id=""
do_peak=0

while (( "$#" )); do
	case $1 in
		unit_id)
			unit_id="$2"
			shift;
		;;
		do_peak)
			do_peak=1
		;;
		*)
			echo "ERROR!"
			exit
		;;
	esac
	shift
done

if [ -z "$unit_id" ]
then
	echo "Must define unit_id"
	exit
fi

cat <<END_SQL
select sustained.unit_id, sustained.isp, sustained.isp_down, sustained.isp_up, AVG(IF(sustained.sustained>sustained_quantiles.sustained99, sustained.sustained, sustained.sustained)) AS sustained_trimmed_mean FROM
	(
	SELECT sustained.unit_id, sustained.dtime, sustained.sustained, 
	sustained.isp, sustained.isp_down, sustained.isp_up, sustained_quantiles.sustained1, sustained_quantiles.sustained99 
	FROM
		(
		SELECT unit_id, dtime, isp, isp_down, isp_up,
			if ($MEDIANF>0,
				if ($MAXF-$MEDIANF<=3000000,
					if ($MAXF-$MINF<=3000000,
						AVG(bytes_sec), 
						($MAXB-$MINB)/(($MAXF-$MINF)/1000000)
					),
					($MAXB-$MEDIANB)/(($MAXF-$MEDIANF)/1000000)
				),
				if ($MINF == $MAXF,
					SUM(bytes_sec),
					($MAXB-$MINB)/(($MAXF-$MINF)/1000000)
				)
			) AS sustained
		FROM [measurement-lab:fcc_samknows_data.2011_03_validated_httpgetmt]
		INNER JOIN 
		(
			SELECT unit_id AS u, dtime AS d, target as t, MAX(sequence) AS s, MIN(sequence) as ms
			FROM [measurement-lab:fcc_samknows_data.2011_03_validated_httpgetmt] 
			WHERE unit_id=STRING("${unit_id}") GROUP BY 1,2,3 HAVING ms = 0
		) as a
		ON a.d = dtime AND a.u = unit_id AND a.t = target
		WHERE (a.s = sequence OR a.s-1 = sequence OR a.s-2 = sequence) 
END_SQL
if [ $do_peak -eq 1 ]
then
	cat <<END_PEAK
		AND
			(UTC_USEC_TO_DAY(${UDTIME}) + ${PEAK_HOUR_START}) <= ${UDTIME}
		AND 
			${UDTIME} <= UTC_USEC_TO_DAY(${UDTIME}) + ${PEAK_HOUR_END}
		AND
			UTC_USEC_TO_WEEK(${UDTIME},0) + ${PEAK_WEEK_START} <= ${UDTIME}
		AND
			${UDTIME} <= UTC_USEC_TO_WEEK(${UDTIME},0) + ${PEAK_WEEK_END}
END_PEAK
fi
cat <<END_SQL2
		GROUP BY unit_id, dtime, isp, isp_down, isp_up order by dtime asc
		) AS sustained
	JOIN
		(SELECT "${unit_id}" as unit_id, sustained1, sustained99 
		FROM
			(SELECT 
				NTH(11, QUANTILES(sustained, 1001)) as sustained1,
				NTH(990, QUANTILES(sustained, 1001)) as sustained99
				FROM
				(
				SELECT unit_id, dtime,
					if ($MEDIANF>0,
						if ($MAXF-$MEDIANF<=3000000,
							if ($MAXF-$MINF<=3000000,
								AVG(bytes_sec), 
								($MAXB-$MINB)/(($MAXF-$MINF)/1000000)
							),
							($MAXB-$MEDIANB)/(($MAXF-$MEDIANF)/1000000)
						),
						if ($MINF == $MAXF,
							SUM(bytes_sec),
							($MAXB-$MINB)/(($MAXF-$MINF)/1000000)
						)
					) AS sustained
				FROM [measurement-lab:fcc_samknows_data.2011_03_validated_httpgetmt]
				INNER JOIN 
				(
					SELECT unit_id AS u, dtime AS d, target as t, MAX(sequence) AS s, MIN(sequence) as ms
					FROM [measurement-lab:fcc_samknows_data.2011_03_validated_httpgetmt] 
					WHERE unit_id=STRING("${unit_id}") GROUP BY 1,2,3 HAVING ms = 0
				) as a
				ON a.d = dtime AND a.u = unit_id AND a.t = target
				WHERE (a.s = sequence OR a.s-1 = sequence OR a.s-2 = sequence) 
				GROUP BY unit_id, dtime order by dtime asc
				)
			)
		) AS sustained_quantiles ON sustained_quantiles.unit_id = sustained.unit_id order by sustained.dtime
	)
GROUP BY sustained.unit_id, sustained.isp, sustained.isp_down, sustained.isp_up;
END_SQL2
