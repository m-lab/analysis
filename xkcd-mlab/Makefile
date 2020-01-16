all: years.gif

20%.gif: 20%.png
	convert $< -resize 1024x1024 $@

years.gif: 2009.gif 2010.gif 2011.gif 2012.gif 2013.gif 2014.gif 2015.gif 2016.gif 2017.gif 2018.gif 2019.gif
	convert -delay 100 $^ years.gif

%.png: %.csv xkcd-mlab
	./xkcd-mlab --csv=$< --png=$@ --label=$*

.PRECIOUS: %.csv %.png
%.csv: query.sql
	cat query.sql | sed -e 's/{YEAR}/$*/g' | bq query --use_legacy_sql=false --format=csv --headless --max_rows=16777216 > $@

xkcd-mlab: main.go
	go build .
