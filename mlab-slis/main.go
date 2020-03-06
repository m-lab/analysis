package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/iterator"
)

var (
	query = `
with per_ip as (
	SELECT a.MeanThroughputMbps as mbps, NET.SAFE_IP_FROM_STRING(Client.IP) as ip
	FROM ` + "`measurement-lab.library.ndt_unified_downloads`" + `
	WHERE test_date > '2019-01-01'
),
avg_per_ip as (
	SELECT NET.IP_TRUNC(ip, 24) AS netblock, AVG(mbps) as mbps, ip
	FROM per_ip
	WHERE ip IS NOT NULL AND BYTE_LENGTH(ip) = 4
	GROUP BY ip
)
SELECT CONCAT(NET.IP_TO_STRING(netblock), "/24") as Block, COUNT(*) as IPsCovered, 
	SUM(IF(mbps > 1, 1, 0)) /  COUNT(*) as MoreThan1,  SUM(IF(mbps > 2, 1, 0)) /  COUNT(*) as MoreThan2,  SUM(IF(mbps > 5, 1, 0)) /  COUNT(*) as MoreThan5,
	SUM(IF(mbps > 10, 1, 0)) /  COUNT(*) as MoreThan10,  SUM(IF(mbps > 20, 1, 0)) /  COUNT(*) as MoreThan20,  SUM(IF(mbps > 50, 1, 0)) /  COUNT(*) as MoreThan50,
	SUM(IF(mbps > 100, 1, 0)) /  COUNT(*) as MoreThan100,
FROM avg_per_ip
GROUP BY netblock
ORDER BY netblock
`
)

type HistWriter struct {
	index            int
	record           int
	maxRecord        int
	filenameTemplate string
	w                io.Writer
}

func (hw *HistWriter) Write(h *Hist) (err error) {
	if hw.w == nil || hw.record >= hw.maxRecord {
		fname := fmt.Sprintf(hw.filenameTemplate, hw.index)
		log.Println("Creating", fname)
		hw.w, err = os.Create(fname)
		hw.index++
		if err != nil {
			return
		}
		_, err = fmt.Fprintln(hw.w, "Block, IPsCovered, >1, >2, >5, >10, >20, >50, >100")
		if err != nil {
			return
		}
		hw.record = 0
	}
	_, err = fmt.Fprintf(
		hw.w,
		"%s, %d, %f, %f, %f, %f, %f, %f, %f\n",
		h.Block, h.IPsCovered,
		h.MoreThan1, h.MoreThan2, h.MoreThan5,
		h.MoreThan10, h.MoreThan20, h.MoreThan50,
		h.MoreThan100,
	)
	hw.record++
	return
}

type Hist struct {
	Block      string
	IPsCovered int

	MoreThan1, MoreThan2, MoreThan5, MoreThan10, MoreThan20, MoreThan50, MoreThan100 float64
}

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not parse env variables")

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "measurement-lab")
	rtx.Must(err, "Could not create BQ client")
	q := client.Query(query)
	it, err := q.Read(ctx)
	rtx.Must(err, "Could not read query results")
	hw := &HistWriter{
		maxRecord:        10000,
		filenameTemplate: "netblockSLOs-%d.csv",
	}

	var h Hist
	for err := it.Next(&h); err != iterator.Done; err = it.Next(&h) {
		rtx.Must(err, "Could not read next row")
		rtx.Must(hw.Write(&h), "Could not write line")
	}
}
