package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cyberluisda/devo-go/devoquery"
)

const (
	query = `from demo.ecommerce.data
where statusCode != 200
select
	uafamily(userAgent) as device_family,
	mmcity(clientIpAddress) as city,
	mmcountry(clientIpAddress) as country
group by country, city, device_family
every -
select
	count() as errors,
	last(eventdate) as last_eventdate,
	first(eventdate) as first_eventdate
`
)

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: %s devo_token from_timestamp_in_RFC3339_format to_timestamp_in_RFC3339_format \n", os.Args[0])
		fmt.Println()
		fmt.Println("devo_token is Token to authenticated in Devo. See 'OAuth token' section in https://docs.devo.com/confluence/ndt/api-reference/rest-api/authorizing-rest-api-requests for more info")
		fmt.Println("from_timestamp_in_RFC3339_format is the begin of query time range in RFC3339 format. For example: '2021-01-01T00:00:00+00:00'")
		fmt.Println("to_timestamp_in_RFC3339_format is the end of query time range in RFC3339 format. For example: '2021-01-02T00:00:00+00:00'")
		fmt.Println("Devo API entry point is", devoquery.DevoQueryApiv2US)
		fmt.Println("Query:")
		fmt.Println(query)
		os.Exit(1)
	}

	token := os.Args[1]
	from, _ := time.Parse(time.RFC3339, os.Args[2])
	to, _ := time.Parse(time.RFC3339, os.Args[3])

	queryEngine, err := devoquery.NewTokenEngineDefaultQuery(devoquery.DevoQueryApiv2US, token, query)
	if err != nil {
		log.Fatalf("Error when create queryEngine: %v\n", err)
	}

	r, err := devoquery.RunDefaultQuery(queryEngine, from, to)
	if err != nil {
		log.Fatalf("Error when run default query: %v\n", err)
	}

	fmt.Println("Columns", r.Columns)

	rd := process(*r)

	fmt.Println("Result transformed to struct:")
	for _, v := range rd {
		fmt.Printf("%+v\n", v)
	}
}

// Parsing to custom structure
type resultData struct {
	From         time.Time
	To           time.Time
	DeviceFamily string
	City         string
	Country      string
	Errors       int
}

func process(dqr devoquery.QueryResult) []resultData {
	result := make([]resultData, len(dqr.Values))
	for i, d := range dqr.Values {
		result[i] = resultData{
			From: time.Unix(
				int64(
					d[dqr.Columns["first_eventdate"].Index].(float64)/1000,
				),
				0,
			),
			To: time.Unix(
				int64(
					d[dqr.Columns["last_eventdate"].Index].(float64)/1000,
				),
				0,
			),
			DeviceFamily: fmt.Sprintf("%v", d[dqr.Columns["device_family"].Index]),
			City:         fmt.Sprintf("%v", d[dqr.Columns["city"].Index]),
			Country:      fmt.Sprintf("%v", d[dqr.Columns["country"].Index]),
			Errors:       int(d[dqr.Columns["errors"].Index].(float64)),
		}
	}

	return result
}
