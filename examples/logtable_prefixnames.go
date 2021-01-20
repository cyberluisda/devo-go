package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cyberluisda/devo-go/devologtable"
	"github.com/cyberluisda/devo-go/devoquery"
	"github.com/cyberluisda/devo-go/devosender"
)

const (
	table  = "test.keep.free"
	column = "message"
)

func main() {

	if len(os.Args) < 7 {
		fmt.Println("usage:", os.Args[0], "keyFile certFile chainFile US|EU token name_prefix [--delete]")
		fmt.Println("This command is an example to list a all variables with specific prefix. Optionally these values can be deleted")
		fmt.Println("keyFile certFile chainFile are the files required to stablish TLS connection and authenticate to your Devo domain. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info")
		fmt.Println("US|UE select the Devo site")
		fmt.Println("devo_token is Token to authenticated in Devo. See 'OAuth token' section in https://docs.devo.com/confluence/ndt/api-reference/rest-api/authorizing-rest-api-requests for more info")
		fmt.Println("name_prefix is the prefix used to get names. Names returned are ones that match with '^name_prefix.*$'")
		fmt.Println("--delete if present names listed will be deleted")
		os.Exit(1)
	}

	var entryPoint string
	var apiEndPoint string
	if strings.EqualFold("US", os.Args[4]) {
		entryPoint = devosender.DevoCentralRelayUS
		apiEndPoint = devoquery.DevoQueryApiv2US
	} else if strings.EqualFold("EU", os.Args[4]) {
		entryPoint = devosender.DevoCentralRelayEU
		apiEndPoint = devoquery.DevoQueryApiv2EU
	} else {
		log.Fatalf("Site '%s' is not valid", os.Args[4])
	}

	token := os.Args[5]
	namePrefix := os.Args[6]

	sender, err := devosender.NewDevoSenderTLSFiles(entryPoint, os.Args[1], os.Args[2], &os.Args[3])
	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}
	defer sender.Close()

	queryEngine, err := devoquery.NewTokenEngine(apiEndPoint, token)
	if err != nil {
		log.Fatalf("Error when initialize Devo query engine: %v\n", err)
	}

	logtable, err := devologtable.NewLogTableOneStringColumn(
		queryEngine,
		sender,
		table,
		column,
	)
	if err != nil {
		log.Fatalf("Error when initialize Devo log table engine: %v\n", err)
	}

	fmt.Println("Refreshing init data")
	err = logtable.RefreshDataHead()
	if err != nil {
		log.Fatalf("Error when refresh data head: %v\n", err)
	}
	fmt.Println("BeginTable after refresh data head:", logtable.BeginTable)

	fmt.Println("Loading values name")
	values, err := logtable.GetNames("^" + namePrefix + ".*$")
	if err != nil {
		log.Fatalf("Error when load value names using %s prefix: %v\n", namePrefix, err)
	}

	for _, v := range values{
		fmt.Println(v)
	}

	if len(os.Args) > 7 && os.Args[7] == "--delete" {
		fmt.Println("Deleting values")
		err = logtable.DeleteBatchValues(values)
		if err != nil {
			log.Fatalf("Error when delete elements: %v\n", err)
		}
	}
}
