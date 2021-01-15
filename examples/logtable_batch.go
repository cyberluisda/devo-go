package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cyberluisda/devo-go/devologtable"
	"github.com/cyberluisda/devo-go/devoquery"
	"github.com/cyberluisda/devo-go/devosender"
)

const (
	table  = "test.keep.free"
	column = "message"
)

func main() {

	if len(os.Args) < 8 {
		fmt.Println("usage:", os.Args[0], "keyFile certFile chainFile US|EU token name_prefix #number_of_values")
		fmt.Println("This command is an example of save data in batch mode using logtable, a Key Value persistence system")
		fmt.Println("keyFile certFile chainFile are the files required to stablish TLS connection and authenticate to your Devo domain. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info")
		fmt.Println("US|UE select the Devo site")
		fmt.Println("devo_token is Token to authenticated in Devo. See 'OAuth token' section in https://docs.devo.com/confluence/ndt/api-reference/rest-api/authorizing-rest-api-requests for more info")
		fmt.Println("name_prefix is the prefix used to create names. Format is name_pefixX and value set will be X. Where X is each element of a sequence of integers starting in 0 and ends to number_of_values)")
		fmt.Println("number_of_values is the number of elements + 1 to be saved")
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
	total, err := strconv.Atoi(os.Args[7])
	if err != nil {
		log.Fatal("Error when parse '%s' name_prefix param as integer: %v", os.Args[7], err)
	}

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

	fmt.Println("Generating data")
	values := map[string]string{}
	for i := 0; i < total; i++ {
		name := fmt.Sprintf("%s%d", namePrefix, i)
		values[name] = fmt.Sprintf("%d", i)
	}

	fmt.Println("Saving data batch")
	logtable.SetBatchValues(values)

	fmt.Println("Saving last value with check enabled")
	name := fmt.Sprintf("%s%d", namePrefix, total)
	value := fmt.Sprintf("%d", total)
	err = logtable.SetValueAndCheck(name, value, time.Millisecond*500, 6)
	if err != nil {
		log.Fatalf("Error when save last value: %v\n", err)
	}

	fmt.Println("Load all values from Devo:")
	d, err := logtable.GetAll()
	if err != nil {
		log.Fatalf("Error when load all values: %v\n", err)
	}
	for k, v := range d {
		fmt.Println(k, "-->", v)
	}
}
