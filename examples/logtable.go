package main

import (
	"fmt"
	"log"
	"os"
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
		fmt.Println("usage:", os.Args[0], "keyFile certFile chainFile US|EU token name value [--delete|--refresh-head]")
		fmt.Println("This command is an example of load and save using logtable, a Key Value persistence system")
		fmt.Println("keyFile certFile chainFile are the files required to stablish TLS connection and authenticate to your Devo domain. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info")
		fmt.Println("US|UE select the Devo site")
		fmt.Println("devo_token is Token to authenticated in Devo. See 'OAuth token' section in https://docs.devo.com/confluence/ndt/api-reference/rest-api/authorizing-rest-api-requests for more info")
		fmt.Println("name is the name of the value to save in Devo")
		fmt.Println("value is the value assotiacted to name to save in Devo")
		fmt.Println("--delete Do a delete operation after set value.")
		fmt.Println("--refresh-head Move internal logtable pointer to increase query performance")
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
	name := os.Args[6]
	value := os.Args[7]

	sender, err := devosender.NewDevoSenderTLSFiles(entryPoint, os.Args[1], os.Args[2], &os.Args[3])
	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}

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

	if len(os.Args) > 8 && os.Args[8] == "--refresh-head" {
		err = logtable.RefreshDataHead()
		if err != nil {
			log.Fatalf("Error when refresh data head: %v\n", err)
		}
		fmt.Println("BeginTable after refresh data head:", logtable.BeginTable)
	}

	fmt.Println("Setting...", name, "with value", value)
	// err = logtable.SetValue(name, value) You should wait for data ingestion if use this mode
	err = logtable.SetValueAndCheck(name, value, time.Millisecond*500, 6)
	if err != nil {
		log.Fatalf("Error when save '%s' with value '%s': %v\n", name, value, err)
	}

	fmt.Println("All values:")
	data, err := logtable.GetAll()
	if err != nil {
		log.Fatalf("Error when load all values: %v\n", err)
	}
	for k, v := range data {
		fmt.Println(k, "-->", v)
	}

	fmt.Println("Waiting 2 seconds")
	time.Sleep(time.Second * 2)
	savedValue, err := logtable.GetValue(name)
	if err != nil {
		log.Fatalf("Error when load value: %v\n", err)
	}
	fmt.Println("Value saved:", *savedValue)

	if len(os.Args) > 8 && os.Args[8] == "--delete" {
		fmt.Println("Deleting...", name)
		//err = logtable.DeleteValue(name) You should wait for data ingestion if use this mode
		err = logtable.DeleteValueAndCheck(name, time.Millisecond*500, 6)
		if err != nil {
			log.Fatalf("Error when delete '%s': %v\n", name, err)
		}
	}

	fmt.Println("All values:")
	data, err = logtable.GetAll()
	if err != nil {
		log.Fatalf("Error when load all values: %v\n", err)
	}
	for k, v := range data {
		fmt.Println(k, "-->", v)
	}

	if len(os.Args) > 8 && os.Args[8] == "--delete" {
		savedValue, err = logtable.GetValue(name)
		if err != nil {
			log.Fatalf("Error when load value second time: %v\n", err)
		}
		fmt.Println("Value from name (nil expeted):", savedValue)
	}

	fmt.Println("refreshing init data")
	err = logtable.RefreshDataHead()
	if err != nil {
		log.Fatalf("Error when refresh data head: %v\n", err)
	}
	fmt.Println("BeginTable after refresh data head:", logtable.BeginTable)

	fmt.Println("All values:")
	data, err = logtable.GetAll()
	if err != nil {
		log.Fatalf("Error when load all values: %v\n", err)
	}
	for k, v := range data {
		fmt.Println(k, "-->", v)
	}

	fmt.Println("Creating control point")
	err = logtable.AddControlPoint()
	if err != nil {
		log.Fatalf("Error when create control point: %v\n", err)
	}

	fmt.Println("Waiting 2 seconds")
	time.Sleep(time.Second * 2)

	fmt.Println("refreshing init data")
	err = logtable.RefreshDataHead()
	if err != nil {
		log.Fatalf("Error when refresh data head: %v\n", err)
	}
	fmt.Println("BeginTable after refresh data:", logtable.BeginTable)

	fmt.Println("All values:")
	data, err = logtable.GetAll()
	if err != nil {
		log.Fatalf("Error when load all values: %v\n", err)
	}
	for k, v := range data {
		fmt.Println(k, "-->", v)
	}

	fmt.Println("Number values:")
	for k := range data {
		fmt.Print(k, "--> ")
		v, err := logtable.GetValueAsNumber(k)
		if err == nil {
			fmt.Println("v", *v)
		} else {
			fmt.Println("err", err)
		}
	}

	fmt.Println("Bool values:")
	for k := range data {
		fmt.Print(k, "--> ")
		v, err := logtable.GetValueAsBool(k)
		if err == nil {
			fmt.Println("v", *v)
		} else {
			fmt.Println("err", err)
		}
	}
}
