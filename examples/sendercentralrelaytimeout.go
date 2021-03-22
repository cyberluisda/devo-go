package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/cyberluisda/devo-go/devosender"
)

const (
	tag                     = "test.keep.free"
	message                 = "this is message the message number #"
	secondsWaitBtwnMessages = 1
)

func main() {

	if len(os.Args) < 7 {
		fmt.Println("usage:", os.Args[0], "keyFile certFile chainFile US|EU timeout #events")
		fmt.Println()
		fmt.Println("This command is an example of sending data to Central Devo Relay. See https://docs.devo.com/confluence/ndt/sending-data-to-devo for more info")
		fmt.Println("keyFile certFile chainFile are the files required to stablish TLS connection and authenticate to your Devo domain. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info")
		fmt.Println("US|UE select the Devo site")
		fmt.Println("timeout is the timeout parseable by time.ParseDuration")
		fmt.Println("#events is the number of events that current program will send to Devo (one event per second)")
		os.Exit(1)
	}

	numSeconds, err := strconv.Atoi(os.Args[6])
	if err != nil {
		log.Fatalf("Error when parse number of seconds while to send events (%s): %v\n", os.Args[6], err)
	}

	site, err := devosender.ParseDevoCentralEntrySite(os.Args[4])
	if err != nil {
		log.Fatalf("Site '%s' is not valid", os.Args[4])
	}

	dcb := devosender.NewClientBuilder()
	sender, err := dcb.
		DevoCentralEntryPoint(site).
		TLSFiles(os.Args[1], os.Args[2], &os.Args[3]).
		TCPTimeout(mustParseDuration(os.Args[5])).
		Build()

	if err != nil {
		log.Fatalf("Error when intialize Devo Sender Client: %v\n", err)
	}
	defer sender.Close()

	sender.SetDefaultTag(tag)

	for i := 1; i <= numSeconds; i++ {
		msg := fmt.Sprintf("%s: %s%d", time.Now(), message, i)

		fmt.Println("Sending message number", i, "to", sender.GetEntryPoint(), ":", tag)
		err := sender.Send(msg)
		if err != nil {
			log.Fatalf("Error when send message # %d to default tag: %v", i, err)
		}

		time.Sleep(time.Second)
	}
}

func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}
