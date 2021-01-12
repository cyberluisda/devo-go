package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cyberluisda/devo-go/devosender"
)

const (
	tag                     = "test.keep.free"
	message                 = "this is message the message number #"
	secondsWaitBtwnMessages = 1
)

func main() {

	if len(os.Args) < 6 {
		fmt.Println("usage:", os.Args[0], "keyFile certFile chainFile US|EU #seconds")
		fmt.Println("This command is an example of sending data to Central Devo Relay. See https://docs.devo.com/confluence/ndt/sending-data-to-devo for more info")
		fmt.Println("keyFile certFile chainFile are the files required to stablish TLS connection and authenticate to your Devo domain. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info")
		fmt.Println("#seconds is the number of seconds that current program will be sending events to Devo (one event per second)")
		os.Exit(1)
	}

	var entryPoint string
	if strings.EqualFold("US", os.Args[4]) {
		entryPoint = devosender.DevoCentralRelayUS
	} else if strings.EqualFold("EU", os.Args[4]) {
		entryPoint = devosender.DevoCentralRelayEU
	} else {
		log.Fatalf("Site '%s' is not valid", os.Args[4])
	}

	numSeconds, err := strconv.Atoi(os.Args[5])
	if err != nil {
		log.Fatalf("Error when parse number of seconds while to send events (%s): %v\n", os.Args[5], err)
	}

	sender, err := devosender.NewDevoSenderTLSFiles(entryPoint, os.Args[1], os.Args[2], &os.Args[3])
	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}

	sender.SetDefaultTag(tag)

	for i := 1; i <= numSeconds; i++ {
		msg := fmt.Sprintf("%s: %s%d", time.Now(), message, i)

		fmt.Println("Sending message number", i, "to", entryPoint, ":", tag)
		err := sender.Send(msg)
		if err != nil {
			log.Fatalf("Error when send message # %d to default tag: %v", i, err)
		}

		time.Sleep(time.Second)
	}
}
