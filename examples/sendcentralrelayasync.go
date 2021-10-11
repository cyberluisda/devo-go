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
	defaultTag = "test.keep.free"
	message    = "this is message the message number #"
)

func main() {

	if len(os.Args) < 7 {
		fmt.Println("usage:", os.Args[0], "keyFile certFile chainFile US|EU #messages tag")
		fmt.Println("This command is an example of sending data to Central Devo Relay in ASYNCHRONOUS mode. See https://docs.devo.com/confluence/ndt/sending-data-to-devo for more info")
		fmt.Println("keyFile certFile chainFile are the files required to stablish TLS connection and authenticate to your Devo domain. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info")
		fmt.Println("US|UE select the Devo site")
		fmt.Println("#messages are the number of messages that will be sent")
		fmt.Println("tag: is the tag to mark the half of amount of messages, the other half are marked with default tag", defaultTag)
		os.Exit(1)
	}

	// Arguments
	var entrypoint string
	if strings.EqualFold("US", os.Args[4]) {
		entrypoint = devosender.DevoCentralRelayUS
	} else if strings.EqualFold("EU", os.Args[4]) {
		entrypoint = devosender.DevoCentralRelayEU
	} else {
		log.Fatalf("Site '%s' is not valid", os.Args[4])
	}

	numMessages, err := strconv.Atoi(os.Args[5])
	if err != nil {
		log.Fatalf("Error when parse number of messages (%s): %v\n", os.Args[2], err)
	}
	tag := os.Args[6]

	// Sender
	sender, err := devosender.NewDevoSenderTLSFiles(entrypoint, os.Args[1], os.Args[2], &os.Args[3])
	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}
	defer sender.Close()

	sender.SetDefaultTag(defaultTag)

	// Send messages
	fmt.Println("Starting to send messages asynchronously at", time.Now())
	idsDefaultTag := make([]string, numMessages)
	idsCustomTag := make([]string, numMessages)
	for i := 1; i <= numMessages; i++ {
		idsDefaultTag[i-1] = sender.SendAsync(fmt.Sprintf("%s%d", message, i))
		idsCustomTag[i-1] = sender.SendWTagAsync(tag, fmt.Sprintf("%s%d", message, i))
	}

	fmt.Printf("Remaining ids (before wait): %v\n", sender.AsyncIds())

	fmt.Printf("Id of messages sent with custom tag (%s):\n", defaultTag)
	for _, id := range idsDefaultTag {
		fmt.Println(id)
	}
	fmt.Printf("Id of messages sent with custom tag (%s):\n", tag)
	for _, id := range idsCustomTag {
		fmt.Println(id)
	}

	fmt.Printf(
		"All messages (%d) queued at %s. and waiting for messages queue will be flushed\n",
		len(idsCustomTag)+len(idsDefaultTag),
		time.Now(),
	)

	// Wait for messages sent and report errors if found
	sender.WaitForPendingAsyncMessages()

	fmt.Printf("Remaining ids (after wait): %v\n", sender.AsyncIds())

	if len(sender.AsyncErrors()) > 0 {
		fmt.Println("Errors inventory:")
		for id, e := range sender.AsyncErrors() {
			fmt.Printf("Error from id '%s': %v\n", id, e)
		}
		log.Fatalf("Errors returned when run messages in asynchronous mode: %d", len(sender.AsyncErrors()))
	}

	fmt.Println("Finished at", time.Now())
}
