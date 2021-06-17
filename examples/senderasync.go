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
	defaultTag = "my.app.default.tag"
	message    = "this is message the message number #"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Println("usage:", os.Args[0], "tcp://relayFQDN:relayPort #messages tag")
		fmt.Println("This command is an example of sending data to Devo relay in house in ASYNCHRONOUS mode. See https://docs.devo.com/confluence/ndt/sending-data-to-devo/the-devo-in-house-relay for more info")
		fmt.Println("Alternatively you can create a relay-in-house mock using netcat tool. For example")
		fmt.Println("Run 'nc -kl localhost 13000' and leave executing")
		fmt.Println("In other terminal run", os.Args[0], "tcp://localhost:13000 20")
		fmt.Println("You will see raw events displayed on first terminal")
		fmt.Println("#messages are the number of messages that will be sent")
		fmt.Println("tag: is the tag to mark the half of amount of messages, the other half are marked with default tag", defaultTag)
		os.Exit(1)
	}

	// Arguments
	entrypoint := os.Args[1]
	numMessages, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Error when parse number of messages (%s): %v\n", os.Args[2], err)
	}
	tag := os.Args[3]

	// Sender
	sender, err := devosender.NewDevoSender(entrypoint)
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

	if len(sender.AsyncErrors()) > 0 {
		fmt.Println("Errors inventory:")
		for id, e := range sender.AsyncErrors() {
			fmt.Printf("Error from id '%s': %v\n", id, e)
		}
		log.Fatalf("Errors returned when run messages in asynchronous mode: %d", len(sender.AsyncErrors()))
	}

	fmt.Println("Finished at", time.Now())
}
