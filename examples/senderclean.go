package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cyberluisda/devo-go/devosender"
)

const (
	defaultTag = "my.app.default.tag"
	message    = "this is message the message $ number #"
)

func main() {

	if len(os.Args) < 3 {
		fmt.Println("usage:", os.Args[0], "tcp://relayFQDN:relayPort #messages [customtag]")
		fmt.Println("This command is an example of sending data to Devo relay in house. See https://docs.devo.com/confluence/ndt/sending-data-to-devo/the-devo-in-house-relay for more info")
		fmt.Println("Alternatively you can create a relay-in-house mock using netcat tool. For example")
		fmt.Println("Run 'nc -kl localhost 13000' and leave executing")
		fmt.Println("In other terminal run", os.Args[0], "tcp://localhost:13000 20")
		fmt.Println("You will see raw events displayed on first terminal")
		fmt.Println("#messages are the number of messages that will be sent")
		fmt.Println("customtag: If it is defined events will be sent in a second batch using this task changing '$' character by '\\n' in payload message")
		os.Exit(1)
	}

	entrypoint := os.Args[1]
	numMessages, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Error when parse number of messages (%s): %v\n", os.Args[2], err)
	}

	tag := ""
	if len(os.Args) > 3 {
		tag = os.Args[3]
	}

	sender, err := devosender.NewDevoSender(entrypoint)
	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}
	defer sender.Close()

	sender.SetDefaultTag(defaultTag)

	for i := 1; i <= numMessages; i++ {
		err := sender.Send(fmt.Sprintf("%s%d", message, i))
		if err != nil {
			log.Fatalf("Error when send message # %d to default tag: %v", i, err)
		}
	}

	sender.AddReplaceSequences("$", "\\n")

	if tag != "" {
		for i := 1; i <= numMessages; i++ {
			err := sender.SendWTag(tag, fmt.Sprintf("%s%d", message, i))
			if err != nil {
				log.Fatalf("Error when send message # %d to tag '%s': %v", i, tag, err)
			}
		}
	}
}
