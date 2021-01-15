package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/cyberluisda/devo-go/devosender"
)

const (
	tag = "test.keep.free"
)

func main() {

	if len(os.Args) < 3 {
		fmt.Println("usage:", os.Args[0], "tcp://relayFQDN:relayPort text_file_name")
		fmt.Println("This command is an example of sending a file as an event to Devo relay in house. See https://docs.devo.com/confluence/ndt/sending-data-to-devo/the-devo-in-house-relay for more info")
		fmt.Println("Alternatively you can create a relay-in-house mock using netcat tool. For example")
		fmt.Println("Run 'nc -kl localhost 13000' and leave executing")
		fmt.Println("In other terminal run", os.Args[0], "tcp://localhost:13000 file-name")
		fmt.Println("You will see raw events displayed on first terminal")
		fmt.Println("file_name is the name of the text file name that will be sent. Carriage return chars will be escaped (\\n)")
		fmt.Println("Data will be send using", tag, "tag")
		os.Exit(1)
	}

	entrypoint := os.Args[1]

	f, err := os.Open(os.Args[2])
	if err != nil {
		log.Fatalf("Error when open file '%s': %v", os.Args[2], err)
	}
	defer f.Close()

	sender, err := devosender.NewDevoSender(entrypoint)
	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}
	defer sender.Close()

	sender.SetDefaultTag(tag)
	sender.AddReplaceSequences("\n", "\\n")

	io.Copy(sender, f)
}
