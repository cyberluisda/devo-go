package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/cyberluisda/devo-go/devosender"
)

func main() {

	if len(os.Args) < 7 {
		fmt.Println(
			`"usage:", ` + os.Args[0] + `, "tcp://relayFQDN:relayPort__1 #messages tcp://relayFQDN:relayPort__2 #messages tag status_path
This command is an example of sending data to Devo relay in house
(see https://docs.devo.com/confluence/ndt/sending-data-to-devo/the-devo-in-house-relay for more info)
in ASYNCHRONOUS mode using a Reliable client.
Half of the messages send to first url will be compressed to validate that compression algorithm is persisted too.
REMEMBER that if you do not specify the compression algorithm, value will be loaded from current client.

Alternatively you can create a relay-in-house mock using netcat tool. For example:
Run 'nc -kl localhost 13000' and leave executing
In other terminal run", ` + os.Args[0] + `, tcp://this-is-not-valid:80 10 tcp://localhost:13000 10 test.keep.free /tmp/test-reliable
You will see raw events displayed on first terminal

* #messages are the number of messages that will be sent to preceding address
* tag: is the tag to mark the half of amount of messages, the other half are marked with default
tag.
* status_path is the path where save the status. Be carefully because this path will be erased
WITHOUT any confirmation.

To test reliable feature you can set an invaled address at tcp://relayFQDN:relayPort__1, then
the number of messages send to this URL, then set the second url with a valid relay in house address
and the number of messates to send to this one. At the end of the day you will see all events send
to second address.

The main procedure to implement this is:
1. REMOVE STATUS PATH to ensure to start with a clean status environment
2. Create a realiable connection using status path.
3. Send data to first destination address.
4. Close reliable client
5. Crate new connect to second relay address using same status path
6. Send new generated events
7. Close client
      `,
		)
		os.Exit(1)
	}

	// Arguments
	entrypoint1 := os.Args[1]
	entrypoint2 := os.Args[3]
	numMessages1, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Error when parse number of messages (%s): %v\n", os.Args[2], err)
	}
	numMessages2, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatalf("Error when parse number of messages (%s): %v\n", os.Args[4], err)
	}
	tag := os.Args[5]
	statusPath := os.Args[6]

	// Removing statusPath
	os.RemoveAll(statusPath)

	// Sender
	sender, err := devosender.NewReliableClientBuilder().
		DbPath(statusPath).
		ClientBuilder(
			devosender.NewClientBuilder().
				EntryPoint(entrypoint1),
		).
		//RetryDaemonWaitBtwChecks(time.Millisecond * 500).
		Build()

	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}

	// Send messages
	fmt.Println("Starting to send first messages batch asynchronously at", time.Now())
	cpr := &devosender.Compressor{devosender.CompressorGzip, 1}
	for i := 1; i <= numMessages1; i++ {
		if i%2 == 0 {
			sender.SendWTagAsync(
				tag,
				fmt.Sprintf("this is message the message number #%d of batch 1", i),
			)
		} else {
			sender.SendWTagAndCompressorAsync(
				tag,
				fmt.Sprintf("this is message the message number #%d of batch 1 compressed", i),
				cpr,
			)
		}
	}

	fmt.Println("Closing client and waiting a couple of secs")
	// sender.Flush()
	// fmt.Printf("---------- %+v\n", sender.Stats())
	err = sender.Close()
	if err != nil {
		log.Fatalf("Error when Close Devo Sender: %v\n", err)
	}

	time.Sleep(time.Second * 2)

	// Sender to new url
	sender, err = devosender.NewReliableClientBuilder().
		DbPath(statusPath).
		ClientBuilder(
			devosender.NewClientBuilder().
				EntryPoint(entrypoint2),
		).
		//RetryDaemonWaitBtwChecks(time.Millisecond * 500).
		Build()

	if err != nil {
		log.Fatalf("Error when initialize Devo Sender: %v\n", err)
	}

	// Send messages
	fmt.Println("Starting to send second messages batch asynchronously at", time.Now(), "with 1 second of dealy btw events")
	for i := 1; i <= numMessages2; i++ {
		sender.SendWTagAsync(
			tag,
			fmt.Sprintf("this is message the message number #%d of batch 2", i),
		)
		fmt.Print(".")
		time.Sleep(time.Second)
	}

	fmt.Println("")

	// fmt.Println("Waiting two seconds to ensure RetryDamon send pending events. NOTE. you can force it using Flush()")

	fmt.Println("Sender stats")
	fmt.Printf("%+v\n", sender.Stats())

	fmt.Println("Wait 2 seconds and sender stats")
	time.Sleep(time.Second * 2)
	fmt.Printf("%+v\n", sender.Stats())

	fmt.Println("Flush and sender stats")
	sender.WaitForPendingAsyncMessages()
	sender.Flush()
	fmt.Printf("%+v\n", sender.Stats())

	fmt.Println("Wait 2 seconds and closing client")
	time.Sleep(time.Second * 2)
	sender.Close()
	fmt.Println("Finished at", time.Now())
}
