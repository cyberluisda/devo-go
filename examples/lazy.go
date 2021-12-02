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
	batchDelayBtwSendEvents = time.Millisecond * 100
)

func main() {

	if len(os.Args) < 5 {
		fmt.Println(
			`"usage:", ` + os.Args[0] + `, "tcp://relayFQDN:relayPort #messages_in_standby #messages_connected tag
This command is an example of sending data to Devo relay in house
(see https://docs.devo.com/confluence/ndt/sending-data-to-devo/the-devo-in-house-relay for more info)
in ASYNCHRONOUS mode using a Lazy client.

#messages_in_standby mode will be saved in buffer because client is passed to "stand by mode" (no conn active)
before call SendAsyncxxxx method

Then WakeUp is called in order to reconnect to relay and send new #messages_connected messages

Alternatively you can create a relay-in-house mock using netcat tool. For example:
Run 'nc -kl localhost 13000' and leave executing
In other terminal run", ` + os.Args[0] + `, tcp://localhost:13000 10 10 test.keep.free
You will see raw events displayed on first terminal

* #messages_in_standby and #messages_connected are the number of messages to be buferred and then send and send
to the relay
* tag: is the tag to mark the half of amount of messages, the other half are marked with default
tag.


The main procedure implemented is:
1. Create a lazy client connected to provide relay url
2. Send x messages using proper SendAsync method
3. Call WakeUp, this implies that all event sin buffer will be send to relay
4. Send y messages using proper SendAsync method
5. Close client. All pending messages will be send too.
      `,
		)
		os.Exit(1)
	}

	// Arguments
	entrypoint := os.Args[1]
	numMessages1, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Error when parse number of messages (%s): %v\n", os.Args[2], err)
	}
	numMessages2, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Error when parse number of messages (%s): %v\n", os.Args[4], err)
	}
	tag := os.Args[4]

	// Sender
	sender, err := devosender.NewLazyClientBuilder().
		ClientBuilder(
			devosender.NewClientBuilder().
				EntryPoint(entrypoint),
		).
		Build()

	if err != nil {
		log.Fatalf("Error while initialize Devo Sender: %v\n", err)
	}

	fmt.Println("Passing to 'stand by mode'")
	err = sender.StandBy()
	if err != nil {
		log.Fatalf("Error while pass to stand by mode: %v\n", err)
	}

	// Send messages
	fmt.Printf("Starting to send %d messages batch asynchronously in stand-by-mode (waiting %v between events) at %v\n", numMessages1, batchDelayBtwSendEvents, time.Now())
	for i := 1; i <= numMessages1; i++ {
		sender.SendWTagAsync(
			tag,
			fmt.Sprintf("this is message the message number #%d of batch 1", i),
		)
		time.Sleep(batchDelayBtwSendEvents)
	}

	fmt.Println("Waking up the client and waiting a couple of secs")
	err = sender.WakeUp()
	if err != nil {
		log.Fatalf("Error when WakeUp Devo Sender: %v\n", err)
	}
	time.Sleep(time.Second * 2)

	// Send messages
	fmt.Printf("Starting to send %d messages batch asynchronously (waiting %v between events) at %v\n", numMessages1, batchDelayBtwSendEvents, time.Now())
	for i := 1; i <= numMessages2; i++ {
		sender.SendWTagAsync(
			tag,
			fmt.Sprintf("this is message the message number #%d of batch 2", i),
		)
		time.Sleep(batchDelayBtwSendEvents)
	}

	fmt.Println("Closing the client")
	err = sender.Close()
	if err != nil {
		log.Fatalf("Error when Close Devo Sender: %v\n", err)
	}

	// fmt.Println("Waiting two seconds to ensure RetryDamon send pending events. NOTE. you can force it using Flush()")

	fmt.Println("Sender stats")
	fmt.Printf("%+v\n", sender.Stats)
	fmt.Println("Finished at", time.Now())
}
