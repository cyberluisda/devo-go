package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cyberluisda/devo-go/devosender"
)

const (
	tag     = "test.keep.free"
	message = "this is message the message number #"
)

func main() {

	if len(os.Args) < 7 {
		fmt.Println("usage:", os.Args[0], "keyFile certFile chainFile US|EU duration_btw_events keepalive_duration")
		fmt.Println()
		fmt.Println("This command is an example of sending data to Central Devo Relay. See https://docs.devo.com/confluence/ndt/sending-data-to-devo for more info")
		fmt.Println("keyFile certFile chainFile are the files required to stablish TLS connection and authenticate to your Devo domain. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info")
		fmt.Println("US|UE select the Devo site")
		fmt.Println("duration_btw_events parseable time.Duration string that will be use to pause between events")
		fmt.Println("keepalive_duration parseable time.Duration string that will set as TCP KeepAlive")
		fmt.Println("You can use for example if duration_btw_events is set with big value, 10m for example and you do set keepalive to 20m, the result is that TCP connection will be lost. But if you sett keep-alive to 1m TCP connection continue working")
		os.Exit(1)
	}

	site, err := devosender.ParseDevoCentralEntrySite(os.Args[4])
	if err != nil {
		log.Fatalf("Site '%s' is not valid", os.Args[4])
	}

	pauseEvents := mustParseDuration(os.Args[5])

	dcb := devosender.NewClientBuilder()
	sender, err := dcb.
		DevoCentralEntryPoint(site).
		TLSFiles(os.Args[1], os.Args[2], &os.Args[3]).
		TCPKeepAlive(mustParseDuration(os.Args[6])).
		Build()

	if err != nil {
		log.Fatalf("Error when intialize Devo Sender Client: %v\n", err)
	}
	defer sender.Close()

	sender.SetDefaultTag(tag)

	for i := 1; i > 0; i++ {
		msg := fmt.Sprintf("%s: %s%d", time.Now(), message, i)

		fmt.Println(
			"Sending message number",
			i,
			"to",
			sender.GetEntryPoint(),
			":",
			tag,
			"next event will be sent at",
			time.Now().Add(pauseEvents),
		)
		err := sender.Send(msg)
		if err != nil {
			log.Fatalf("Error when send message # %d to default tag: %v", i, err)
		}

		time.Sleep(pauseEvents)
	}
}

func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}
