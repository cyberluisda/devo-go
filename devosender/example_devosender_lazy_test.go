package devosender

import (
	"fmt"
	"strings"
	"time"
)

func ExampleLazyClientBuilder_initErrors() {
	lcb := &LazyClientBuilder{}
	lc, err := lcb.Build()
	fmt.Println("1: error", err, "- LazyClient", lc)

	lcb.ClientBuilder(NewClientBuilder())
	lc, err = lcb.Build()
	fmt.Println("2: error", err, "- LazyClient", lc)

	lcb.BufferSize(123)
	lc, err = lcb.Build()
	fmt.Println("3: error", err, "- LazyClient", lc)

	lcb.FlushTimeout(time.Second)
	lc, err = lcb.Build()
	fmt.Println("4: error", err, "- LazyClient", lc)

	// Output:
	// 1: error Undefined inner client builder - LazyClient <nil>
	// 2: error Buffer size less than 1 - LazyClient <nil>
	// 3: error Flush timeout empty or negative - LazyClient <nil>
	// 4: error Error while initialize client: Error when create new DevoSender (Clear): Entrypoint can not be empty - LazyClient <nil>

}

func ExampleLazyClientBuilder() {
	lcb := &LazyClientBuilder{}
	lc, err := lcb.
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		BufferSize(256).
		FlushTimeout(time.Second).
		Build()
	lc, err = lcb.Build()

	// Only print first n characters to easy test with output
	lcStr := lc.String()[:143]
	fmt.Println("error", err, "- LazyClient", lcStr, "...")

	// Output:
	// error <nil> - LazyClient bufferSize: 256, standByMode: false, #eventsInBuffer: 0, flushTimeout: 1s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:13000' ...
}

func ExampleNewLazyClientBuilder() {

	lc, err := NewLazyClientBuilder().Build()
	fmt.Println("1: error", err, "- LazyClient", lc)

	lc, err = NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	// Only print first n characters to easy test with output
	lcStr := lc.String()[:146]
	fmt.Println("2: error", err, "- LazyClient", lcStr, "...")

	// Output:
	// 1: error Undefined inner client builder - LazyClient <nil>
	// 2: error <nil> - LazyClient bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:13000' ...
}

func ExampleLazyClient_StandBy() {

	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			// udp protocol never return error
			NewClientBuilder().EntryPoint("udp://localhost:13000"),
		).
		BufferSize(2).                         // very small buffers of two events only
		EnableStandByModeTimeout(time.Second). // Set to 0 to wait for ever for async events when pass to stand by mode
		Build()
	if err != nil {
		panic(err)
	}

	fmt.Println("LazyClient", lc.String()[:141])

	// Pass to stand by mode
	err = lc.StandBy()
	if err != nil {
		panic(err)
	}
	fmt.Println("IsStandBy", lc.IsStandBy())

	// send messages in stand by mode
	err = lc.SendWTag("test.keep.free", "message 1") // Should return error because client is in standby mode
	fmt.Println("SendWTag error", err)

	id := lc.SendWTagAsync("test.keep.free", "message 2")
	fmt.Println("ID has non-conn- prefix", strings.HasPrefix(id, "non-conn-"))

	fmt.Println("Stats", lc.Stats)

	lc.SendWTagAsync("test.keep.free", "message 3")
	lc.SendWTagAsync("test.keep.free", "message 4")
	fmt.Println("Stats", lc.Stats)
	fmt.Println("LazyClient", lc)

	err = lc.WakeUp()
	if err != nil {
		panic(err)
	}

	fmt.Println("Stats after WakeUp", lc.Stats)
	fmt.Println("LazyClient after WakeUp", lc.String()[0:141])

	var sender SwitchDevoSender
	sender = lc
	sender.Close()
	fmt.Println("LazyClient as SwitchDevoSender closed", sender.String())

	// Output:
	// LazyClient bufferSize: 2, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 1s, Client: {entryPoint: 'udp://localhost:13000'
	// IsStandBy true
	// SendWTag error Receiver func call with nil pointer
	// ID has non-conn- prefix true
	// Stats AsyncEvents: 1, TotalBuffered: 1, BufferedLost: 0, SendFromBuffer: 0
	// Stats AsyncEvents: 3, TotalBuffered: 3, BufferedLost: 1, SendFromBuffer: 0
	// LazyClient bufferSize: 2, standByMode: true, #eventsInBuffer: 2, flushTimeout: 2s, standByModeTimeout: 1s, Client: {<nil>}
	// Stats after WakeUp AsyncEvents: 3, TotalBuffered: 3, BufferedLost: 1, SendFromBuffer: 2
	// LazyClient after WakeUp bufferSize: 2, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 1s, Client: {entryPoint: 'udp://localhost:13000'
	// LazyClient as SwitchDevoSender closed bufferSize: 2, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 1s, Client: {<nil>}
}

func ExampleLazyClient() {

	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")). // udp protocol never return error
		Build()
	if err != nil {
		panic(err)
	}

	fmt.Println("LazyClient", lc.String()[:146])

	// send messages in connected mode
	err = lc.SendWTag("test.keep.free", "message 1") // No error because client is connected
	if err != nil {
		panic(err)
	}
	for i := 2; i <= 10; i++ {
		id := lc.SendWTagAsync(
			"test.keep.free",
			fmt.Sprintf("message %d", i),
		)
		fmt.Printf("ID of msg %d has non-conn- prefix: %v\n", i, strings.HasPrefix(id, "non-conn-"))
	}
	fmt.Println("Stats", lc.Stats)

	// WakeUp should not have any effect
	err = lc.WakeUp()
	if err != nil {
		panic(err)
	}
	fmt.Println("Stats (after WakeUp)", lc.Stats)
	fmt.Println("LazyClient (after WakeUp)", lc.String()[0:146])

	var sender SwitchDevoSender
	sender = lc
	sender.Close()
	fmt.Println("LazyClient as SwitchDevoSender closed", sender.String())

	id := sender.SendWTagAsync("test.keep.free", "message after close is the same as after StandBy")
	fmt.Printf("ID has non-conn- prefix: %v\n", strings.HasPrefix(id, "non-conn-"))
	fmt.Println("SwitchDevoSender (pending events after close)", sender.String())
	fmt.Println("Stats (pending events after close)", lc.Stats)
	sender.Close()
	fmt.Println("SwitchDevoSender (after last close)", sender.String())
	fmt.Println("Stats (after last close)", lc.Stats)

	// Output:
	// LazyClient bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:13000'
	// ID of msg 2 has non-conn- prefix: false
	// ID of msg 3 has non-conn- prefix: false
	// ID of msg 4 has non-conn- prefix: false
	// ID of msg 5 has non-conn- prefix: false
	// ID of msg 6 has non-conn- prefix: false
	// ID of msg 7 has non-conn- prefix: false
	// ID of msg 8 has non-conn- prefix: false
	// ID of msg 9 has non-conn- prefix: false
	// ID of msg 10 has non-conn- prefix: false
	// Stats AsyncEvents: 9, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0
	// Stats (after WakeUp) AsyncEvents: 9, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0
	// LazyClient (after WakeUp) bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:13000'
	// LazyClient as SwitchDevoSender closed bufferSize: 256000, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
	// ID has non-conn- prefix: true
	// SwitchDevoSender (pending events after close) bufferSize: 256000, standByMode: true, #eventsInBuffer: 1, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
	// Stats (pending events after close) AsyncEvents: 10, TotalBuffered: 1, BufferedLost: 0, SendFromBuffer: 0
	// SwitchDevoSender (after last close) bufferSize: 256000, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
	// Stats (after last close) AsyncEvents: 10, TotalBuffered: 1, BufferedLost: 0, SendFromBuffer: 1
}

func ExampleLazyClient_SendAsync() {

	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")). // udp protocol never return error
		Build()
	if err != nil {
		panic(err)
	}

	fmt.Println("LazyClient", lc.String()[0:143])

	// send messages in connected mode
	id := lc.SendAsync("message 1") // Empty default tag implies error
	// Wait until id was processed
	for lc.IsAsyncActive(id) {
		time.Sleep(time.Millisecond * 100)
	}
	fmt.Printf("AsyncErrors associated with first id: %v\n", lc.AsyncErrors()[id])

	lc.SetDefaultTag("test.keep.free")
	id2 := lc.SendAsync("message 2")
	fmt.Printf(
		"Second msg id is equal to first id: %v, len(AsyncErrors): %d, AsyncErrors associated with first id: %v\n",
		id == id2,
		len(lc.AsyncErrors()),
		lc.AsyncErrors()[id],
	)

	fmt.Println("Stats", lc.Stats)
	fmt.Println("LazyClient (after events)", lc.String()[0:143])

	err = lc.Close()
	if err != nil {
		panic(err)
	}

	fmt.Println("Stats (after close)", lc.Stats)
	fmt.Println("LazyClient (after close)", lc)

	// Output:
	// LazyClient bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:130
	// AsyncErrors associated with first id: Tag can not be empty
	// Second msg id is equal to first id: false, len(AsyncErrors): 1, AsyncErrors associated with first id: Tag can not be empty
	// Stats AsyncEvents: 2, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0
	// LazyClient (after events) bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:130
	// Stats (after close) AsyncEvents: 2, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0
	// LazyClient (after close) bufferSize: 256000, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
}