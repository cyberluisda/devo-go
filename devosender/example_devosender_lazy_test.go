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
	lcStr := lc.String()[:120]
	fmt.Println("error", err, "- LazyClient", lcStr, "...")

	// Output:
	// error <nil> - LazyClient bufferSize: 256, standByMode: false, #eventsInBuffer: 0, flushTimeout: 1s, Client: {entryPoint: 'udp://localhost:13000', ...
}

func ExampleNewLazyClientBuilder() {

	lc, err := NewLazyClientBuilder().Build()
	fmt.Println("1: error", err, "- LazyClient", lc)

	lc, err = NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	// Only print first n characters to easy test with output
	lcStr := lc.String()[:123]
	fmt.Println("2: error", err, "- LazyClient", lcStr, "...")

	// Output:
	// 1: error Undefined inner client builder - LazyClient <nil>
	// 2: error <nil> - LazyClient bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, Client: {entryPoint: 'udp://localhost:13000', ...
}

func ExampleLazyClient_StandBy() {

	lc, err := NewLazyClientBuilder().
		ClientBuilder(
				NewClientBuilder().EntryPoint("udp://localhost:13000")). // udp protocol never return error
		BufferSize(2). // very small buffers of two events only
		Build()
	if err != nil {
		panic(err)
	}

	fmt.Println("LazyClient", lc.String()[:117])

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
	fmt.Println("LazyClient after WakeUp", lc.String()[0:117])

	var sender SwitchDevoSender
	sender = lc
	sender.Close()
	fmt.Println("LazyClient as SwitchDevoSender closed", sender.String())

	// Output:
	// LazyClient bufferSize: 2, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, Client: {entryPoint: 'udp://localhost:13000'
	// IsStandBy true
	// SendWTag error Receiver func call with nil pointer
	// ID has non-conn- prefix true
	// Stats AsyncEvents: 1, TotalBuffered: 1, BufferedLost: 0, SendFromBuffer: 0
	// Stats AsyncEvents: 3, TotalBuffered: 3, BufferedLost: 1, SendFromBuffer: 0
	// LazyClient bufferSize: 2, standByMode: true, #eventsInBuffer: 2, flushTimeout: 2s, Client: {<nil>}
	// Stats after WakeUp AsyncEvents: 3, TotalBuffered: 3, BufferedLost: 1, SendFromBuffer: 2
	// LazyClient after WakeUp bufferSize: 2, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, Client: {entryPoint: 'udp://localhost:13000'
	// LazyClient as SwitchDevoSender closed bufferSize: 2, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, Client: {<nil>}
}
