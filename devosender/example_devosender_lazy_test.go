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
	// 1: error undefined inner client builder - LazyClient <nil>
	// 2: error buffer size less than 1 - LazyClient <nil>
	// 3: error flush timeout empty or negative - LazyClient <nil>
	// 4: error while initialize client: while create new DevoSender (Clear): entrypoint can not be empty - LazyClient <nil>

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
	// 1: error undefined inner client builder - LazyClient <nil>
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
		MaxRecordsResendByFlush(10).           // Max records can be set too
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

	var sender SwitchDevoSender = lc
	sender.Close()
	fmt.Println("LazyClient as SwitchDevoSender closed", sender.String())

	// Output:
	// LazyClient bufferSize: 2, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 1s, Client: {entryPoint: 'udp://localhost:13000'
	// IsStandBy true
	// SendWTag error receiver func call with nil pointer
	// ID has non-conn- prefix true
	// Stats AsyncEvents: 1, TotalBuffered: 1, BufferedLost: 0, SendFromBuffer: 0 BufferCount: 1
	// Stats AsyncEvents: 3, TotalBuffered: 3, BufferedLost: 1, SendFromBuffer: 0 BufferCount: 2
	// LazyClient bufferSize: 2, standByMode: true, #eventsInBuffer: 2, flushTimeout: 2s, standByModeTimeout: 1s, Client: {<nil>}
	// Stats after WakeUp AsyncEvents: 3, TotalBuffered: 3, BufferedLost: 1, SendFromBuffer: 2 BufferCount: 0
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
	fmt.Println("SwitchDevoSender.LastSendCallTimestamp (after WakeUp) is empty", lc.LastSendCallTimestamp() == time.Time{})

	var sender SwitchDevoSender = lc
	sender.Close()
	fmt.Println("LazyClient as SwitchDevoSender closed", sender.String())

	id := sender.SendWTagAsync("test.keep.free", "message after close is the same as after StandBy")
	fmt.Printf("ID has non-conn- prefix: %v\n", strings.HasPrefix(id, "non-conn-"))
	fmt.Println("SwitchDevoSender (pending events after close)", sender.String())
	fmt.Println("Stats (pending events after close)", lc.Stats)
	fmt.Println("SwitchDevoSender.LastSendCallTimestamp (pending events after close) is empty", lc.LastSendCallTimestamp() == time.Time{})
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
	// Stats AsyncEvents: 9, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0 BufferCount: 0
	// Stats (after WakeUp) AsyncEvents: 9, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0 BufferCount: 0
	// LazyClient (after WakeUp) bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:13000'
	// SwitchDevoSender.LastSendCallTimestamp (after WakeUp) is empty false
	// LazyClient as SwitchDevoSender closed bufferSize: 256000, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
	// ID has non-conn- prefix: true
	// SwitchDevoSender (pending events after close) bufferSize: 256000, standByMode: true, #eventsInBuffer: 1, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
	// Stats (pending events after close) AsyncEvents: 10, TotalBuffered: 1, BufferedLost: 0, SendFromBuffer: 0 BufferCount: 1
	// SwitchDevoSender.LastSendCallTimestamp (pending events after close) is empty true
	// SwitchDevoSender (after last close) bufferSize: 256000, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
	// Stats (after last close) AsyncEvents: 10, TotalBuffered: 1, BufferedLost: 0, SendFromBuffer: 1 BufferCount: 0
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
	// AsyncErrors associated with first id: tag can not be empty
	// Second msg id is equal to first id: false, len(AsyncErrors): 1, AsyncErrors associated with first id: tag can not be empty
	// Stats AsyncEvents: 2, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0
	// LazyClient (after events) bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {entryPoint: 'udp://localhost:130
	// Stats (after close) AsyncEvents: 2, TotalBuffered: 0, BufferedLost: 0, SendFromBuffer: 0
	// LazyClient (after close) bufferSize: 256000, standByMode: true, #eventsInBuffer: 0, flushTimeout: 2s, standByModeTimeout: 0s, Client: {<nil>}
}

func ExampleSwitchDevoSender_StandBy_after_period_last_event() {

	// Instantiate sender
	var sender SwitchDevoSender
	var err error
	sender, err = NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().
				DefaultDevoTag("test.keep.free").
				EntryPoint("udp://localhost:13000")).
		Build()

	// Send event
	sender.SendAsync("message 1") // Any other SendXXXX method can be used here but have in mind that SendXXXAsync needs small time to be processed

	// Wait for event was processed
	for sender.AreAsyncOps() {
		time.Sleep(time.Millisecond * 100)
	}

	// StandBy only if last event was send 1 second ago
	if sender.LastSendCallTimestamp().Add(time.Second).Before(time.Now()) {
		err = sender.StandBy()
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("IsStandBy", sender.IsStandBy())

	// Wait one second
	time.Sleep(time.Second)

	// StandBy only if last event was send 1 second ago
	if sender.LastSendCallTimestamp().Add(time.Second).Before(time.Now()) {
		err = sender.StandBy()
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("IsStandBy", sender.IsStandBy())

	// Output:
	// IsStandBy false
	// IsStandBy true
}

func ExampleLazyClient_IsLimitReachedLastFlush() {
	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("udp://localhost:13000"). // udp does not return error
				DefaultDevoTag("test.keep.free")).
		MaxRecordsResendByFlush(1).
		Build()
	if err != nil {
		panic(err)
	}

	// Pass to standby mode to queue events
	err = lc.StandBy()
	if err != nil {
		panic(err)
	}

	// Send events
	lc.SendWTagAsync("test.keep.free", "event 1")
	lc.SendWTagAsync("test.keep.free", "event 2")

	fmt.Println("IsLimitReachedLastFlush after send events", lc.IsLimitReachedLastFlush())

	// Wake up and
	err = lc.WakeUp()
	if err != nil {
		panic(err)
	}
	fmt.Println("IsLimitReachedLastFlush after Wakeup (Flush implicit)", lc.IsLimitReachedLastFlush())

	//Flush one remaining event => limit reached too
	err = lc.Flush()
	if err != nil {
		panic(err)
	}
	fmt.Println("IsLimitReachedLastFlush after flush 1", lc.IsLimitReachedLastFlush())

	err = lc.Flush()
	if err != nil {
		panic(err)
	}
	fmt.Println("IsLimitReachedLastFlush after flush 2", lc.IsLimitReachedLastFlush())

	// Output:
	// IsLimitReachedLastFlush after send events false
	// IsLimitReachedLastFlush after Wakeup (Flush implicit) true
	// IsLimitReachedLastFlush after flush 1 true
	// IsLimitReachedLastFlush after flush 2 false
}

func ExampleLazyClient_PendingEventsNoConn() {
	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("udp://localhost:13000"). // udp does not return error
				DefaultDevoTag("test.keep.free")).
		Build()
	if err != nil {
		panic(err)
	}

	// Pass to standby mode to queue events
	err = lc.StandBy()
	if err != nil {
		panic(err)
	}

	// Send events
	lc.SendWTagAsync("test.keep.free", "event 1")
	lc.SendWTagAsync("test.keep.free", "event 2")

	fmt.Println("PendingEventsNoConn after StandBy", lc.PendingEventsNoConn())

	// Wake up and
	err = lc.WakeUp()
	if err != nil {
		panic(err)
	}
	fmt.Println("PendingEventsNoConn after Wakeup (Flush implicit)", lc.PendingEventsNoConn())

	// Output:
	// PendingEventsNoConn after StandBy 2
	// PendingEventsNoConn after Wakeup (Flush implicit) 0
}

func ExampleLazyClient_OnlyInMemory() {
	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("udp://localhost:13000")). // udp does not return error
		Build()
	if err != nil {
		panic(err)
	}

	fmt.Printf("OnlyInMemory: %v\n", lc.OnlyInMemory())

	// Output:
	// OnlyInMemory: true
}
